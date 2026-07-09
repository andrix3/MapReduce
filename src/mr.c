#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <threads.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <stdint.h>
#include <dirent.h>
#include <signal.h>
#include "mr.h"
#include "error_utils.h"
#include "utils.h"
#include "log_internal.h"

#define MAX_PATH_LEN 4096
#define MAX_LINE_LEN (10 * 1024 * 1024)
#define MAX_VALUE_LEN (10 * 1024 * 1024)

struct mr {
    mr_attr_t attr;
    mr_mapper_t user_mapper;
    mr_reducer_t user_reducer;
    void *user_arg;
    pid_t mapper_pid;
    pid_t reducer_pid;
    int main_to_mapper_write;
    int reducer_to_main_read;
};

typedef struct { int token_len; int value_len; } mr_pair_header_t;
typedef struct { char *token; int token_len; void *value; int value_len; } pair_entry_t;
typedef struct { int out_fd; mtx_t out_mtx; size_t results_emitted; mtx_t stats_mtx; } reducer_emit_ctx_t;
typedef struct { char *token; int token_len; mr_value_t *values; size_t values_count; } reduce_group_t;
typedef struct { struct mr *mr; int pipe_in; pair_entry_t *pairs; size_t pairs_count, pairs_cap; } reducer_reader_ctx_t;
typedef struct { struct mr *mr; reducer_emit_ctx_t *emit_ctx; reduce_group_t *groups; size_t groups_count, next_group; mtx_t next_group_mtx; } reducer_worker_ctx_t;

static int cmp_pairs(const void *a, const void *b) { return strcmp(((pair_entry_t*)a)->token, ((pair_entry_t*)b)->token); }

static int reducer_emit_top(const char *token, const void *result, size_t result_size, void *emit_arg) {
    reducer_emit_ctx_t *ec = (reducer_emit_ctx_t *)emit_arg;
    if (!token || result_size > MAX_VALUE_LEN) return -1;
    int tlen = (int)strlen(token), rlen = (int)result_size;
    mtx_lock(&ec->out_mtx);
    writen(ec->out_fd, &tlen, sizeof(int));
    writen(ec->out_fd, &rlen, sizeof(int));
    writen(ec->out_fd, token, (size_t)tlen);
    if (rlen > 0 && result) {
        writen(ec->out_fd, result, (size_t)rlen);
    }
    mtx_unlock(&ec->out_mtx);
    mtx_lock(&ec->stats_mtx); ec->results_emitted++; mtx_unlock(&ec->stats_mtx);
    return 0;
}

typedef struct { char *token; int token_len; void *result; int result_len; } out_entry_t;
static int out_cmp(const void *a, const void *b) { return strcmp(((out_entry_t*)a)->token, ((out_entry_t*)b)->token); }

static int process_single_file(mr_t mr, const char *path);
static int process_multiple_files(mr_t mr, const char *path);
static void mapper_process_main(struct mr *mr, int pipe_in, int pipe_out);
static void reducer_process_main(struct mr *mr, int pipe_in, int pipe_out);

int mr_attr_init(mr_attr_t *attr) { if (!attr) return -1; attr->mapper_threads = 1; attr->reducer_threads = 1; attr->queue_size = 16; attr->log_file = NULL; return 0; }
int mr_attr_destroy(mr_attr_t *attr) { return attr ? 0 : -1; }
int mr_attr_set_mapper_threads(mr_attr_t *attr, size_t n) { if (!attr || n == 0) return -1; attr->mapper_threads = n; return 0; }
int mr_attr_set_reducer_threads(mr_attr_t *attr, size_t n) { if (!attr || n == 0) return -1; attr->reducer_threads = n; return 0; }
int mr_attr_set_queue_size(mr_attr_t *attr, size_t n) { if (!attr || n == 0) return -1; attr->queue_size = n; return 0; }
int mr_attr_set_log_file(mr_attr_t *attr, const char *path) { if (!attr) return -1; attr->log_file = path; return 0; }

int mr_create(mr_t *mr, const mr_attr_t *attr, mr_mapper_t mapper, mr_reducer_t reducer, void *user_arg) {
    if (!mr || !attr || !mapper || !reducer) return -1;
    struct mr *new_mr;
    SYSNCALL_RETURN(new_mr = malloc(sizeof(struct mr)), "malloc struct mr");
    new_mr->attr = *attr;
    const char *target_log = (attr->log_file == NULL) ? "mr.log" : attr->log_file;
    SYSNCALL_RETURN(new_mr->attr.log_file = strdup(target_log), "strdup log");
    new_mr->user_mapper = mapper; new_mr->user_reducer = reducer; new_mr->user_arg = user_arg;
    new_mr->mapper_pid = -1; new_mr->reducer_pid = -1;
    new_mr->main_to_mapper_write = -1; new_mr->reducer_to_main_read = -1;
    *mr = new_mr; return 0;
}

int mr_start(mr_t mr, const char *input_path, const char *output_path) {
    if (!mr || !input_path || !output_path) return -1;
    path_type_t in_type = check_path(input_path);
    if (in_type == PATH_INVALID || check_output_path(output_path) == -1) return -1;

    int p1[2], p2[2], p3[2];
    if (pipe(p1) == -1) return -1;
    if (pipe(p2) == -1) { close(p1[0]); close(p1[1]); return -1; }
    if (pipe(p3) == -1) { close(p1[0]); close(p1[1]); close(p2[0]); close(p2[1]); return -1; }

    if ((mr->mapper_pid = fork()) == -1) {
        close(p1[0]); close(p1[1]);
        close(p2[0]); close(p2[1]);
        close(p3[0]); close(p3[1]);
        return -1;
    }
    if (mr->mapper_pid == 0) {
        dup2(p1[0], STDIN_FILENO); dup2(p2[1], STDOUT_FILENO);
        close(p1[0]); close(p1[1]); close(p2[0]); close(p2[1]); close(p3[0]); close(p3[1]);
        mapper_process_main(mr, STDIN_FILENO, STDOUT_FILENO); _exit(0);
    }
    if ((mr->reducer_pid = fork()) == -1) {
        close(p1[0]); close(p1[1]);
        close(p2[0]); close(p2[1]);
        close(p3[0]); close(p3[1]);
        kill(mr->mapper_pid, SIGKILL);
        waitpid(mr->mapper_pid, NULL, 0);
        return -1;
    }
    if (mr->reducer_pid == 0) {
        dup2(p2[0], STDIN_FILENO); dup2(p3[1], STDOUT_FILENO);
        close(p1[0]); close(p1[1]); close(p2[0]); close(p2[1]); close(p3[0]); close(p3[1]);
        reducer_process_main(mr, STDIN_FILENO, STDOUT_FILENO); _exit(0);
    }
    close(p1[0]); close(p2[0]); close(p2[1]); close(p3[1]);
    mr->main_to_mapper_write = p1[1]; mr->reducer_to_main_read = p3[0];

    int status = (in_type == PATH_FILE) ? process_single_file(mr, input_path) : process_multiple_files(mr, input_path);
    close(mr->main_to_mapper_write); mr->main_to_mapper_write = -1;

    out_entry_t *outs = NULL; size_t o_cnt = 0, o_cap = 0;
    int run_status = 0;
    while (1) {
        int tl = 0, rl = 0;
        ssize_t bytes_read = readn(mr->reducer_to_main_read, &tl, sizeof(int));
        if (bytes_read < 0) { run_status = -1; goto err_out; }
        if (bytes_read == 0) break;
        if (readn(mr->reducer_to_main_read, &rl, sizeof(int)) != (ssize_t)sizeof(int)) { run_status = -1; goto err_out; }
        if (tl < 0 || tl > MAX_PATH_LEN || rl < 0 || rl > MAX_LINE_LEN) { run_status = -1; goto err_out; }
        char *tok = malloc((size_t)(tl + 1)); void *res = rl > 0 ? malloc((size_t)rl) : NULL;
        if (tok == NULL || (rl > 0 && res == NULL)) { free(tok); free(res); run_status = -1; goto err_out; }
        if (readn(mr->reducer_to_main_read, tok, (size_t)tl) != (ssize_t)tl) { free(tok); free(res); run_status = -1; goto err_out; }
        tok[tl] = '\0';
        if (rl > 0 && readn(mr->reducer_to_main_read, res, (size_t)rl) != (ssize_t)rl) { free(tok); free(res); run_status = -1; goto err_out; }
        if (o_cnt == o_cap) {
            o_cap = o_cap ? o_cap*2 : 64;
            out_entry_t *new_outs = realloc(outs, o_cap*sizeof(out_entry_t));
            if (!new_outs) { free(tok); free(res); run_status = -1; goto err_out; }
            outs = new_outs;
        }
        outs[o_cnt++] = (out_entry_t){tok, tl, res, rl};
    }
    close(mr->reducer_to_main_read); mr->reducer_to_main_read = -1;
    if (o_cnt > 1) qsort(outs, o_cnt, sizeof(out_entry_t), out_cmp);
    FILE *f = fopen(output_path, "wb");
    if (f) {
        for (size_t i=0; i<o_cnt; i++) {
            fwrite(&outs[i].token_len, sizeof(int), 1, f);
            fwrite(outs[i].token, 1, (size_t)outs[i].token_len, f);
            fwrite(&outs[i].result_len, sizeof(int), 1, f);
            if (outs[i].result_len > 0) {
                fwrite(outs[i].result, 1, (size_t)outs[i].result_len, f);
            }
        }
        fclose(f);
    } else {
        run_status = -1;
    }
err_out:
    for (size_t i=0; i<o_cnt; i++) { free(outs[i].token); free(outs[i].result); }
    free(outs);
    if (mr->reducer_to_main_read != -1) { close(mr->reducer_to_main_read); mr->reducer_to_main_read = -1; }
    waitpid(mr->mapper_pid, NULL, 0); waitpid(mr->reducer_pid, NULL, 0);
    return (status == -1 || run_status == -1) ? -1 : 0;
}

int mr_destroy(mr_t mr) {
    if (!mr) return -1;
    if (mr->main_to_mapper_write != -1) close(mr->main_to_mapper_write);
    if (mr->reducer_to_main_read != -1) close(mr->reducer_to_main_read);
    free((char *)mr->attr.log_file); free(mr); return 0;
}

typedef struct { char *file_name; size_t file_name_len; unsigned long line_number; char *line; size_t line_len; } internal_line_t;
typedef struct { internal_line_t *buffer; size_t head, tail, size, max_size; int eof; mtx_t mtx; cnd_t not_empty, not_full; } line_queue_t;
typedef struct { struct mr *mr; int pipe_in, pipe_out; line_queue_t queue; mtx_t pipe_out_mtx, stats_mtx; size_t lines_read, pairs_produced; } mapper_ctx_t;

static int mapper_emit(const char *token, const void *value, size_t value_size, void *emit_arg) {
    mapper_ctx_t *ctx = (mapper_ctx_t *)emit_arg;
    if (!token || value_size > MAX_VALUE_LEN) return -1;
    int tl = (int)strlen(token), vl = (int)value_size;
    mtx_lock(&ctx->pipe_out_mtx);
    writen(ctx->pipe_out, &tl, sizeof(int)); writen(ctx->pipe_out, &vl, sizeof(int));
    writen(ctx->pipe_out, token, (size_t)tl); if (vl > 0) writen(ctx->pipe_out, value, (size_t)vl);
    mtx_unlock(&ctx->pipe_out_mtx);
    mtx_lock(&ctx->stats_mtx); ctx->pairs_produced++; mtx_unlock(&ctx->stats_mtx);
    return 0;
}

static int reader_main(void *arg) {
    mapper_ctx_t *ctx = (mapper_ctx_t *)arg;
    while (1) {
        int pl = 0;
        ssize_t bytes_read = readn(ctx->pipe_in, &pl, sizeof(int));
        if (bytes_read <= 0) break;
        if (pl <= 0 || pl > MAX_PATH_LEN) goto r_err;
        char *path = malloc((size_t)(pl + 1));
        if (!path) goto r_err;
        if (readn(ctx->pipe_in, path, (size_t)pl) != (ssize_t)pl) { free(path); goto r_err; }
        path[pl] = '\0';
        int ln = 0, ll = 0;
        if (readn(ctx->pipe_in, &ln, sizeof(int)) != (ssize_t)sizeof(int)) { free(path); goto r_err; }
        if (readn(ctx->pipe_in, &ll, sizeof(int)) != (ssize_t)sizeof(int)) { free(path); goto r_err; }
        if (ll < 0 || ll > MAX_LINE_LEN) { free(path); goto r_err; }
        char *line = malloc((size_t)(ll + 1));
        if (!line) { free(path); goto r_err; }
        if (ll > 0) {
            if (readn(ctx->pipe_in, line, (size_t)ll) != (ssize_t)ll) { free(path); free(line); goto r_err; }
        }
        line[ll] = '\0';
        mtx_lock(&ctx->queue.mtx);
        while (ctx->queue.size == ctx->queue.max_size && !ctx->queue.eof) cnd_wait(&ctx->queue.not_full, &ctx->queue.mtx);
        ctx->queue.buffer[ctx->queue.tail] = (internal_line_t){path, (size_t)pl, (unsigned long)ln, line, (size_t)ll};
        ctx->queue.tail = (ctx->queue.tail+1)%ctx->queue.max_size; ctx->queue.size++;
        cnd_signal(&ctx->queue.not_empty); mtx_unlock(&ctx->queue.mtx);
    }
r_err:
    mtx_lock(&ctx->queue.mtx); ctx->queue.eof = 1; cnd_broadcast(&ctx->queue.not_empty); mtx_unlock(&ctx->queue.mtx);
    return 0;
}

static int mapper_worker_main(void *arg) {
    mapper_ctx_t *ctx = (mapper_ctx_t *)arg;
    while (1) {
        mtx_lock(&ctx->queue.mtx);
        while (ctx->queue.size == 0 && !ctx->queue.eof) cnd_wait(&ctx->queue.not_empty, &ctx->queue.mtx);
        if (ctx->queue.size == 0 && ctx->queue.eof) { mtx_unlock(&ctx->queue.mtx); break; }
        internal_line_t il = ctx->queue.buffer[ctx->queue.head];
        ctx->queue.head = (ctx->queue.head+1)%ctx->queue.max_size; ctx->queue.size--;
        cnd_signal(&ctx->queue.not_full); mtx_unlock(&ctx->queue.mtx);
        mr_file_line_t ml = {il.file_name, il.file_name_len, il.line_number, il.line, il.line_len};
        ctx->mr->user_mapper(&ml, mapper_emit, ctx, ctx->mr->user_arg);
        free(il.file_name); free(il.line);
    }
    return 0;
}

static void mapper_process_main(struct mr *mr, int pipe_in, int pipe_out) {
    mapper_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.mr = mr;
    ctx.pipe_in = pipe_in;
    ctx.pipe_out = pipe_out;
    ctx.queue.max_size = mr->attr.queue_size; ctx.queue.buffer = malloc(sizeof(internal_line_t)*ctx.queue.max_size);
    mtx_init(&ctx.queue.mtx, mtx_plain); cnd_init(&ctx.queue.not_empty); cnd_init(&ctx.queue.not_full);
    mtx_init(&ctx.pipe_out_mtx, mtx_plain); mtx_init(&ctx.stats_mtx, mtx_plain);
    thrd_t r; thrd_create(&r, reader_main, &ctx);
    thrd_t *ws = malloc(sizeof(thrd_t)*mr->attr.mapper_threads);
    for (size_t i=0; i<mr->attr.mapper_threads; i++) thrd_create(&ws[i], mapper_worker_main, &ctx);
    thrd_join(r, NULL); for (size_t i=0; i<mr->attr.mapper_threads; i++) thrd_join(ws[i], NULL);
    free(ws); free(ctx.queue.buffer);
}

static int reducer_reader_main(void *arg) {
    reducer_reader_ctx_t *ctx = (reducer_reader_ctx_t *)arg;
    while (1) {
        mr_pair_header_t h;
        ssize_t bytes_read = readn(ctx->pipe_in, &h, sizeof(h));
        if (bytes_read <= 0) break;
        if (h.token_len < 0 || h.value_len < 0) break;
        char *tok = malloc((size_t)(h.token_len + 1));
        if (!tok) break;
        if (readn(ctx->pipe_in, tok, (size_t)h.token_len) != (ssize_t)h.token_len) { free(tok); break; }
        tok[h.token_len] = '\0';
        void *val = h.value_len > 0 ? malloc((size_t)h.value_len) : NULL;
        if (h.value_len > 0 && !val) { free(tok); break; }
        if (val) {
            if (readn(ctx->pipe_in, val, (size_t)h.value_len) != (ssize_t)h.value_len) { free(tok); free(val); break; }
        }
        if (ctx->pairs_count == ctx->pairs_cap) {
            ctx->pairs_cap = ctx->pairs_cap ? ctx->pairs_cap * 2 : 128;
            pair_entry_t *new_pairs = realloc(ctx->pairs, ctx->pairs_cap * sizeof(pair_entry_t));
            if (!new_pairs) { free(tok); free(val); break; }
            ctx->pairs = new_pairs;
        }
        ctx->pairs[ctx->pairs_count++] = (pair_entry_t){tok, h.token_len, val, h.value_len};
    }
    return 0;
}

static int reducer_worker_main(void *arg) {
    reducer_worker_ctx_t *ctx = (reducer_worker_ctx_t *)arg;
    while (1) {
        size_t idx; mtx_lock(&ctx->next_group_mtx);
        if (ctx->next_group >= ctx->groups_count) { mtx_unlock(&ctx->next_group_mtx); break; }
        idx = ctx->next_group++; mtx_unlock(&ctx->next_group_mtx);
        reduce_group_t *g = &ctx->groups[idx];
        ctx->mr->user_reducer(g->token, g->values, g->values_count, reducer_emit_top, ctx->emit_ctx, ctx->mr->user_arg);
    }
    return 0;
}

static void reducer_process_main(struct mr *mr, int pipe_in, int pipe_out) {
    reducer_reader_ctx_t rctx = {mr, pipe_in, NULL, 0, 0};
    thrd_t r; thrd_create(&r, reducer_reader_main, &rctx); thrd_join(r, NULL);
    if (rctx.pairs_count == 0) { close(pipe_out); return; }
    qsort(rctx.pairs, rctx.pairs_count, sizeof(pair_entry_t), cmp_pairs);
    size_t gc = 1; for (size_t i=1; i<rctx.pairs_count; i++) if (strcmp(rctx.pairs[i].token, rctx.pairs[i-1].token)!=0) gc++;
    reduce_group_t *gs = malloc(sizeof(reduce_group_t)*gc);
    size_t gi=0, pi=0;
    while (pi < rctx.pairs_count) {
        size_t s = pi; while (pi < rctx.pairs_count && strcmp(rctx.pairs[pi].token, rctx.pairs[s].token)==0) pi++;
        gs[gi].token = rctx.pairs[s].token; gs[gi].values_count = pi-s;
        gs[gi].values = malloc(sizeof(mr_value_t)*(pi-s));
        for (size_t k=0; k<(pi-s); k++) { gs[gi].values[k].data = rctx.pairs[s+k].value; gs[gi].values[k].size = (size_t)rctx.pairs[s+k].value_len; }
        gi++;
    }
    reducer_emit_ctx_t ectx;
    memset(&ectx, 0, sizeof(ectx));
    ectx.out_fd = pipe_out;
    mtx_init(&ectx.out_mtx, mtx_plain); mtx_init(&ectx.stats_mtx, mtx_plain);
    
    reducer_worker_ctx_t wctx;
    memset(&wctx, 0, sizeof(wctx));
    wctx.mr = mr;
    wctx.emit_ctx = &ectx;
    wctx.groups = gs;
    wctx.groups_count = gc;
    wctx.next_group = 0;
    mtx_init(&wctx.next_group_mtx, mtx_plain);
    
    thrd_t *ws = malloc(sizeof(thrd_t)*mr->attr.reducer_threads);
    for (size_t i=0; i<mr->attr.reducer_threads; i++) thrd_create(&ws[i], reducer_worker_main, &wctx);
    for (size_t i=0; i<mr->attr.reducer_threads; i++) thrd_join(ws[i], NULL);
    for (size_t i=0; i<gc; i++) free(gs[i].values);
    for (size_t i=0; i<rctx.pairs_count; i++) { free(rctx.pairs[i].token); free(rctx.pairs[i].value); }
    free(rctx.pairs); free(gs); free(ws); close(pipe_out);
}

static int process_single_file(mr_t mr, const char *path) {
    FILE *f = fopen(path, "r"); if (!f) return -1;
    char *l = NULL; size_t len = 0; ssize_t nr; int pl = (int)strlen(path), ln = 1;
    int status = 0;
    while ((nr = getline(&l, &len, f)) != -1) {
        if (nr>0 && l[nr-1]=='\n') { l[nr-1]='\0'; nr--; }
        int ll = (int)nr;
        if (writen(mr->main_to_mapper_write, &pl, sizeof(int)) != sizeof(int) ||
            writen(mr->main_to_mapper_write, path, (size_t)pl) != (ssize_t)pl ||
            writen(mr->main_to_mapper_write, &ln, sizeof(int)) != sizeof(int) ||
            writen(mr->main_to_mapper_write, &ll, sizeof(int)) != sizeof(int) ||
            (ll > 0 && writen(mr->main_to_mapper_write, l, (size_t)ll) != (ssize_t)ll)) {
            status = -1;
            break;
        }
        ln++;
    }
    free(l); fclose(f); return status;
}

static int process_multiple_files(mr_t mr, const char *path) {
    struct dirent **nl; int n = scandir(path, &nl, NULL, alphasort); if (n<0) return -1;
    int status = 0;
    for (int i=0; i<n; i++) {
        if (status == 0 && nl[i]->d_type == DT_REG) {
            char fp[4096];
            snprintf(fp, 4096, "%s/%s", path, nl[i]->d_name);
            if (process_single_file(mr, fp) == -1) status = -1;
        }
        free(nl[i]);
    }
    free(nl); return status;
}
