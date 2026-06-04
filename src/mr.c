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

/* --- Strutture Interne --- */

/**
 * Definizione della struttura opaca mr_t.
 * Contiene lo stato necessario per gestire l'intera pipeline.
 */
struct mr
{
    mr_attr_t attr;            // Copia degli attributi di configurazione
    mr_mapper_t user_mapper;   // Funzione mapper dell'utente
    mr_reducer_t user_reducer; // Funzione reducer dell'utente
    void *user_arg;            // Argomento utente da passare alle callback

    pid_t mapper_pid;  // PID del processo figlio mapper
    pid_t reducer_pid; // PID del processo figlio reducer

    // Descrittori delle pipe necessari al processo principale
    int main_to_mapper_write;
    int reducer_to_main_read;
};

/**
 * Header per il protocollo di comunicazione sulle pipe
 * Usato per trasmettere coppie (token, valore) e risultati finali.
 */
typedef struct
{
    int token_len;
    int value_len;
} mr_pair_header_t;

/* Entry usata internamente dal reducer per memorizzare le coppie lette */
typedef struct pair_entry {
    char *token;
    int token_len;
    void *value;
    int value_len;
} pair_entry_t;

/* Contesto per l'emissione dei risultati dal reducer verso il main */
typedef struct {
    int out_fd;
    mtx_t out_mtx;
    size_t results_emitted;
    mtx_t stats_mtx;
} reducer_emit_ctx_t;

typedef struct {
    char *token;
    int token_len;
    mr_value_t *values;
    size_t values_count;
} reduce_group_t;

typedef struct {
    struct mr *mr;
    int pipe_in;
    pair_entry_t *pairs;
    size_t pairs_count;
    size_t pairs_cap;
} reducer_reader_ctx_t;

typedef struct {
    struct mr *mr;
    reducer_emit_ctx_t *emit_ctx;
    reduce_group_t *groups;
    size_t groups_count;
    size_t next_group;
    mtx_t next_group_mtx;
} reducer_worker_ctx_t;

static int cmp_pairs(const void *a, const void *b) {
    const pair_entry_t *pa = a;
    const pair_entry_t *pb = b;
    return strcmp(pa->token, pb->token);
}

static int reducer_emit_top(const char *token, const void *result, size_t result_size, void *emit_arg) {
    reducer_emit_ctx_t *ec = (reducer_emit_ctx_t *)emit_arg;
    if (!token) return -1;
    int token_len = (int)strlen(token);
    if (result_size > (size_t)(10 * 1024 * 1024)) return -1;

    mtx_lock(&ec->out_mtx);
    writen(ec->out_fd, &token_len, sizeof(int));
    int rlen = (int)result_size;
    writen(ec->out_fd, &rlen, sizeof(int));
    writen(ec->out_fd, token, token_len);
    if (rlen > 0 && result) writen(ec->out_fd, result, rlen);
    mtx_unlock(&ec->out_mtx);

    mtx_lock(&ec->stats_mtx);
    ec->results_emitted++;
    mtx_unlock(&ec->stats_mtx);

    return 0;
}

/* Struttura per i record di output letti dal reducer->main */
typedef struct {
    char *token;
    int token_len;
    void *result;
    int result_len;
} out_entry_t;

static int out_cmp(const void *a, const void *b) {
    const out_entry_t *A = a;
    const out_entry_t *B = b;
    return strcmp(A->token, B->token);
}

/* --- Prototipi di Funzioni Ausiliarie --- */

static int process_single_file(mr_t mr, const char *path);
static int process_multiple_files(mr_t mr, const char *path);

// Funzioni principali eseguite dai processi figli
static void mapper_process_main(struct mr *mr, int pipe_in, int pipe_out);
static void reducer_process_main(struct mr *mr, int pipe_in, int pipe_out);
// Funzioni per i thread (mapper, reducer e lettori)
static int mapper_worker_main(void *arg);
static int reader_main(void *arg);
static int reducer_reader_main(void *arg);
static int reducer_worker_main(void *arg);

/* --- Implementazione Interfaccia Pubblica --- */

int mr_attr_init(mr_attr_t *attr)
{
    if (!attr)
        return -1;

    attr->mapper_threads = 1;
    attr->reducer_threads = 1;
    attr->queue_size = 16;
    attr->log_file = NULL;

    return 0;
}
int mr_attr_destroy(mr_attr_t *attr)
{
    if (attr == NULL)
        return -1;

    // Non essendoci stati malloc interni in mr_attr_init o nei setter,
    // non c'è nulla da liberare.

    attr->mapper_threads = 0;
    attr->reducer_threads = 0;
    attr->queue_size = 0;
    attr->log_file = NULL;

    return 0;
}

int mr_attr_set_mapper_threads(mr_attr_t *attr, size_t n)
{
    if (!attr || n == 0)
        return -1;

    attr->mapper_threads = n;

    return 0;
}

int mr_attr_set_reducer_threads(mr_attr_t *attr, size_t n)
{
    if (!attr || n == 0)
        return -1;

    attr->reducer_threads = n;

    return 0;
}

int mr_attr_set_queue_size(mr_attr_t *attr, size_t n)
{
    if (!attr || n == 0)
        return -1;

    attr->queue_size = n;

    return 0;
}

int mr_attr_set_log_file(mr_attr_t *attr, const char *path)
{
    if (!attr)
        return -1;

    if (path != NULL)
    {
        struct stat s;
        // Se il percorso esiste, verifichiamo che non sia una directory
        if (stat(path, &s) == 0)
        {
            if (S_ISDIR(s.st_mode))
            {
                errno = EISDIR; // Impostiamo l'errore corretto
                return -1;
            }
        }
        // Nota: se stat fallisce con ENOENT significa che il file non esiste ancora,
        // però potrebbe venir creato in seguito.
    }

    attr->log_file = path; // NULL è valido per il default
    return 0;
}

int mr_create(mr_t *mr, const mr_attr_t *attr, mr_mapper_t mapper, mr_reducer_t reducer, void *user_arg)
{
    if (!mr || !attr || !mapper || !reducer)
        return -1;

    struct mr *new_mr;
    SYSNCALL_EXIT(new_mr = malloc(sizeof(struct mr)), "malloc struct mr fallita");

    // 1. Copia i valori scalari (mapper_threads, reducer_threads, queue_size)
    new_mr->attr = *attr;

    // 2. Gestione "Deep Copy" del log file
    // Se log_file è NULL, usiamo il default "mr.log"
    const char *target_log = (attr->log_file == NULL) ? "mr.log" : attr->log_file;

    // Usiamo strdup per creare una copia privata della stringa.
    // In questo modo siamo indipendenti dalla memoria dell'utente.
    SYSNCALL_EXIT(new_mr->attr.log_file = strdup(target_log), "strdup log_file fallita");

    // 3. Inizializzazione degli altri campi
    new_mr->user_mapper = mapper;
    new_mr->user_reducer = reducer;
    new_mr->user_arg = user_arg;
    new_mr->mapper_pid = -1;
    new_mr->reducer_pid = -1;
    new_mr->main_to_mapper_write = -1;
    new_mr->reducer_to_main_read = -1;

    // Comunichiamo al sistema di log quale file usare effettivamente
    // mr_log_set_path(new_mr->attr.log_file); // NON DEFINITA O RIMOSSA

    *mr = new_mr;
    return 0;
}

int mr_start(mr_t mr, const char *input_path, const char *output_path)
{
    if (!mr || !input_path || !output_path)
        return -1;

    path_type_t input_path_type = check_path(input_path);

    if (input_path_type == PATH_INVALID)
    {
        MR_LOG(mr, "ERROR", "Percorso di input non valido o inaccessibile");
        return -1;
    }

    if (check_output_path(output_path) == -1)
    {
        MR_LOG(mr, "ERROR", "Percorso di output non valido o inaccessibile");
        return -1;
    }

    int pipe_main_to_mapper[2];
    int pipe_mapper_to_reducer[2];
    int pipe_reducer_to_main[2];

    CHECK_ERROR(pipe(pipe_main_to_mapper), "Creazione pipe_main_to_mapper fallita");
    CHECK_ERROR(pipe(pipe_mapper_to_reducer), "Creazione pipe_mapper_to_reducer fallita");
    CHECK_ERROR(pipe(pipe_reducer_to_main), "Creazione pipe_reducer_to_main fallita");
    
    MR_LOG(mr, "INFO", "Creazione pipe completata");

    if ((mr->mapper_pid = fork()) == 0)
    {
        // Sono nel processo mapper
        SYSCALL_EXIT(dup2(pipe_main_to_mapper[0], STDIN_FILENO), "dup2 mapper stdin fallita");
        SYSCALL_EXIT(dup2(pipe_mapper_to_reducer[1], STDOUT_FILENO), "dup2 mapper stdout fallita");

        SYSCALL_EXIT(close(pipe_main_to_mapper[0]), "close mapper pipe_main_to_mapper[0] fallita");
        SYSCALL_EXIT(close(pipe_main_to_mapper[1]), "close mapper pipe_main_to_mapper[1] fallita");
        SYSCALL_EXIT(close(pipe_mapper_to_reducer[0]), "close mapper pipe_mapper_to_reducer[0] fallita");
        SYSCALL_EXIT(close(pipe_mapper_to_reducer[1]), "close mapper pipe_mapper_to_reducer[1] fallita");
        SYSCALL_EXIT(close(pipe_reducer_to_main[0]), "close mapper pipe_reducer_to_main[0] fallita");
        SYSCALL_EXIT(close(pipe_reducer_to_main[1]), "close mapper pipe_reducer_to_main[1] fallita");

        MR_LOG(mr, "INFO", "Processo mapper creato");
        mapper_process_main(mr, STDIN_FILENO, STDOUT_FILENO);

        _exit(EXIT_SUCCESS);
    }
    else {
        CHECK_ERROR(mr->mapper_pid, "Fork mapper fallita");
    }

    if ((mr->reducer_pid = fork()) == 0)
    {
        // Sono nel processo reducer
        SYSCALL_EXIT(dup2(pipe_mapper_to_reducer[0], STDIN_FILENO), "dup2 reducer stdin fallita");
        SYSCALL_EXIT(dup2(pipe_reducer_to_main[1], STDOUT_FILENO), "dup2 reducer stdout fallita");

        SYSCALL_EXIT(close(pipe_main_to_mapper[0]), "close reducer pipe_main_to_mapper[0] fallita");
        SYSCALL_EXIT(close(pipe_main_to_mapper[1]), "close reducer pipe_main_to_mapper[1] fallita");
        SYSCALL_EXIT(close(pipe_mapper_to_reducer[0]), "close reducer pipe_mapper_to_reducer[0] fallita");
        SYSCALL_EXIT(close(pipe_mapper_to_reducer[1]), "close reducer pipe_mapper_to_reducer[1] fallita");
        SYSCALL_EXIT(close(pipe_reducer_to_main[0]), "close reducer pipe_reducer_to_main[0] fallita");
        SYSCALL_EXIT(close(pipe_reducer_to_main[1]), "close reducer pipe_reducer_to_main[1] fallita");

        MR_LOG(mr, "INFO", "Processo reducer creato");
        reducer_process_main(mr, STDIN_FILENO, STDOUT_FILENO);

        _exit(EXIT_SUCCESS);
    }
    else {
        CHECK_ERROR(mr->reducer_pid, "Fork reducer fallita");
    }

    CHECK_ERROR(close(pipe_main_to_mapper[0]), "close parent pipe_main_to_mapper[0] fallita");
    CHECK_ERROR(close(pipe_mapper_to_reducer[0]), "close parent pipe_mapper_to_reducer[0] fallita");
    CHECK_ERROR(close(pipe_mapper_to_reducer[1]), "close parent pipe_mapper_to_reducer[1] fallita");
    CHECK_ERROR(close(pipe_reducer_to_main[1]), "close parent pipe_reducer_to_main[1] fallita");

    mr->main_to_mapper_write = pipe_main_to_mapper[1];
    mr->reducer_to_main_read = pipe_reducer_to_main[0];

    int input_status = 0;
    if (input_path_type == PATH_FILE)
    {
        input_status = process_single_file(mr, input_path);
    }
    else if (input_path_type == PATH_DIRECTORY)
    {
        input_status = process_multiple_files(mr, input_path);
    }

    if (input_status != 0) {
        MR_LOG(mr, "ERROR", "mr_start: errore durante l'elaborazione dell'input");
    }

    CHECK_ERROR(close(mr->main_to_mapper_write), "close main_to_mapper_write fallita");
    mr->main_to_mapper_write = -1;

    /* ricevo da reducer e scrivo su output (leggo tutti i record, ordino e
       scrivo file di output in formato: int token_len; token bytes; int result_len; result bytes) */

    out_entry_t *outs = NULL;
    size_t outs_count = 0, outs_cap = 0;

    while (1) {
        int token_len = 0;
        ssize_t rn = readn(mr->reducer_to_main_read, &token_len, sizeof(int));
        if (rn == 0) break; /* EOF */
        CHECK_ERROR(rn, "mr_start: errore lettura token_len");

        int result_len = 0;
        CHECK_ERROR(readn(mr->reducer_to_main_read, &result_len, sizeof(int)), "mr_start: errore lettura result_len");

        if (token_len < 0 || result_len < 0) { 
            MR_LOG(mr, "ERROR", "mr_start: lunghezze negative"); 
            return -1;
        }

        char *token = NULL;
        if (token_len > 0) {
            SYSNCALL_EXIT(token = malloc((size_t)token_len + 1), "malloc token fallita");
            if (readn(mr->reducer_to_main_read, token, token_len) != token_len) { free(token); return -1; }
            token[token_len] = '\0';
        } else {
            SYSNCALL_EXIT(token = strdup(""), "strdup token fallita");
        }

        void *result = NULL;
        if (result_len > 0) {
            SYSNCALL_EXIT(result = malloc((size_t)result_len), "malloc result fallita");
            if (readn(mr->reducer_to_main_read, result, result_len) != result_len) { free(token); free(result); return -1; }
        }

        if (outs_count == outs_cap) {
            size_t nc = outs_cap ? outs_cap * 2 : 256;
            SYSNCALL_EXIT(outs = realloc(outs, nc * sizeof(*outs)), "realloc outs fallita");
            outs_cap = nc;
        }

        outs[outs_count].token = token;
        outs[outs_count].token_len = token_len;
        outs[outs_count].result = result;
        outs[outs_count].result_len = result_len;
        outs_count++;
    }

    /* Chiudo descrittore di lettura dalla pipe del reducer */
    CHECK_ERROR(close(mr->reducer_to_main_read), "close reducer_to_main_read fallita");
    mr->reducer_to_main_read = -1;

    /* Ordino lessicograficamente per token per garantire determinismo */
    if (outs_count > 1) qsort(outs, outs_count, sizeof(*outs), out_cmp);

    /* Scrivo su file di output (sovrascrivo/creo) */
    char log_buf[256];
    snprintf(log_buf, sizeof(log_buf), "Apertura file di output: %s", output_path);
    MR_LOG(mr, "INFO", log_buf);

    FILE *ofd = fopen(output_path, "wb");
    if (!ofd) {
        MR_LOG(mr, "ERROR", "mr_start: impossibile aprire output_path");
        return -1;
    } else {
        for (size_t i = 0; i < outs_count; i++) {
            int tlen = outs[i].token_len;
            int rlen = outs[i].result_len;
            /* scrivo: token_len, token, result_len, result */
            if (fwrite(&tlen, sizeof(int), 1, ofd) != 1) { MR_LOG(mr, "ERROR", "mr_start: fwrite token_len fallita"); break; }
            if (tlen > 0) fwrite(outs[i].token, 1, tlen, ofd);
            if (fwrite(&rlen, sizeof(int), 1, ofd) != 1) { MR_LOG(mr, "ERROR", "mr_start: fwrite result_len fallita"); break; }
            if (rlen > 0) fwrite(outs[i].result, 1, rlen, ofd);
        }
        fclose(ofd);
    }
    snprintf(log_buf, sizeof(log_buf), "Chiusura file di output: %s", output_path);
    MR_LOG(mr, "INFO", log_buf);

    /* Pulizia */
    for (size_t i = 0; i < outs_count; i++) {
        free(outs[i].token);
        if (outs[i].result) free(outs[i].result);
    }
    free(outs);

    CHECK_ERROR(waitpid(mr->mapper_pid, NULL, 0), "waitpid mapper fallita");
    CHECK_ERROR(waitpid(mr->reducer_pid, NULL, 0), "waitpid reducer fallita");

    return (input_status == 0) ? 0 : -1;
}

int mr_destroy(mr_t mr)
{
    if (mr == NULL)
    {
        return -1;
    }

    if (mr->main_to_mapper_write != -1)
    {
        CHECK_ERROR(close(mr->main_to_mapper_write), "close mr_destroy main_to_mapper_write fallita");
    }
    if (mr->reducer_to_main_read != -1)
    {
        CHECK_ERROR(close(mr->reducer_to_main_read), "close mr_destroy reducer_to_main_read fallita");
    }

    if (mr->attr.log_file)
    {
        free((char *)mr->attr.log_file);
    }

    // 3. Libera la memoria della struttura principale.
    free(mr);

    return 0;
}

/* --- Implementazione funzioni ausiliarie ---*/

static int process_single_file(mr_t mr, const char *path)
{
    if (!mr || !path)
    {
        return -1;
    }

    FILE *fd = fopen(path, "r");
    if (fd == NULL)
    {
        MR_LOG(mr, "ERROR", "Impossibile aprire il file di input");
        return -1;
    }

    char log_buf[256];
    snprintf(log_buf, sizeof(log_buf), "Apertura file di input: %s", path);
    MR_LOG(mr, "INFO", log_buf);

    char *line = NULL;
    size_t len = 0;
    ssize_t nread;
    uint32_t line_num = 1;
    size_t path_len = strlen(path);

    while ((nread = getline(&line, &len, fd)) != -1)
    {
        if (nread > 0 && line[nread - 1] == '\n')
        {
            line[nread - 1] = '\0';
            nread--;
        }

        int line_len_int = (int)nread;
        int path_len_int = (int)path_len;
        int line_num_int = (int)line_num;

        writen(mr->main_to_mapper_write, &path_len_int, sizeof(int));
        writen(mr->main_to_mapper_write, path, path_len);
        writen(mr->main_to_mapper_write, &line_num_int, sizeof(int));
        writen(mr->main_to_mapper_write, &line_len_int, sizeof(int));
        writen(mr->main_to_mapper_write, line, line_len_int);

        line_num++;
    }

    snprintf(log_buf, sizeof(log_buf), "Chiusura file di input: %s", path);
    MR_LOG(mr, "INFO", log_buf);

    fclose(fd);

    if (line)
        free(line);

    return 0;
}

static int process_multiple_files(mr_t mr, const char *path)
{
    struct dirent **namelist;
    int n;

    MR_LOG(mr, "INFO", "Inizio scansione directory di input per elaborazione multipla");

    // Scansioniamo la directory. alphasort ordina i file in modo lessicografico (A-Z)
    n = scandir(path, &namelist, NULL, alphasort);
    if (n < 0) {
        MR_LOG(mr, "ERROR", "Impossibile aprire o scansionare la directory di input");
        return -1;
    }

    int status = 0;

    // Iteriamo attraverso tutti i file trovati nella directory
    for (int i = 0; i < n; i++) {
        // Consideriamo solo i file regolari (ignoriamo cartelle annidate, link, ecc.)
        if (namelist[i]->d_type == DT_REG) {
            char full_path[4096]; // Dimensione di sicurezza standard per i path nei sistemi POSIX
            
            // Costruiamo il percorso completo: directory/nome_file
            snprintf(full_path, sizeof(full_path), "%s/%s", path, namelist[i]->d_name);

            // Passiamo il file singolo alla funzione che abbiamo già scritto
            if (process_single_file(mr, full_path) != 0) {
                MR_LOG(mr, "ERROR", "Errore durante l'elaborazione del file nella directory");
                status = -1; // Tracciamo il fallimento ma continuiamo a liberare la memoria
            }
        }
        
        // Liberiamo la memoria della singola voce allocata da scandir
        free(namelist[i]);
    }
    
    // Liberiamo l'array di puntatori principale
    free(namelist);

    if (status == 0) {
        MR_LOG(mr, "INFO", "Elaborazione della directory completata con successo");
    }

    return status;
}

/* --- Mapper Process --- */

typedef struct {
    char *file_name;
    size_t file_name_len;
    unsigned long line_number;
    char *line;
    size_t line_len;
} internal_line_t;

typedef struct {
    internal_line_t *buffer;
    size_t head;
    size_t tail;
    size_t size;
    size_t max_size;
    int eof;
    mtx_t mtx;
    cnd_t not_empty;
    cnd_t not_full;
} line_queue_t;

typedef struct {
    struct mr *mr;
    int pipe_in;
    int pipe_out;
    line_queue_t queue;
    mtx_t pipe_out_mtx;
    size_t lines_read;
    size_t pairs_produced;
    mtx_t stats_mtx;
} mapper_ctx_t;

static int mapper_emit(const char *token, const void *value, size_t value_size, void *emit_arg) {
    if (!token) return -1;
    mapper_ctx_t *ctx = (mapper_ctx_t *)emit_arg;
    
    int token_len = strlen(token);
    int val_len = (int)value_size;
    
    if (val_len < 0) return -1;

    mtx_lock(&ctx->pipe_out_mtx);
    
    writen(ctx->pipe_out, &token_len, sizeof(int));
    writen(ctx->pipe_out, &val_len, sizeof(int));
    writen(ctx->pipe_out, token, token_len);
    if(value && val_len > 0) {
        writen(ctx->pipe_out, value, val_len);
    }
        
    mtx_unlock(&ctx->pipe_out_mtx);

    mtx_lock(&ctx->stats_mtx);
    ctx->pairs_produced++;
    mtx_unlock(&ctx->stats_mtx);

    return 0;
}

static int reader_main(void *arg) {
    mapper_ctx_t *ctx = (mapper_ctx_t *)arg;
    MR_LOG(ctx->mr, "INFO", "Mapper reader thread started");
    
    while(1) {
        int path_len_int = 0;
        ssize_t n = readn(ctx->pipe_in, &path_len_int, sizeof(int));
        if (n == 0) break; // EOF
        CHECK_ERROR(n, "Mapper reader: error reading path_len");
        
        char *path;
        SYSNCALL_EXIT(path = malloc(path_len_int + 1), "malloc path fallita");
        CHECK_ERROR(readn(ctx->pipe_in, path, path_len_int), "Mapper reader: error reading path");
        path[path_len_int] = '\0';
        
        int line_num_int = 0;
        CHECK_ERROR(readn(ctx->pipe_in, &line_num_int, sizeof(int)), "Mapper reader: error reading line_num");
        
        int line_len_int = 0;
        CHECK_ERROR(readn(ctx->pipe_in, &line_len_int, sizeof(int)), "Mapper reader: error reading line_len");
        if (line_len_int < 0) line_len_int = 0;

        char *line;
        SYSNCALL_EXIT(line = malloc(line_len_int + 1), "malloc line fallita");
        if (line_len_int > 0) {
            CHECK_ERROR(readn(ctx->pipe_in, line, line_len_int), "Mapper reader: error reading line content");
        }
        line[line_len_int] = '\0';
        
        internal_line_t iline;
        iline.file_name = path;
        iline.file_name_len = path_len_int;
        iline.line_number = line_num_int;
        iline.line = line;
        iline.line_len = line_len_int;
        
        mtx_lock(&ctx->queue.mtx);
        while(ctx->queue.size == ctx->queue.max_size) {
            cnd_wait(&ctx->queue.not_full, &ctx->queue.mtx);
        }
        
        ctx->queue.buffer[ctx->queue.tail] = iline;
        ctx->queue.tail = (ctx->queue.tail + 1) % ctx->queue.max_size;
        ctx->queue.size++;
        
        cnd_signal(&ctx->queue.not_empty);
        mtx_unlock(&ctx->queue.mtx);

        mtx_lock(&ctx->stats_mtx);
        ctx->lines_read++;
        mtx_unlock(&ctx->stats_mtx);
    }
    
    mtx_lock(&ctx->queue.mtx);
    ctx->queue.eof = 1;
    cnd_broadcast(&ctx->queue.not_empty);
    mtx_unlock(&ctx->queue.mtx);
    
    MR_LOG(ctx->mr, "INFO", "Mapper reader thread ended");
    return 0;
}

static int mapper_worker_main(void *arg) {
    mapper_ctx_t *ctx = (mapper_ctx_t *)arg;
    MR_LOG(ctx->mr, "INFO", "Mapper worker thread started");
    
    while(1) {
        mtx_lock(&ctx->queue.mtx);
        while(ctx->queue.size == 0 && !ctx->queue.eof) {
            cnd_wait(&ctx->queue.not_empty, &ctx->queue.mtx);
        }
        
        if (ctx->queue.size == 0 && ctx->queue.eof) {
            mtx_unlock(&ctx->queue.mtx);
            break;
        }
        
        internal_line_t iline = ctx->queue.buffer[ctx->queue.head];
        ctx->queue.head = (ctx->queue.head + 1) % ctx->queue.max_size;
        ctx->queue.size--;
        
        cnd_signal(&ctx->queue.not_full);
        mtx_unlock(&ctx->queue.mtx);
        
        mr_file_line_t mr_line;
        mr_line.file_name = iline.file_name;
        mr_line.file_name_len = iline.file_name_len;
        mr_line.line_number = iline.line_number;
        mr_line.line = iline.line;
        mr_line.line_len = iline.line_len;
        
        ctx->mr->user_mapper(&mr_line, mapper_emit, ctx, ctx->mr->user_arg);
        
        free(iline.file_name);
        free(iline.line);
    }
    
    MR_LOG(ctx->mr, "INFO", "Mapper worker thread ended");
    return 0;
}

static void mapper_process_main(struct mr *mr, int pipe_in, int pipe_out)
{
    mapper_ctx_t ctx;
    ctx.mr = mr;
    ctx.pipe_in = pipe_in;
    ctx.pipe_out = pipe_out;
    ctx.lines_read = 0;
    ctx.pairs_produced = 0;
    
    ctx.queue.max_size = mr->attr.queue_size;
    SYSNCALL_EXIT(ctx.queue.buffer = malloc(sizeof(internal_line_t) * ctx.queue.max_size), "malloc mapper buffer fallita");
    ctx.queue.head = 0;
    ctx.queue.tail = 0;
    ctx.queue.size = 0;
    ctx.queue.eof = 0;
    mtx_init(&ctx.queue.mtx, mtx_plain);
    cnd_init(&ctx.queue.not_empty);
    cnd_init(&ctx.queue.not_full);
    mtx_init(&ctx.pipe_out_mtx, mtx_plain);
    mtx_init(&ctx.stats_mtx, mtx_plain);

    thrd_t reader_thrd;
    thrd_create(&reader_thrd, reader_main, &ctx);
    
    thrd_t *worker_thrds;
    SYSNCALL_EXIT(worker_thrds = malloc(sizeof(thrd_t) * mr->attr.mapper_threads), "malloc mapper worker_thrds fallita");
    for (size_t i = 0; i < mr->attr.mapper_threads; i++) {
        thrd_create(&worker_thrds[i], mapper_worker_main, &ctx);
    }
    
    thrd_join(reader_thrd, NULL);
    for (size_t i = 0; i < mr->attr.mapper_threads; i++) {
        thrd_join(worker_thrds[i], NULL);
    }

    char log_msg[128];
    snprintf(log_msg, sizeof(log_msg), "Mapper: righe lette = %zu, coppie prodotte = %zu", ctx.lines_read, ctx.pairs_produced);
    MR_LOG(mr, "INFO", log_msg);
    
    free(ctx.queue.buffer);
    free(worker_thrds);
    mtx_destroy(&ctx.queue.mtx);
    cnd_destroy(&ctx.queue.not_empty);
    cnd_destroy(&ctx.queue.not_full);
    mtx_destroy(&ctx.pipe_out_mtx);
    mtx_destroy(&ctx.stats_mtx);
}

static int reducer_reader_main(void *arg)
{
    reducer_reader_ctx_t *ctx = (reducer_reader_ctx_t *)arg;
    const int MAX_FIELD = 10 * 1024 * 1024;
    MR_LOG(ctx->mr, "INFO", "Reducer reader thread started");

    while (1) {
        mr_pair_header_t header;
        ssize_t rn = readn(ctx->pipe_in, &header, sizeof(header));
        if (rn == 0) break;
        CHECK_ERROR(rn, "Reducer reader: errore lettura header");

        if (header.token_len < 0 || header.value_len < 0 ||
            header.token_len > MAX_FIELD || header.value_len > MAX_FIELD) {
            MR_LOG(ctx->mr, "ERROR", "Reducer reader: dimensione campo non valida");
            return -1;
        }

        char *token;
        SYSNCALL_EXIT(token = malloc((size_t)header.token_len + 1), "malloc token fallita");
        if (header.token_len > 0) {
            if (readn(ctx->pipe_in, token, header.token_len) != header.token_len) {
                free(token);
                return -1;
            }
        }
        token[header.token_len] = '\0';

        void *value = NULL;
        if (header.value_len > 0) {
            SYSNCALL_EXIT(value = malloc((size_t)header.value_len), "malloc value fallita");
            if (readn(ctx->pipe_in, value, header.value_len) != header.value_len) {
                free(token);
                free(value);
                return -1;
            }
        }

        if (ctx->pairs_count == ctx->pairs_cap) {
            size_t newcap = ctx->pairs_cap ? ctx->pairs_cap * 2 : 256;
            SYSNCALL_EXIT(ctx->pairs = realloc(ctx->pairs, newcap * sizeof(*ctx->pairs)), "realloc pairs fallita");
            ctx->pairs_cap = newcap;
        }

        ctx->pairs[ctx->pairs_count].token = token;
        ctx->pairs[ctx->pairs_count].token_len = header.token_len;
        ctx->pairs[ctx->pairs_count].value = value;
        ctx->pairs[ctx->pairs_count].value_len = header.value_len;
        ctx->pairs_count++;
    }

    MR_LOG(ctx->mr, "INFO", "Reducer reader thread ended");
    return 0;
}

static int reducer_worker_main(void *arg)
{
    reducer_worker_ctx_t *ctx = (reducer_worker_ctx_t *)arg;
    MR_LOG(ctx->mr, "INFO", "Reducer worker thread started");

    while (1) {
        size_t group_index;

        mtx_lock(&ctx->next_group_mtx);
        if (ctx->next_group >= ctx->groups_count) {
            mtx_unlock(&ctx->next_group_mtx);
            break;
        }
        group_index = ctx->next_group++;
        mtx_unlock(&ctx->next_group_mtx);

        reduce_group_t *group = &ctx->groups[group_index];
        ctx->mr->user_reducer(group->token, group->values, group->values_count,
                              (mr_emit_result_t)reducer_emit_top, ctx->emit_ctx,
                              ctx->mr->user_arg);
    }

    MR_LOG(ctx->mr, "INFO", "Reducer worker thread ended");
    return 0;
}

static void reducer_process_main(struct mr *mr, int pipe_in, int pipe_out)
{
    reducer_reader_ctx_t reader_ctx;
    reader_ctx.mr = mr;
    reader_ctx.pipe_in = pipe_in;
    reader_ctx.pairs = NULL;
    reader_ctx.pairs_count = 0;
    reader_ctx.pairs_cap = 0;

    thrd_t reader_thrd;
    if (thrd_create(&reader_thrd, reducer_reader_main, &reader_ctx) != thrd_success) {
        MR_LOG(mr, "ERROR", "Reducer: impossibile creare reader thread");
        close(pipe_out);
        return;
    }

    thrd_join(reader_thrd, NULL);

    pair_entry_t *pairs = reader_ctx.pairs;
    size_t pairs_count = reader_ctx.pairs_count;

    reducer_emit_ctx_t emit_ctx;
    emit_ctx.out_fd = pipe_out;
    emit_ctx.results_emitted = 0;
    mtx_init(&emit_ctx.out_mtx, mtx_plain);
    mtx_init(&emit_ctx.stats_mtx, mtx_plain);

    if (pairs_count == 0) {
        close(pipe_out);
        mtx_destroy(&emit_ctx.out_mtx);
        mtx_destroy(&emit_ctx.stats_mtx);
        MR_LOG(mr, "INFO", "Reducer: nessuna coppia ricevuta");
        return;
    }

    // 1. Ordina per token
    qsort(pairs, pairs_count, sizeof(*pairs), cmp_pairs);

    // 2. Conta i gruppi (token distinti)
    size_t groups_count = 1;
    for (size_t k = 1; k < pairs_count; k++) {
        if (strcmp(pairs[k].token, pairs[k-1].token) != 0) groups_count++;
    }

    // 3. Alloca i gruppi
    reduce_group_t *groups;
    SYSNCALL_EXIT(groups = malloc(sizeof(reduce_group_t) * groups_count), "malloc groups fallita");

    size_t g_idx = 0;
    size_t i = 0;
    while (i < pairs_count) {
        size_t j = i + 1;
        while (j < pairs_count && strcmp(pairs[i].token, pairs[j].token) == 0) j++;

        size_t values_count = j - i;
        groups[g_idx].token = pairs[i].token;
        groups[g_idx].token_len = pairs[i].token_len;
        groups[g_idx].values_count = values_count;
        SYSNCALL_EXIT(groups[g_idx].values = malloc(sizeof(mr_value_t) * values_count), "malloc group values fallita");

        for (size_t k = 0; k < values_count; k++) {
            groups[g_idx].values[k].data = pairs[i + k].value;
            groups[g_idx].values[k].size = (size_t)pairs[i + k].value_len;
        }

        g_idx++;
        i = j;
    }

    // 4. Elaborazione parallela
    reducer_worker_ctx_t worker_ctx;
    worker_ctx.mr = mr;
    worker_ctx.emit_ctx = &emit_ctx;
    worker_ctx.groups = groups;
    worker_ctx.groups_count = groups_count;
    worker_ctx.next_group = 0;
    mtx_init(&worker_ctx.next_group_mtx, mtx_plain);

    size_t worker_count = mr->attr.reducer_threads > 0 ? mr->attr.reducer_threads : 1;
    thrd_t *workers;
    SYSNCALL_EXIT(workers = malloc(sizeof(thrd_t) * worker_count), "malloc reducer workers fallita");

    for (size_t t = 0; t < worker_count; t++) {
        if (thrd_create(&workers[t], reducer_worker_main, &worker_ctx) != thrd_success) {
            MR_LOG(mr, "ERROR", "Reducer: impossibile creare worker thread");
            // Se fallisce la creazione di un thread, andiamo avanti con quelli creati
            worker_count = t;
            break;
        }
    }
    for (size_t t = 0; t < worker_count; t++) {
        thrd_join(workers[t], NULL);
    }
    free(workers);

    mtx_destroy(&worker_ctx.next_group_mtx);

    char log_msg[128];
    snprintf(log_msg, sizeof(log_msg), "Reducer: token distinti = %zu, risultati prodotti = %zu", groups_count, emit_ctx.results_emitted);
    MR_LOG(mr, "INFO", log_msg);

    // 5. Cleanup
    for (size_t g = 0; g < groups_count; g++) {
        free(groups[g].values);
    }
    free(groups);

    for (size_t k = 0; k < pairs_count; k++) {
        free(pairs[k].token);
        if (pairs[k].value) free(pairs[k].value);
    }
    free(pairs);

    close(pipe_out);
    mtx_destroy(&emit_ctx.out_mtx);
    mtx_destroy(&emit_ctx.stats_mtx);
}