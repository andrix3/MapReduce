#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "mr.h"

int my_mapper(const mr_file_line_t *line, mr_emit_pair_t emit, void *arg, void *u) {
    (void)u;
    char buf[256]; size_t ti = 0;
    for (size_t i=0; i<=line->line_len; i++) {
        char c = (i < line->line_len) ? line->line[i] : '\0';
        if (isalnum(c)) { if(ti<255) buf[ti++] = tolower(c); }
        else if (ti > 0) { buf[ti] = '\0'; int one = 1; emit(buf, &one, sizeof(int), arg); ti = 0; }
    }
    return 0;
}

int my_reducer(const char *t, const mr_value_t *vs, size_t vc, mr_emit_result_t emit, void *arg, void *u) {
    (void)u;
    long sum = 0;
    for (size_t i=0; i<vc; i++) sum += *(int*)vs[i].data;
    char out[32]; int l = snprintf(out, 32, "%ld", sum);
    emit(t, out, l, arg); return 0;
}

int main(int argc, char **argv) {
    if (argc != 3) { fprintf(stderr, "Uso: %s <input> <output>\n", argv[0]); return 1; }
    mr_t mr; mr_attr_t at; mr_attr_init(&at);
    mr_attr_set_mapper_threads(&at, 2); mr_attr_set_reducer_threads(&at, 2);
    if (mr_create(&mr, &at, my_mapper, my_reducer, NULL) == 0) {
        mr_start(mr, argv[1], argv[2]); mr_destroy(mr);
    }
    return 0;
}
