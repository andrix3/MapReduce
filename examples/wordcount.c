#include <stdio.h>
#include <stdlib.h>
#include "mr.h"
 #include <string.h>
 #include <ctype.h>

 /* Mapper: estrae token "parole" dalla riga e emette <token, 1> (int)
    Token: sequenza di caratteri alfanumerici; case-insensitive (convertiamo in lower).
 */
 int my_mapper(const mr_file_line_t *line, mr_emit_pair_t emit, void *emit_arg, void *user_arg) {
     (void)emit_arg; (void)user_arg;
     if (!line || !line->line) return 0;

     const char *s = line->line;
     size_t len = line->line_len;
     char token[256];
     size_t ti = 0;

     for (size_t i = 0; i <= len; ++i) {
         char c = (i < len) ? s[i] : '\0';
         if (isalnum((unsigned char)c)) {
             if (ti + 1 < sizeof(token)) token[ti++] = tolower((unsigned char)c);
         } else {
             if (ti > 0) {
                 token[ti] = '\0';
                 int one = 1;
                 emit(token, &one, sizeof(one), emit_arg);
                 ti = 0;
             }
         }
     }

     return 0;
 }

 /* Reducer: somma i valori (assumiamo int binari emessi dal mapper) e emette
    il risultato come stringa (decimal) per facilità di lettura nel file output.
 */
 int my_reducer(const char *token, const mr_value_t *values, size_t values_count, mr_emit_result_t emit, void *emit_arg, void *user_arg) {
     (void)emit_arg; (void)user_arg;
     if (!token || (!values && values_count > 0)) return -1;

     long sum = 0;
     for (size_t i = 0; i < values_count; ++i) {
         const mr_value_t *v = &values[i];
         if (v->size == sizeof(int)) {
             int tmp = 0;
             memcpy(&tmp, v->data, sizeof(tmp));
             sum += tmp;
         } else if (v->data && v->size > 0) {
             /* Fallback se il valore è una stringa numerica */
             char buf[64];
             size_t n = (v->size < (sizeof(buf)-1)) ? v->size : (sizeof(buf)-1);
             memcpy(buf, v->data, n);
             buf[n] = '\0';
             sum += strtol(buf, NULL, 10);
         }
     }

     char out[32];
     int l = snprintf(out, sizeof(out), "%ld", sum);
     if (l > 0) emit(token, out, (size_t)l, emit_arg);
     return 0;
 }

 int main(int argc, char **argv) {
     if (argc != 3) {
         fprintf(stderr, "Usage: %s <input_path> <output_path>\n", argv[0]);
         return 1;
     }

     const char *input = argv[1];
     const char *output = argv[2];

     mr_t mr;
     mr_attr_t attr;

     if (mr_attr_init(&attr) == -1) return 1;
     mr_attr_set_mapper_threads(&attr, 2);
     mr_attr_set_reducer_threads(&attr, 2);

     if (mr_create(&mr, &attr, my_mapper, my_reducer, NULL) == -1) {
         perror("mr_create");
         mr_attr_destroy(&attr);
         return 1;
     }

     if (mr_start(mr, input, output) == -1) {
         fprintf(stderr, "mr_start failed\n");
         mr_destroy(mr);
         mr_attr_destroy(&attr);
         return 1;
     }

     mr_destroy(mr);
     mr_attr_destroy(&attr);
     printf("Wordcount completato. Output scritto in %s\n", output);
     return 0;
 }