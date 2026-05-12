#include <stdio.h>
#include <stdlib.h>
#include "mr.h"

// Funzioni di esempio (per ora gusci vuoti per compilare)
int my_mapper(const mr_file_line_t *line, mr_emit_pair_t emit, void *emit_arg, void *user_arg) {
    (void)line; (void)emit; (void)emit_arg; (void)user_arg;
    return 0;
}

int my_reducer(const char *token, const mr_value_t *values, size_t values_count, mr_emit_result_t emit, void *emit_arg, void *user_arg) {
    (void)token; (void)values; (void)values_count; (void)emit; (void)emit_arg; (void)user_arg;
    return 0;
}

int main() {
    mr_t mr;
    mr_attr_t attr;

    if (mr_attr_init(&attr) == -1) return 1;
    
    // Configurazione base
    mr_attr_set_mapper_threads(&attr, 2); 
    mr_attr_set_reducer_threads(&attr, 2);

    if (mr_create(&mr, &attr, my_mapper, my_reducer, NULL) == -1) {
        perror("mr_create");
        return 1;
    }

    printf("Framework creato correttamente!\n");

    mr_destroy(mr); 
    mr_attr_destroy(&attr);
    return 0;
}