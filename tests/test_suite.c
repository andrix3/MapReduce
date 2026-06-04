#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "mr.h"

/* --- Test 1: Word Count --- */

int wc_mapper(const mr_file_line_t *line, mr_emit_pair_t emit, void *emit_arg, void *user_arg) {
    (void)user_arg;
    char *s = strdup(line->line);
    char *token = strtok(s, " \t\n\r");
    while (token) {
        int one = 1;
        emit(token, &one, sizeof(int), emit_arg);
        token = strtok(NULL, " \t\n\r");
    }
    free(s);
    return 0;
}

int wc_reducer(const char *token, const mr_value_t *values, size_t values_count, mr_emit_result_t emit, void *emit_arg, void *user_arg) {
    (void)user_arg;
    int sum = 0;
    for (size_t i = 0; i < values_count; i++) {
        sum += *(int *)values[i].data;
    }
    emit(token, &sum, sizeof(int), emit_arg);
    return 0;
}

void test_wordcount() {
    printf("Running Test: Word Count...\n");
    mr_t mr;
    mr_attr_t attr;
    mr_attr_init(&attr);
    mr_attr_set_mapper_threads(&attr, 2);
    mr_attr_set_reducer_threads(&attr, 2);

    // Create a temporary input file
    system("mkdir -p test_input");
    system("echo 'hello world hello' > test_input/f1.txt");
    system("echo 'world world' > test_input/f2.txt");

    assert(mr_create(&mr, &attr, wc_mapper, wc_reducer, NULL) == 0);
    assert(mr_start(mr, "test_input", "test_output.mro") == 0);

    // Verify output
    FILE *f = fopen("test_output.mro", "rb");
    assert(f != NULL);
    int tlen, rlen, val;
    char buf[256];
    int found_hello = 0, found_world = 0;

    while (fread(&tlen, sizeof(int), 1, f) == 1) {
        fread(buf, 1, tlen, f);
        buf[tlen] = '\0';
        fread(&rlen, sizeof(int), 1, f);
        fread(&val, 1, rlen, f);
        if (strcmp(buf, "hello") == 0) { assert(val == 2); found_hello = 1; }
        if (strcmp(buf, "world") == 0) { assert(val == 3); found_world = 1; }
    }
    assert(found_hello && found_world);
    fclose(f);

    mr_destroy(mr);
    mr_attr_destroy(&attr);
    system("rm -rf test_input test_output.mro");
    printf("Test Word Count: PASSED\n");
}

/* --- Test 2: Binary Data --- */

int bin_mapper(const mr_file_line_t *line, mr_emit_pair_t emit, void *emit_arg, void *user_arg) {
    (void)user_arg;
    // Emit the line length as binary data
    int len = (int)line->line_len;
    emit("length", &len, sizeof(int), emit_arg);
    return 0;
}

int bin_reducer(const char *token, const mr_value_t *values, size_t values_count, mr_emit_result_t emit, void *emit_arg, void *user_arg) {
    (void)user_arg;
    (void)token;
    int max = 0;
    for (size_t i = 0; i < values_count; i++) {
        int v = *(int *)values[i].data;
        if (v > max) max = v;
    }
    emit("max_len", &max, sizeof(int), emit_arg);
    return 0;
}

void test_binary() {
    printf("Running Test: Binary Data...\n");
    mr_t mr;
    mr_attr_t attr;
    mr_attr_init(&attr);

    system("mkdir -p test_input_bin");
    system("echo '123' > test_input_bin/f1.txt");
    system("echo '12345' > test_input_bin/f2.txt");

    assert(mr_create(&mr, &attr, bin_mapper, bin_reducer, NULL) == 0);
    assert(mr_start(mr, "test_input_bin", "test_output_bin.mro") == 0);

    FILE *f = fopen("test_output_bin.mro", "rb");
    int tlen, rlen, val;
    char buf[256];
    assert(fread(&tlen, sizeof(int), 1, f) == 1);
    fread(buf, 1, tlen, f);
    fread(&rlen, sizeof(int), 1, f);
    fread(&val, 1, rlen, f);
    assert(val == 5);
    fclose(f);

    mr_destroy(mr);
    mr_attr_destroy(&attr);
    system("rm -rf test_input_bin test_output_bin.mro");
    printf("Test Binary Data: PASSED\n");
}

int main() {
    test_wordcount();
    test_binary();
    printf("\nALL TESTS PASSED!\n");
    return 0;
}
