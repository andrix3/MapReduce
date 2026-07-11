#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mr.h"

// Helper struct for verification
typedef struct {
    char token[256];
    int val;
} test_result_t;

static int read_mro(const char *path, test_result_t *out, size_t max_results, size_t *count) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    size_t i = 0;
    while (i < max_results) {
        int tlen = 0;
        if (fread(&tlen, sizeof(int), 1, f) != 1) break;
        if (tlen < 0 || tlen >= 256) { fclose(f); return -1; }
        if (fread(out[i].token, 1, (size_t)tlen, f) != (size_t)tlen) { fclose(f); return -1; }
        out[i].token[tlen] = '\0';
        int rlen = 0;
        if (fread(&rlen, sizeof(int), 1, f) != 1) { fclose(f); return -1; }
        if (rlen != sizeof(int)) {
            // Skip result bytes if not int size
            fseek(f, rlen, SEEK_CUR);
            out[i].val = -1;
        } else {
            if (fread(&out[i].val, 1, (size_t)rlen, f) != (size_t)rlen) { fclose(f); return -1; }
        }
        i++;
    }
    *count = i;
    fclose(f);
    return 0;
}

// User callbacks for simple wordcount/counting
static int map_emit_ones(const mr_file_line_t *l, mr_emit_pair_t e, void *a, void *u) {
    (void)l; (void)u;
    int v = 1;
    e("stress", &v, sizeof(int), a);
    return 0;
}

static int reduce_sum_ones(const char *t, const mr_value_t *vs, size_t vc, mr_emit_result_t e, void *a, void *u) {
    (void)u;
    int sum = 0;
    for (size_t i = 0; i < vc; i++) {
        sum += *(int*)vs[i].data;
    }
    e(t, &sum, sizeof(int), a);
    return 0;
}

// User callbacks for checking line length
static int map_line_len(const mr_file_line_t *l, mr_emit_pair_t e, void *a, void *u) {
    (void)u;
    int len = (int)l->line_len;
    e("length", &len, sizeof(int), a);
    return 0;
}

static int reduce_max_len(const char *t, const mr_value_t *vs, size_t vc, mr_emit_result_t e, void *a, void *u) {
    (void)u;
    int max = 0;
    for (size_t i = 0; i < vc; i++) {
        int val = *(int*)vs[i].data;
        if (val > max) max = val;
    }
    e(t, &max, sizeof(int), a);
    return 0;
}

// Test case implementations
static void test_empty_file() {
    printf("Running test: Empty Input File...\n");
    system("touch test_empty.txt");
    mr_t mr;
    mr_attr_t at;
    mr_attr_init(&at);
    assert(mr_create(&mr, &at, map_emit_ones, reduce_sum_ones, NULL) == 0);
    assert(mr_start(mr, "test_empty.txt", "out_empty.mro") == 0);
    mr_destroy(mr);
    mr_attr_destroy(&at);

    // Verify empty output file
    FILE *f = fopen("out_empty.mro", "rb");
    assert(f != NULL);
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    assert(size == 0);
    fclose(f);

    system("rm -f test_empty.txt out_empty.mro");
    printf("Test: Empty Input File passed.\n\n");
}

static void test_very_long_line() {
    printf("Running test: Very Long Line (100KB)...\n");
    // Generate a file with a single 100KB line
    FILE *f = fopen("test_long.txt", "w");
    assert(f != NULL);
    for (int i = 0; i < 100000; i++) {
        fputc('a', f);
    }
    fputc('\n', f);
    fclose(f);

    mr_t mr;
    mr_attr_t at;
    mr_attr_init(&at);
    assert(mr_create(&mr, &at, map_line_len, reduce_max_len, NULL) == 0);
    assert(mr_start(mr, "test_long.txt", "out_long.mro") == 0);
    mr_destroy(mr);
    mr_attr_destroy(&at);

    // Verify length is 100,000
    test_result_t res[10];
    size_t count = 0;
    assert(read_mro("out_long.mro", res, 10, &count) == 0);
    assert(count == 1);
    assert(strcmp(res[0].token, "length") == 0);
    assert(res[0].val == 100000);

    system("rm -f test_long.txt out_long.mro");
    printf("Test: Very Long Line passed.\n\n");
}

static void test_stress_queue_size_1() {
    printf("Running test: Stress Test (1000 lines, queue_size=1, multiple threads)...\n");
    FILE *f = fopen("test_stress.txt", "w");
    assert(f != NULL);
    for (int i = 0; i < 1000; i++) {
        fprintf(f, "stress line %d\n", i);
    }
    fclose(f);

    mr_t mr;
    mr_attr_t at;
    mr_attr_init(&at);
    assert(mr_attr_set_queue_size(&at, 1) == 0);
    assert(mr_attr_set_mapper_threads(&at, 4) == 0);
    assert(mr_attr_set_reducer_threads(&at, 4) == 0);

    assert(mr_create(&mr, &at, map_emit_ones, reduce_sum_ones, NULL) == 0);
    assert(mr_start(mr, "test_stress.txt", "out_stress.mro") == 0);
    mr_destroy(mr);
    mr_attr_destroy(&at);

    // Verify output count is 1000
    test_result_t res[10];
    size_t count = 0;
    assert(read_mro("out_stress.mro", res, 10, &count) == 0);
    assert(count == 1);
    assert(strcmp(res[0].token, "stress") == 0);
    assert(res[0].val == 1000);

    system("rm -f test_stress.txt out_stress.mro");
    printf("Test: Stress Test passed.\n\n");
}

static void test_invalid_paths() {
    printf("Running test: Invalid Input/Output Paths...\n");
    mr_t mr;
    mr_attr_t at;
    mr_attr_init(&at);
    assert(mr_create(&mr, &at, map_emit_ones, reduce_sum_ones, NULL) == 0);

    // Non-existent input file
    assert(mr_start(mr, "non_existent_file.txt", "out_invalid.mro") == -1);

    // Invalid output directory
    assert(mr_start(mr, "test_suite.c", "/invalid_dir_xyz/out_invalid.mro") == -1);

    mr_destroy(mr);
    mr_attr_destroy(&at);
    printf("Test: Invalid Paths passed.\n\n");
}

int main() {
    printf("=== Esecuzione Test Suite Avanzata (Casi Limite) ===\n\n");
    test_empty_file();
    test_very_long_line();
    test_stress_queue_size_1();
    test_invalid_paths();
    printf("=== TUTTI I TEST DEI CASI LIMITE COMPLETATI CON SUCCESSO! ===\n");
    return 0;
}
