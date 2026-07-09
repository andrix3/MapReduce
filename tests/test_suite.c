#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "mr.h"

int test_map(const mr_file_line_t *l, mr_emit_pair_t e, void *a, void *u) {
    (void)l; (void)u;
    int v = 1; e("test", &v, sizeof(int), a); return 0;
}
int test_red(const char *t, const mr_value_t *vs, size_t vc, mr_emit_result_t e, void *a, void *u) {
    (void)vs; (void)u;
    int sum = (int)vc; e(t, &sum, sizeof(int), a); return 0;
}

int main() {
    printf("Esecuzione Test Suite...\n");
    system("echo 'line1\nline2' > test.txt");
    mr_t mr; mr_attr_t at; mr_attr_init(&at);
    assert(mr_create(&mr, &at, test_map, test_red, NULL) == 0);
    assert(mr_start(mr, "test.txt", "out.mro") == 0);
    mr_destroy(mr);
    system("rm -f test.txt out.mro");
    printf("TEST COMPLETATI CON SUCCESSO!\n");
    return 0;
}
