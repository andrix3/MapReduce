#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    if (argc != 2) return 1;
    FILE *f = fopen(argv[1], "rb"); if (!f) return 1;
    int tl, rl;
    while (fread(&tl, sizeof(int), 1, f) == 1) {
        if (tl < 0) break;
        char *t = malloc((size_t)(tl + 1));
        if (!t) break;
        if (fread(t, 1, (size_t)tl, f) != (size_t)tl) { free(t); break; }
        t[tl] = '\0';
        if (fread(&rl, sizeof(int), 1, f) != 1) { free(t); break; }
        if (rl < 0) { free(t); break; }
        char *r = malloc((size_t)(rl + 1));
        if (!r) { free(t); break; }
        if (fread(r, 1, (size_t)rl, f) != (size_t)rl) { free(t); free(r); break; }
        r[rl] = '\0';
        printf("%-20s | %s\n", t, r);
        free(t); free(r);
    }
    fclose(f); return 0;
}
