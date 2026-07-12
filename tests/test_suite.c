#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mr.h"

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

static int map_line_content(const mr_file_line_t *l, mr_emit_pair_t e, void *a, void *u) {
    (void)u;
    int v = 1;
    e(l->line, &v, sizeof(int), a);
    return 0;
}

static int reduce_sum(const char *t, const mr_value_t *vs, size_t vc, mr_emit_result_t e, void *a, void *u) {
    (void)u;
    int sum = 0;
    for (size_t i = 0; i < vc; i++) {
        sum += *(int*)vs[i].data;
    }
    e(t, &sum, sizeof(int), a);
    return 0;
}

static int map_emit_ones(const mr_file_line_t *l, mr_emit_pair_t e, void *a, void *u) {
    (void)l; (void)u;
    int v = 1;
    e("stress", &v, sizeof(int), a);
    return 0;
}

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


// === Caso Limite 1: Gestione di File Vuoti (0 byte) ===
// Verifica che il framework completi con successo l'esecuzione senza
// bloccarsi o andare in crash quando riceve in input un file vuoto.
static void test_empty_file() {
    printf("Esecuzione test: File di input vuoto (0 byte)...\n");
    system("touch test_empty.txt");
    mr_t mr;
    mr_attr_t at;
    mr_attr_init(&at);
    assert(mr_create(&mr, &at, map_emit_ones, reduce_sum, NULL) == 0);
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
    printf("Test: File di input vuoto superato.\n\n");
}

// === Caso Limite 2: File contenenti una sola riga (con newline) ===
// Verifica la corretta elaborazione, deserializzazione e scrittura
// dei record quando l'input è composto da una sola linea di testo.
static void test_single_line_file() {
    printf("Esecuzione test: File a riga singola (con newline)...\n");
    FILE *f = fopen("test_single.txt", "w");
    assert(f != NULL);
    fprintf(f, "single_line\n");
    fclose(f);

    mr_t mr;
    mr_attr_t at;
    mr_attr_init(&at);
    assert(mr_create(&mr, &at, map_line_content, reduce_sum, NULL) == 0);
    assert(mr_start(mr, "test_single.txt", "out_single.mro") == 0);
    mr_destroy(mr);
    mr_attr_destroy(&at);

    test_result_t res[10];
    size_t count = 0;
    assert(read_mro("out_single.mro", res, 10, &count) == 0);
    assert(count == 1);
    assert(strcmp(res[0].token, "single_line") == 0);
    assert(res[0].val == 1);

    system("rm -f test_single.txt out_single.mro");
    printf("Test: File a riga singola superato.\n\n");
}

// === Caso Limite 3: Righe vuote e Ultima riga non terminata da \\n ===
// Verifica che le righe vuote intermedie siano lette ed elaborate correttamente,
// e che l'ultima riga del file, anche se priva di newline finale prima di EOF,
// venga completamente acquisita e processata dal framework.
static void test_empty_lines_and_no_newline_eof() {
    printf("Esecuzione test: Righe vuote e ultima riga senza newline a EOF...\n");
    FILE *f = fopen("test_specials.txt", "w");
    assert(f != NULL);
    // Scrive:
    // line1\n
    // \n (riga vuota)
    // line3\n
    // line4 (ultima riga non terminata da \n)
    fprintf(f, "line1\n\nline3\nline4");
    fclose(f);

    mr_t mr;
    mr_attr_t at;
    mr_attr_init(&at);
    assert(mr_create(&mr, &at, map_line_content, reduce_sum, NULL) == 0);
    assert(mr_start(mr, "test_specials.txt", "out_specials.mro") == 0);
    mr_destroy(mr);
    mr_attr_destroy(&at);

    // Risultati attesi ordinati lessicograficamente:
    // 1. "" (riga vuota) -> 1
    // 2. "line1" -> 1
    // 3. "line3" -> 1
    // 4. "line4" -> 1
    test_result_t res[10];
    size_t count = 0;
    assert(read_mro("out_specials.mro", res, 10, &count) == 0);
    assert(count == 4);
    assert(strcmp(res[0].token, "") == 0);
    assert(res[0].val == 1);
    assert(strcmp(res[1].token, "line1") == 0);
    assert(res[1].val == 1);
    assert(strcmp(res[2].token, "line3") == 0);
    assert(res[2].val == 1);
    assert(strcmp(res[3].token, "line4") == 0);
    assert(res[3].val == 1);

    system("rm -f test_specials.txt out_specials.mro");
    printf("Test: Righe vuote ed EOF senza newline superato.\n\n");
}

// --- Altri Casi Limite ---

// === Caso Limite 4: Righe Logiche Estremamente Lunghe (100KB) ===
// Verifica che i buffer dinamici e il protocollo di serializzazione
// gestiscano linee di grandi dimensioni senza overflow o corruzione di memoria.
static void test_very_long_line() {
    printf("Esecuzione test: Riga molto lunga (100KB)...\n");
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

    test_result_t res[10];
    size_t count = 0;
    assert(read_mro("out_long.mro", res, 10, &count) == 0);
    assert(count == 1);
    assert(strcmp(res[0].token, "length") == 0);
    assert(res[0].val == 100000);

    system("rm -f test_long.txt out_long.mro");
    printf("Test: Riga molto lunga superato.\n\n");
}

// === Caso Limite 5: Saturazione della Coda (queue_size = 1) ===
// Stress test che verifica la sincronizzazione multithreading. Utilizzando una coda
// limitata ad 1 solo elemento, costringe i thread Mapper a bloccarsi e sbloccarsi
// costantemente, validando il comportamento corretto di mutex e condition variables.
static void test_stress_queue_size_1() {
    printf("Esecuzione test: Stress Test (1000 righe, queue_size=1, thread multipli)...\n");
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

    assert(mr_create(&mr, &at, map_emit_ones, reduce_sum, NULL) == 0);
    assert(mr_start(mr, "test_stress.txt", "out_stress.mro") == 0);
    mr_destroy(mr);
    mr_attr_destroy(&at);

    test_result_t res[10];
    size_t count = 0;
    assert(read_mro("out_stress.mro", res, 10, &count) == 0);
    assert(count == 1);
    assert(strcmp(res[0].token, "stress") == 0);
    assert(res[0].val == 1000);

    system("rm -f test_stress.txt out_stress.mro");
    printf("Test: Stress Test superato.\n\n");
}

// === Caso Limite 6: Percorsi Input/Output Non Validi ===
// Verifica che mr_start ritorni ordinatamente -1 (con errno appropriato)
// in presenza di file mancanti o cartelle di destinazione senza permessi di scrittura,
// assicurando che non ci siano leak di risorse o deadlock.
static void test_invalid_paths() {
    printf("Esecuzione test: Percorsi di input/output non validi...\n");
    mr_t mr;
    mr_attr_t at;
    mr_attr_init(&at);
    assert(mr_create(&mr, &at, map_emit_ones, reduce_sum, NULL) == 0);

    assert(mr_start(mr, "non_existent_file.txt", "out_invalid.mro") == -1);
    assert(mr_start(mr, "test_suite.c", "/invalid_dir_xyz/out_invalid.mro") == -1);

    mr_destroy(mr);
    mr_attr_destroy(&at);
    printf("Test: Percorsi non validi superato.\n\n");
}

// === Caso Limite 7: Righe che superano MAX_LINE_LEN ===
// Verifica che una riga che supera MAX_LINE_LEN venga ignorata
// e non causi il desync o crash del mapper, mentre le righe successive
// valide siano correttamente elaborate.
static void test_exceed_max_line_len() {
    printf("Esecuzione test: Riga che supera MAX_LINE_LEN (12MB)...\n");
    FILE *f = fopen("test_huge.txt", "w");
    assert(f != NULL);
    
    // Scrivi 12 MB di 'a'
    char *buf = malloc(1024 * 1024);
    assert(buf != NULL);
    memset(buf, 'a', 1024 * 1024);
    for (int i = 0; i < 12; i++) {
        assert(fwrite(buf, 1, 1024 * 1024, f) == 1024 * 1024);
    }
    free(buf);
    fputc('\n', f); // Fine della riga da 12MB
    
    // Seconda riga valida
    fprintf(f, "riga_valida\n");
    fclose(f);

    mr_t mr;
    mr_attr_t at;
    mr_attr_init(&at);
    assert(mr_create(&mr, &at, map_line_content, reduce_sum, NULL) == 0);
    assert(mr_start(mr, "test_huge.txt", "out_huge.mro") == 0);
    mr_destroy(mr);
    mr_attr_destroy(&at);

    test_result_t res[10];
    size_t count = 0;
    assert(read_mro("out_huge.mro", res, 10, &count) == 0);
    // Deve contenere solo "riga_valida"
    assert(count == 1);
    assert(strcmp(res[0].token, "riga_valida") == 0);
    assert(res[0].val == 1);

    system("rm -f test_huge.txt out_huge.mro");
    printf("Test: Riga che supera MAX_LINE_LEN superato.\n\n");
}

int main() {
    printf("=== Esecuzione Test Suite Avanzata (Casi Limite) ===\n\n");
    test_empty_file();
    test_single_line_file();
    test_empty_lines_and_no_newline_eof();
    test_very_long_line();
    test_stress_queue_size_1();
    test_invalid_paths();
    test_exceed_max_line_len();
    printf("=== TUTTI I TEST DEI CASI LIMITE COMPLETATI CON SUCCESSO! ===\n");
    return 0;
}
