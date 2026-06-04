#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * mr_viewer.c
 * 
 * Semplice utility per leggere e visualizzare il contenuto del file di output
 * binario generato dal framework MapReduce.
 * 
 * Formato del file (come da specifica):
 * [int token_len] [token bytes] [int result_len] [result bytes]
 */

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "Uso: %s <file_output.mro>\n", argv[0]);
        return 1;
    }

    FILE *f = fopen(argv[1], "rb");
    if (!f) {
        perror("Errore apertura file");
        return 1;
    }

    printf("%-20s | %-10s\n", "TOKEN", "RISULTATO");
    printf("------------------------------------------\n");

    int token_len;
    int result_len;
    size_t count = 0;

    while (fread(&token_len, sizeof(int), 1, f) == 1) {
        // Leggi Token
        char *token = malloc((size_t)token_len + 1);
        if (!token) break;
        if (token_len > 0) {
            if (fread(token, 1, (size_t)token_len, f) != (size_t)token_len) {
                free(token);
                break;
            }
        }
        token[token_len] = '\0';

        // Leggi Result Length
        if (fread(&result_len, sizeof(int), 1, f) != 1) {
            free(token);
            break;
        }

        // Leggi Result
        char *result = malloc((size_t)result_len + 1);
        if (!result) {
            free(token);
            break;
        }
        if (result_len > 0) {
            if (fread(result, 1, (size_t)result_len, f) != (size_t)result_len) {
                free(token);
                free(result);
                break;
            }
        }
        result[result_len] = '\0';

        // Visualizzazione
        // Nell'esempio wordcount.c, il reducer emette il risultato come stringa numerica
        printf("%-20s | %s\n", token, result);

        free(token);
        free(result);
        count++;
    }

    printf("------------------------------------------\n");
    printf("Totale record: %zu\n", count);

    fclose(f);
    return 0;
}
