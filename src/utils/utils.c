#include <sys/stat.h>
#include <unistd.h>
#include "utils.h"
#include "log_internal.h"

/** 
 * controllo validità path
 * return -1 PATH_INVALID
 * retunr 0 PATH_FILE
 * return 1 PATH_DIRECTORY 
*/
path_type_t check_path(const char * path) {
    struct stat s;

    // 1. Esistenza e accessibilità
    if (stat(path, &s) != 0) {
        return PATH_INVALID;
    }

    // 2. Controllo permessi di lettura (per l'input)
    if (access(path, R_OK) != 0) {
        return PATH_INVALID;
    }

    // 3. Distinzione tipo
    if (S_ISREG(s.st_mode)) {
        return PATH_FILE;
    } else if (S_ISDIR(s.st_mode)) {
        return PATH_DIRECTORY;
    }

    return PATH_INVALID;
}

int check_output_path(const char *path) {
    if (!path) return -1;

    // Creiamo una copia perché dirname() può modificare la stringa originale
    char *path_copy = strdup(path);
    if (!path_copy) return -1;

    // Estraiamo il percorso della directory
    char *dir = dirname(path_copy);

    // Verifichiamo se abbiamo il permesso di scrittura (W_OK) nella directory
    // Se la directory non esiste o non è scrivibile, access() restituirà -1
    if (access(dir, W_OK) != 0) {
        free(path_copy);
        return -1;
    }

    free(path_copy);
    return 0;
}