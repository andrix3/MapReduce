#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <libgen.h>
#include <errno.h>

#include "utils.h"
#include "error_utils.h"

ssize_t writen(int fd, const void *buf, size_t n) {
    size_t nleft = n;
    ssize_t nwritten;
    const char *ptr = buf;

    while (nleft > 0) {
        if ((nwritten = write(fd, ptr, nleft)) <= 0) {
            if (nwritten < 0 && errno == EINTR)
                nwritten = 0;   /* and call write() again */
            else
                return -1;      /* error */
        }
        nleft -= nwritten;
        ptr += nwritten;
    }
    return n;
}

ssize_t readn(int fd, void *buf, size_t n) {
    size_t nleft = n;
    ssize_t nread;
    char *ptr = buf;

    while (nleft > 0) {
        if ((nread = read(fd, ptr, nleft)) < 0) {
            if (errno == EINTR)
                nread = 0;      /* and call read() again */
            else
                return -1;
        } else if (nread == 0)
            break;              /* EOF */

        nleft -= nread;
        ptr += nread;
    }
    return n - nleft;         /* return >= 0 */
}
#include "log_internal.h"

/** 
 * controllo validità path
 * return: 
 * PATH_INVALID: -1
 * PATH_FILE: 0
 * PATH_DIRECTORY: 1
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
    char *path_copy;
    SYSNCALL_EXIT(path_copy = strdup(path), "strdup path_copy fallita");

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
