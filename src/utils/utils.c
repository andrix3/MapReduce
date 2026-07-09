#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <errno.h>
#include "utils.h"

ssize_t writen(int fd, const void *buf, size_t n) {
    size_t nl = n; ssize_t nw; const char *p = buf;
    while (nl > 0) {
        if ((nw = write(fd, p, nl)) <= 0) { if (nw < 0 && errno == EINTR) nw = 0; else return -1; }
        nl -= nw; p += nw;
    }
    return n;
}

ssize_t readn(int fd, void *buf, size_t n) {
    size_t nl = n; ssize_t nr; char *p = buf;
    while (nl > 0) {
        if ((nr = read(fd, p, nl)) < 0) { if (errno == EINTR) nr = 0; else return -1; }
        else if (nr == 0) break;
        nl -= nr; p += nr;
    }
    return n - nl;
}

path_type_t check_path(const char *p) {
    struct stat s; if (stat(p, &s) != 0 || access(p, R_OK) != 0) return PATH_INVALID;
    return S_ISREG(s.st_mode) ? PATH_FILE : (S_ISDIR(s.st_mode) ? PATH_DIRECTORY : PATH_INVALID);
}

int check_output_path(const char *p) {
    char *c = strdup(p); if (!c) return -1;
    char *d = dirname(c);
    int r = (access(d, W_OK) == 0) ? 0 : -1;
    free(c); return r;
}
