#include "log_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <threads.h>
#include <sys/syscall.h>

static mtx_t l_mtx;
static once_flag l_once = ONCE_FLAG_INIT;
static void l_init() { mtx_init(&l_mtx, mtx_plain); }

void mr_log_internal(const char *p, const char *e, const char *m, const char *f, int l) {
    (void)f; (void)l; if (!p) p = "mr.log";
    call_once(&l_once, l_init);
    struct timespec ts; timespec_get(&ts, TIME_UTC);
    char t_buf[64]; strftime(t_buf, 64, "%Y-%m-%d %H:%M:%S", localtime(&ts.tv_sec));
    char buf[1024]; int len = snprintf(buf, 1024, "[%s.%03ld] [%d] [%s] %s\n", t_buf, ts.tv_nsec/1000000, getpid(), e, m);
    if (len < 0) return;
    size_t write_len = (size_t)len;
    if (write_len >= 1024) {
        write_len = 1023;
    }
    mtx_lock(&l_mtx);
    int fd = open(p, O_CREAT|O_WRONLY|O_APPEND, 0666);
    if (fd>=0) {
        struct flock fl = { .l_type = F_WRLCK, .l_whence = SEEK_SET };
        fcntl(fd, F_SETLKW, &fl);
        ssize_t nw = write(fd, buf, write_len);
        (void)nw;
        fl.l_type = F_UNLCK; fcntl(fd, F_SETLKW, &fl);
        close(fd);
    }
    mtx_unlock(&l_mtx);
}
void mr_log_error_internal(const char *m, const char *f, int l) { mr_log_internal(NULL, "ERROR", m, f, l); }
