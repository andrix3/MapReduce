#include "log_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <threads.h>

static mtx_t log_mtx;
static once_flag log_mtx_once = ONCE_FLAG_INIT;

static void init_log_mtx(void) {
    mtx_init(&log_mtx, mtx_plain);
}

void mr_log_internal(const char *path, const char *event, const char *msg, const char *file, int line)
{
    (void)file;
    (void)line;

    if (!path) path = "mr.log";

    call_once(&log_mtx_once, init_log_mtx);

    // Get time
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    struct tm tm_info;
    localtime_r(&ts.tv_sec, &tm_info);
    char time_buf[64];
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", &tm_info);

    // Get IDs
    pid_t pid = getpid();
    pid_t tid = (pid_t)syscall(SYS_gettid);

    // Format: [timestamp] [processo] [thread] [evento] messaggio
    // Add \n at the end
    char log_buf[1024];
    int len = snprintf(log_buf, sizeof(log_buf), "[%s.%03ld] [%d] [%d] [%s] %s\n",
                       time_buf, ts.tv_nsec / 1000000, pid, tid, event, msg);

    if (len < 0) return;

    // Concurrency control: thread safe within process, process safe via fcntl
    mtx_lock(&log_mtx);

    int fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0666);
    if (fd >= 0) {
        struct flock fl;
        memset(&fl, 0, sizeof(fl));
        fl.l_type = F_WRLCK;
        fl.l_whence = SEEK_SET;
        
        // Wait for lock
        fcntl(fd, F_SETLKW, &fl);
        
        // Write the log
        const char *p = log_buf;
        size_t to_write = (size_t)len;
        while (to_write > 0) {
            ssize_t written = write(fd, p, to_write);
            if (written < 0) break;
            p += written;
            to_write -= written;
        }

        // Unlock
        fl.l_type = F_UNLCK;
        fcntl(fd, F_SETLKW, &fl);
        
        close(fd);
    }

    mtx_unlock(&log_mtx);
}

void mr_log_error_internal(const char *msg, const char *file, int line)
{
    mr_log_internal(NULL, "ERROR", msg, file, line);
}
