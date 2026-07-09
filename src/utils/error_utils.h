#ifndef ERROR_UTILS_H
#define ERROR_UTILS_H
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include "log_internal.h"

#define SYSCALL_EXIT(S, m) do { if ((S) == -1) { mr_log_error_internal(m, __FILE__, __LINE__); _exit(1); } } while(0)
#define CHECK_ERROR(S, m) do { if ((S) == -1) { mr_log_error_internal(m, __FILE__, __LINE__); return -1; } } while(0)
#define SYSNCALL_EXIT(S, m) do { if ((S) == NULL) { mr_log_error_internal(m, __FILE__, __LINE__); _exit(1); } } while(0)
#define SYSNCALL_RETURN(S, m) do { if ((S) == NULL) { mr_log_error_internal(m, __FILE__, __LINE__); errno = ENOMEM; return -1; } } while(0)
#endif
