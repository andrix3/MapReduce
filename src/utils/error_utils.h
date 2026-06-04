
#ifndef ERROR_UTILS_H
#define ERROR_UTILS_H

#include "log_internal.h"

// Macro per chiamate che restituiscono -1 (es. fork, pipe, write)
#define SYSCALL_EXIT(S, m) \
    do { \
        if ((S) == -1) { \
            mr_log_error_internal(m, __FILE__, __LINE__); \
            exit(EXIT_FAILURE); \
        } \
    } while(0)

// Macro per funzioni del framework che devono tornare -1 all'utente
#define CHECK_ERROR(S, m) \
    do { \
        if ((S) == -1) { \
            mr_log_error_internal(m, __FILE__, __LINE__); \
            return -1; \
        } \
    } while(0)

// Macro per chiamate che restituiscono NULL (es. malloc)
#define SYSNCALL_EXIT(S, m) \
    do { \
        if ((S) == NULL) { \
            mr_log_error_internal(m, __FILE__, __LINE__); \
            exit(EXIT_FAILURE); \
        } \
    } while(0)

#endif