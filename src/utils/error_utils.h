#ifndef ERROR_UTILS_H
#define ERROR_UTILS_H

#include <errno.h>
#include <stdlib.h>
#include "log_internal.h"

// Macro per chiamate che restituiscono -1 nei figli (termina il processo figlio)
#define SYSCALL_EXIT(S, m) \
    do { \
        if ((S) == -1) { \
            mr_log_error_internal(m, __FILE__, __LINE__); \
            _exit(EXIT_FAILURE); \
        } \
    } while(0)

// Macro per funzioni del framework che devono tornare -1 all'utente senza uscire
#define CHECK_ERROR(S, m) \
    do { \
        if ((S) == -1) { \
            mr_log_error_internal(m, __FILE__, __LINE__); \
            return -1; \
        } \
    } while(0)

// Macro per malloc nei figli (termina il processo figlio)
#define SYSNCALL_EXIT(S, m) \
    do { \
        if ((S) == NULL) { \
            mr_log_error_internal(m, __FILE__, __LINE__); \
            _exit(EXIT_FAILURE); \
        } \
    } while(0)

/**
 * PATCH SICUREZZA: Macro per il processo principale.
 * In caso di fallimento della malloc, imposta errno e ritorna -1
 * permettendo all'applicazione chiamante di gestire l'errore.
 */
#define SYSNCALL_RETURN(S, m) \
    do { \
        if ((S) == NULL) { \
            mr_log_error_internal(m, __FILE__, __LINE__); \
            errno = ENOMEM; \
            return -1; \
        } \
    } while(0)

#endif
