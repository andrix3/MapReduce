#ifndef LOG_INTERNAL_H
#define LOG_INTERNAL_H

#include "mr.h" // Per la definizione di mr_t e mr_attr_t

/**
 * Prototipo della funzione di log implementata in log_internal.c
 */
void mr_log_internal(const char *path, const char *event, const char *msg, const char *file, int line);
void mr_log_error_internal(const char *msg, const char *file, int line);

/**
 * Macro standard: da usare quando hai mr a disposizione.
 * Esempio: MR_LOG(mr, "INFO", "Pipe creata");
 */
#define MR_LOG(mr_ptr, event, msg) \
    mr_log_internal((mr_ptr)->attr.log_file, event, msg, __FILE__, __LINE__)

/**
 * Macro raw: da usare quando non hai ancora mr o se mr è NULL.
 * Esempio: MR_LOG_RAW(NULL, "ERROR", "Malloc fallita");
 */
#define MR_LOG_RAW(path, event, msg) \
    mr_log_internal(path, event, msg, __FILE__, __LINE__)

#endif