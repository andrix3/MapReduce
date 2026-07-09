#ifndef LOG_INTERNAL_H
#define LOG_INTERNAL_H
void mr_log_internal(const char *p, const char *e, const char *m, const char *f, int l);
void mr_log_error_internal(const char *m, const char *f, int l);
#define MR_LOG(mr, e, m) mr_log_internal((mr)->attr.log_file, e, m, __FILE__, __LINE__)
#endif
