#ifndef UTILS_H
#define UTILS_H
#include <unistd.h>
typedef enum { PATH_INVALID = -1, PATH_FILE = 0, PATH_DIRECTORY = 1 } path_type_t;
path_type_t check_path(const char *p);
int check_output_path(const char *p);
ssize_t readn(int fd, void *buf, size_t n);
ssize_t writen(int fd, const void *buf, size_t n);
#endif
