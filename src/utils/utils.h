#ifndef UTILS_H
#define UTILS_H

typedef enum 
{
    PATH_INVALID = -1,
    PATH_FILE = 0,
    PATH_DIRECTORY = 1
} path_type_t;

path_type_t check_path(const char *path);

#endif