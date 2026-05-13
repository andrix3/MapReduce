#include "utils.h"
#include "error_utils.h"
#include <sys/stat.h>

int check_path(char * path)
{
    struct stat path_stat;
    
    if(stat(path, &path_stat) != 0)
    {
        mr_log_internal("ERROR", "path non esistente", __FILE__, __LINE__);
        return -1;
    }

    if(S_ISREG(path_stat.st_mode)) return 0;
     
    if(S_ISDIR(path_stat.st_mode)) return 1;

    // tipo di file non supportato
    return -1;
}