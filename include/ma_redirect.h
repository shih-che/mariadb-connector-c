#ifndef MA_REDIRECT_H
#define MA_REDIRECT_H

#include <mysql.h>

MYSQL* redirect(MYSQL* mysql, int client_flag);
MYSQL* check_redirect(MYSQL* mysql, const char* host,
    const char* user, const char* passwd,
    const char* db, uint port,
    const char* unix_socket,
    ulong client_flag);
void init_redirection_cache();

#endif 
