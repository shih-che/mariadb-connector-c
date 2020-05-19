#ifndef MA_REDIRECT_H
#define MA_REDIRECT_H

#include <mysql.h>

MYSQL* redirect(MYSQL* mysql, int client_flag);
MYSQL* check_redirect(MYSQL* mysql, const char* host,
    const char* user, const char* passwd,
    const char* db, unsigned int port,
    const char* unix_socket,
    unsigned long client_flag);
int init_redirection_cache();

#endif 
