/************************************************************************************
    Copyright (C) 2015 MariaDB Corporation AB,

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.

   You should have received a copy of the GNU Library General Public
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc.,
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA
*************************************************************************************/

#include <ma_global.h>
#include <ma_sys.h>
#include <mysql.h>
#include <errmsg.h>
#include <mysql/client_plugin.h>
#include <string.h>
#include <ma_common.h>
#include <mariadb_ctype.h>
#include "ma_server_error.h"
#include <ma_pthread.h>
#include "redirection.h"

typedef struct Redirection_Entry
{
    char* key;
    char* host;
    char* user;
    unsigned int port;
    struct Redirection_Entry* next;
    struct Redirection_Entry* prev;
} Redirection_Entry;

typedef struct Redirection_Info
{
    char* host;
    char* user;
    unsigned int port;
} Redirection_Info;

#define MAX_CACHED_REDIRECTION_ENTRYS 4
#define MAX_REDIRECTION_KEY_LENGTH 512
static unsigned int redirection_cache_num;
static struct Redirection_Entry redirection_cache_root;
static struct Redirection_Entry* redirection_cache_rear;

static pthread_mutex_t redirect_lock;

#define redirect_mutex redirect_lock

#define mutex_lock(x) pthread_mutex_lock(&x);

#define mutex_unlock(x) pthread_mutex_unlock(&x)

#define mutex_destroy(x) pthread_mutex_destroy(&x)

int init_redirection_cache()
{
    pthread_mutex_init(&redirect_mutex, NULL);

    redirection_cache_root.next = NULL;
    redirection_cache_rear = &redirection_cache_root;
    redirection_cache_num = 0;

    return 0;
}

void add_redirection_entry(char* key, char* host, char* user, unsigned int port);

void delete_redirection_entry(char* key);

my_bool get_redirection_info(char* key, Redirection_Info** info, my_bool copy_on_found);

Redirection_Info* parse_redirection_info(MYSQL* mysql);

MYSQL* check_redirect(MYSQL* mysql, const char* host,
    const char* user, const char* passwd,
    const char* db, uint port,
    const char* unix_socket,
    ulong client_flag)
{
    if (mysql->options.enable_redirect != REDIRECTION_OFF)
    {
        char redirect_key[MAX_REDIRECTION_KEY_LENGTH] = { 0 };
        struct Redirection_Info* info = NULL;

        if (!port)
            port = MARIADB_PORT;

        sprintf(redirect_key, "%s_%s_%u", host, user, port);

        if (get_redirection_info(redirect_key, &info, 1))
        {
            MYSQL* ret_mysql = mysql->methods->api->mysql_real_connect(mysql,
                info->host,
                info->user,
                passwd,
                db,
                info->port,
                unix_socket,
                client_flag | CLIENT_REMEMBER_OPTIONS);

            free(info);

            if (ret_mysql)
            {
                return ret_mysql;
            }
            else
            {
                // redirection_entry is incorrect or has expired, delete entry before going back to normal workflow
                delete_redirection_entry(redirect_key);
            }
        }
    }

    return NULL;
}

MYSQL* redirect(MYSQL* mysql, int client_flag)
{

    // redirection info is present in mysql->info if redirection enabled on both ends
    // if info contains requested formatted string
    // dup current mysql context
    // fire another round of mysql_real_connect
    // successful case: dispose old mysql, use the new one
    // failure case: 1) use_redirect=YES, report error
    // 2) enable_redirect=PREFERRED, report warning and use original mysql
    if (mysql->options.enable_redirect != REDIRECTION_OFF)
    {
        Redirection_Info* info = NULL;

        if (!(info = parse_redirection_info(mysql)))
        {
            if (mysql->options.enable_redirect == REDIRECTION_ON)
            {
                mysql->methods->set_error(mysql, ER_BAD_HOST_ERROR, "HY000",
                    "redirection set to ON on client side but parse_redirection_info failed. Redirection info: %s",
                    mysql->info);
                return NULL;
            }
            else
            {
                // enable_redirect = PREFERRED, fallback to normal connection workflow
                return mysql;
            }
        }

        // No need to redirect if we are talking directly to the server
        if (!strcmp(info->host, mysql->host) &&
            !strcmp(info->user, mysql->user) &&
            info->port == mysql->port)
        {
            free(info);
            return mysql;
        }

        // Start of redirection workflow
        MYSQL redirect_mysql;
        mysql->methods->api->mysql_init(&redirect_mysql);
        redirect_mysql.options = mysql->options;

        void* old_context = mysql->options.extension->async_context;
        redirect_mysql.options.extension->async_context = NULL;

        if (!mysql->methods->api->mysql_real_connect(&redirect_mysql, info->host, info->user, mysql->passwd,
            mysql->db, info->port, mysql->unix_socket,
            mysql->client_flag | CLIENT_REMEMBER_OPTIONS))
        {
            // at this point redirect_mysql has executed end_server and clear_mysql
            free(info);
            memset(&redirect_mysql.options, 0, sizeof(struct st_mysql_options));

            if (mysql->options.enable_redirect == REDIRECTION_ON)
            {
                mysql->methods->set_error(mysql, redirect_mysql.net.last_errno,
                    redirect_mysql.net.sqlstate,
                    redirect_mysql.net.last_error);
                mysql->methods->api->mysql_close(&redirect_mysql);
                return NULL;
            }
            else
            {
                // enable_redirect == PREFERRED, fallback to normal connection workflow
                mysql->methods->api->mysql_close(&redirect_mysql);
                return mysql;
            }
        }

        // real_connect succeeded, copy redirect_mysql to *mysql after freeing mysql
        // save original host, user and port
        // save original free_me as well
        char redirect_key[MAX_REDIRECTION_KEY_LENGTH] = { 0 };
        sprintf(redirect_key, "%s_%s_%u", mysql->host, mysql->user, mysql->port);
        redirect_mysql.free_me = mysql->free_me;
        redirect_mysql.options.extension->async_context = old_context;

        // clean up mysql
        memset(&mysql->options, 0, sizeof(mysql->options));
        mysql->free_me = 0;
        mysql->methods->api->mysql_close(mysql); // mysql->methods->api->mysql_close contains end_server(in mysql_close_slow_part), so no need to end it again

        // point *mysql to redirect_mysql
        *mysql = redirect_mysql;
        mysql->net.pvio->mysql = mysql;

        // add redirection entry and free redirection info
        add_redirection_entry(redirect_key, mysql->host, mysql->user, mysql->port);
        free(info);
    }

    return mysql;
}

Redirection_Info* parse_redirection_info(MYSQL* mysql)
{
    Redirection_Info* info = NULL;
    if (!mysql->info || !mysql->info[0])
    {
        return NULL;
    }

    char* info_str = strtok(mysql->info, "\b");

    /*
  * redirection string somehow look like:
  * Location: mysql://redirectedHostName:redirectedPort/user=redirectedUser
  * the minimal len is 27 bytes
  */
    if (strlen(info_str) >= 27 && (strncmp(info_str, "Location:", strlen("Location:")) == 0))
    {
        char* p1 = strstr(info_str, "//");
        char* p2 = strstr(p1, ":");
        char* p3 = strstr(info_str, "user=");
        int host_len = p2 - p1 - 2;
        int port_len = p3 - p2 - 2;
        int user_len = strlen(info_str) - (p3 + 5 - info_str);

        if (host_len <= 0 || port_len <= 0 || user_len <= 0)
        {
            return NULL;
        }

        char* host_str = NULL;
        char* user_str = NULL;
        char* port_str = (char*)malloc(port_len + 1);

        // free(info) will free all pointers alloced by this ma_multi_malloc
        // do not include port_str here because port_str is a temp var, whereas port is the real member
        // of redirection info, so port_str should not get out of this function's scope
        if (!ma_multi_malloc(0,
            &info, sizeof(Redirection_Info),
            &host_str, (size_t)host_len + 1,
            &user_str, (size_t)user_len + 1,
            NULL))
            return NULL;

        ma_strmake(host_str, p1 + 2, host_len);
        ma_strmake(user_str, p3 + 5, user_len);
        ma_strmake(port_str, p2 + 1, port_len);
        info->host = host_str;
        info->user = user_str;

        if (!(info->port = strtoul(port_str, NULL, 0)))
        {
            free(info);
            free(port_str);
            return NULL;
        }

        free(port_str);
        return info;
    }

    return NULL;
}

my_bool get_redirection_info(char* key, Redirection_Info** info, my_bool copy_on_found)
{
    struct Redirection_Entry* ret_entry = NULL;
    struct Redirection_Entry* cur_entry = NULL;

    mutex_lock(redirect_mutex);
    cur_entry = redirection_cache_root.next;
    while (cur_entry)
    {
        if (cur_entry->key)
        {
            if (!strcmp(cur_entry->key, key))
            {
                ret_entry = cur_entry;
                break;
            }
            cur_entry = cur_entry->next;
        }
    }

    if (ret_entry)
    {
        // after find the cache, move it to list header
        if (redirection_cache_root.next != ret_entry)
        {
            if (redirection_cache_rear == ret_entry)
            {
                redirection_cache_rear = ret_entry->prev;
            }

            ret_entry->prev->next = ret_entry->next;

            if (ret_entry->next)
            {
                ret_entry->next->prev = ret_entry->prev;
            }

            ret_entry->next = redirection_cache_root.next;
            ret_entry->prev = &redirection_cache_root;

            redirection_cache_root.next = ret_entry;
        }

        if (copy_on_found)
        {
            size_t host_len = strlen(ret_entry->host);
            size_t user_len = strlen(ret_entry->user);

            char* host_str;
            char* user_str;

            if (!ma_multi_malloc(0,
                info, sizeof(Redirection_Info),
                &host_str, host_len + 1,
                &user_str, user_len + 1,
                NULL))
            {
                mutex_unlock(redirect_mutex);
                return 0;
            }

            ma_strmake(host_str, ret_entry->host, strlen(ret_entry->host));
            ma_strmake(user_str, ret_entry->user, strlen(ret_entry->user));
            (*info)->host = host_str;
            (*info)->user = user_str;
            (*info)->port = ret_entry->port;
        }
    }

    my_bool retval = (ret_entry != NULL);
    mutex_unlock(redirect_mutex);

    return retval;
}

void add_redirection_entry(char* key, char* host, char* user, unsigned int port)
{
    struct Redirection_Entry* new_entry;
    char* _key;
    char* _host;
    char* _user;
    size_t key_len = 0;
    size_t host_len = 0;
    size_t user_len = 0;

    if (key == NULL || host == NULL || user == NULL)
    {
        return;
    }

    if (get_redirection_info(key, NULL, 0))
    {
        return;
    }

    key_len = strlen(key);
    host_len = strlen(host);
    user_len = strlen(user);

    if (!ma_multi_malloc(0,
        &new_entry, sizeof(struct Redirection_Entry),
        &_key, key_len + 1,
        &_host, host_len + 1,
        &_user, user_len + 1,
        NULL))
    {
        return;
    }

    mutex_lock(redirect_mutex);
    new_entry->key = _key;
    new_entry->host = _host;
    new_entry->user = _user;
    new_entry->next = NULL;
    new_entry->prev = NULL;

    strcpy(new_entry->key, key);
    strcpy(new_entry->host, host);
    strcpy(new_entry->user, user);
    new_entry->port = port;

    if (NULL == redirection_cache_root.next)
    {
        redirection_cache_root.next = new_entry;
        new_entry->prev = &redirection_cache_root;
        redirection_cache_rear = new_entry;
    }
    else
    {
        new_entry->next = redirection_cache_root.next;
        new_entry->prev = &redirection_cache_root;

        redirection_cache_root.next->prev = new_entry;
        redirection_cache_root.next = new_entry;
    }

    redirection_cache_num++;

    if (redirection_cache_num > MAX_CACHED_REDIRECTION_ENTRYS)
    {
        struct Redirection_Entry* popingEntry = redirection_cache_rear;
        redirection_cache_rear = redirection_cache_rear->prev;
        redirection_cache_rear->next = NULL;

        free(popingEntry);
        redirection_cache_num--;
    }

    mutex_unlock(redirect_mutex);

    return;
}

void delete_redirection_entry(char* key)
{
    struct Redirection_Entry* del_entry = NULL;
    struct Redirection_Entry* cur_entry = NULL;

    mutex_lock(redirect_mutex);
    cur_entry = redirection_cache_root.next;
    while (cur_entry)
    {
        if (cur_entry->key)
        {
            if (!strcmp(cur_entry->key, key))
            {
                del_entry = cur_entry;
                break;
            }
            cur_entry = cur_entry->next;
        }
    }

    if (del_entry)
    {
        if (redirection_cache_rear == del_entry)
        {
            redirection_cache_rear = del_entry->prev;
        }
        del_entry->prev->next = del_entry->next;

        if (del_entry->next)
        {
            del_entry->next->prev = del_entry->prev;
        }

        redirection_cache_num--;
        free(del_entry);
    }

    mutex_unlock(redirect_mutex);
}

void* ma_multi_malloc(myf myFlags, ...)
{
    va_list args;
    char** ptr, * start, * res;
    size_t tot_length, length;

    va_start(args, myFlags);
    tot_length = 0;
    while ((ptr = va_arg(args, char**)))
    {
        length = va_arg(args, size_t);
        tot_length += ALIGN_SIZE(length);
    }
    va_end(args);

    if (!(start = (char*)malloc(tot_length)))
        return 0;

    va_start(args, myFlags);
    res = start;
    while ((ptr = va_arg(args, char**)))
    {
        *ptr = res;
        length = va_arg(args, size_t);
        res += ALIGN_SIZE(length);
    }
    va_end(args);
    return start;
}

char* ma_strmake(register char* dst, register const char* src, size_t length)
{
    while (length--)
        if (!(*dst++ = *src++))
            return dst - 1;
    *dst = 0;
    return dst;
}
