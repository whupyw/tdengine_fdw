#ifndef CONNECTION_HPP
#define CONNECTION_HPP

extern "C" {
#include "tdengine_fdw.h"
#include <taosws.h>
}

extern WS_TAOS* tdengine_get_connection(UserMapping *user, tdengine_opt *options);

extern WS_TAOS* create_tdengine_connection(char* dsn);

extern void tdengine_cleanup_connection(void);

#endif /* CONNECTION_HPP */