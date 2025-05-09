extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
}

#include "connection.hpp"

typedef Oid ConnCacheKey;

/*
 * 连接缓存条目结构体，用于管理TDengine连接缓存
 */
typedef struct ConnCacheEntry
{
    ConnCacheKey key;           /* 哈希键值(必须是第一个成员) */
    WS_TAOS *conn;             /* TDengine服务器连接指针，NULL表示无有效连接 */
    bool invalidated;          /* 连接失效标志，true表示需要重新连接 */
    uint32 server_hashvalue;   /* 外部服务器OID的哈希值，用于缓存失效检测 */
    uint32 mapping_hashvalue;  /* 用户映射OID的哈希值，用于缓存失效检测 */
} ConnCacheEntry;

static HTAB *ConnectionHash = NULL;

static void tdengine_make_new_connection(ConnCacheEntry *entry, UserMapping *user, tdengine_opt *options);
static WS_TAOS* tdengine_connect_server(tdengine_opt *options);
static void tdengine_disconnect_server(ConnCacheEntry *entry);
static void tdengine_inval_callback(Datum arg, int cacheid, uint32 hashvalue);

/*
 * 获取或创建与TDengine服务器的连接
 */
WS_TAOS*
tdengine_get_connection(UserMapping *user, tdengine_opt *options)
{
    bool found;
    ConnCacheEntry *entry;
    ConnCacheKey key;

    /* 首次调用时初始化连接缓存哈希表 */
    if (ConnectionHash == NULL)
    {
        HASHCTL ctl;

        ctl.keysize = sizeof(ConnCacheKey);
        ctl.entrysize = sizeof(ConnCacheEntry);
        ConnectionHash = hash_create("tdengine_fdw connections", 8,
                                   &ctl,
                                   HASH_ELEM | HASH_BLOBS);

        /* 注册回调函数用于连接清理 */
        CacheRegisterSyscacheCallback(FOREIGNSERVEROID,tdengine_inval_callback, (Datum) 0);
        CacheRegisterSyscacheCallback(USERMAPPINGOID,tdengine_inval_callback, (Datum) 0);
    }

    /* 使用用户映射ID作为哈希键 */
    key = user->umid;

    entry = (ConnCacheEntry *)hash_search(ConnectionHash, &key, HASH_ENTER, &found);
    if (!found)
    {
        entry->conn = NULL;
    }

    if (entry->conn != NULL && entry->invalidated)
    {
        elog(DEBUG3, "tdengine_fdw: closing connection %p for option changes to take effect",
             entry->conn);
        tdengine_disconnect_server(entry);
    }

    if (entry->conn == NULL)
        tdengine_make_new_connection(entry, user, options);

    return entry->conn;
}

/*
 * 创建新的TDengine服务器连接并初始化连接缓存项
 */
static void
tdengine_make_new_connection(ConnCacheEntry *entry, UserMapping *user, tdengine_opt *opts)
{
    /* 获取外部服务器信息 */
    ForeignServer *server = GetForeignServer(user->serverid);

    Assert(entry->conn == NULL);

    entry->invalidated = false;
    entry->server_hashvalue = GetSysCacheHashValue1(FOREIGNSERVEROID,ObjectIdGetDatum(server->serverid));
    entry->mapping_hashvalue = GetSysCacheHashValue1(USERMAPPINGOID,ObjectIdGetDatum(user->umid));

    entry->conn = tdengine_connect_server(opts);

    elog(DEBUG3, "tdengine_fdw: new TDengine connection %p for server \"%s\" (user mapping oid %u, userid %u)",entry->conn, server->servername, user->umid, user->userid);
}

/*
 * 创建并返回一个TDengine服务器连接
 */
WS_TAOS*
create_tdengine_connection(char* dsn)
{
    /* 尝试建立TDengine连接 */
    WS_TAOS* taos = ws_connect(dsn);
    
    /* 检查连接是否成功 */
    if (taos == NULL)
    {
        /* 获取连接错误信息 */
        int errno = ws_errno(NULL);
        const char* errstr = ws_errstr(NULL);
        
        elog(ERROR, "could not connect to TDengine: %s (error code: %d)",errstr, errno);
    }
    
    /* 返回已建立的连接对象 */
    return taos;
}

// TODO: 添加对超级表的处理
/*
 * 根据连接选项创建TDengine服务器连接
 */
static WS_TAOS*
tdengine_connect_server(tdengine_opt *opts)
{
    char dsn[1024];
    
    snprintf(dsn, sizeof(dsn), 
             "%s[+%s]://[%s:%s@]%s:%d/%s?%s",
             opts->driver ? opts->driver : "",          // 驱动类型
             opts->protocol ? opts->protocol : "",      // 协议类型
             opts->svr_username ? opts->svr_username : "", // 用户名
             opts->svr_password ? opts->svr_password : "", // 密码
             opts->svr_address ? opts->svr_address : "localhost", // 服务器地址
             opts->svr_port ? opts->svr_port : 6030,    // 服务器端口
             opts->svr_database ? opts->svr_database : ""); // 数据库名称
    
    return create_tdengine_connection(dsn);
}

/*
 * 关闭与TDengine服务器的连接并清理连接缓存项
 */
static void
tdengine_disconnect_server(ConnCacheEntry *entry)
{
    if (entry && entry->conn != NULL)
    {
        ws_close(entry->conn);
        entry->conn = NULL;
    }
}

/*
 * 连接失效回调函数，用于处理服务器或用户映射变更时的连接清理
 */
static void
tdengine_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
    HASH_SEQ_STATUS scan;
    ConnCacheEntry *entry;

    Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

    hash_seq_init(&scan, ConnectionHash);
    
    while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
    {
        /* 跳过空连接 */
        if (entry->conn == NULL)
            continue;

        if (hashvalue == 0 || /* 全局失效 */
            (cacheid == FOREIGNSERVEROID && entry->server_hashvalue == hashvalue) || /* 特定服务器失效 */
            (cacheid == USERMAPPINGOID && entry->mapping_hashvalue == hashvalue))   /* 特定用户映射失效 */
        {
            entry->invalidated = true;
            elog(DEBUG3, "tdengine_fdw: discarding connection %p", entry->conn);
            tdengine_disconnect_server(entry);
        }
    }
}

/*
 * 清理所有TDengine服务器连接
 */
void
tdengine_cleanup_connection(void)
{
    HASH_SEQ_STATUS scan;
    ConnCacheEntry *entry;

    if (ConnectionHash == NULL)
        return;

    hash_seq_init(&scan, ConnectionHash);
    
    while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
    {
        if (entry->conn == NULL)
            continue;

        tdengine_disconnect_server(entry);
    }
}
