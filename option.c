// 解析在Postgresql中使用fdw时的语句的OPTIONS

#include "postgres.h"
#include "tdengine_fdw.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

/*
 * 定义有效选项的结构
 */
struct TDengineFdwOption
{
    const char *optname;
    Oid         optcontext;  
};

/*
 * 有效的选项
 * 键：自定义的名称  值：PostgreSQL定义的OID
 */
static struct TDengineFdwOption valid_options[] = {
    {"host", ForeignServerRelationId},
    {"dbname", ForeignServerRelationId},
    {"port", ForeignServerRelationId},

    {"username", UserMappingRelationId},
    {"password", UserMappingRelationId},

	{"table", ForeignTableRelationId},
	{"column_name", AttributeRelationId},
	{"tags", ForeignTableRelationId},
	{"schemaless", ForeignTableRelationId},

	{"tags", AttributeRelationId},
	{"fields", AttributeRelationId},

    {NULL, InvalidOid}
};

extern Datum tdengine_fdw_validator(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(tdengine_fdw_validator);

bool tdengine_is_valid_option(const char *option, Oid context);

Datum tdengine_fdw_validator(PG_FUNCTION_ARGS)
{
    List       *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid         catalog = PG_GETARG_OID(1);
    ListCell   *cell;

    foreach(cell, options_list)
    {
        DefElem    *def = (DefElem *) lfirst(cell);

        if (!tdengine_is_valid_option(def->defname, catalog))
        {
            struct TDengineFdwOption *opt;
            StringInfoData buf;
            

            initStringInfo(&buf);
            for (opt = valid_options; opt->optname; opt++)
            {
                if (catalog == opt->optcontext)
                    appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
                                   opt->optname);
            }

            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME), 
                     errmsg("invalid option \"%s\"", def->defname),
                     errhint("Valid options in this context are: %s",
                            buf.data)));
        }

        // 校验：端口号
        if (strcmp(def->defname, "port") == 0)
        {
            int port = atoi(defGetString(def));
            if (port <= 0 || port > 65535)
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                         errmsg("port number must be between 1 and 65535")));
        }

        // TODO: 超级表支持
		// 校验：是否使用超级表
        // if (strcmp(def->defname, "using_stable") == 0)
        // {
        //     bool using_stable = defGetBoolean(def);
        //     if (using_stable && catalog == ForeignTableRelationId)
        //     {
        //         bool has_stable = false;
        //         ListCell *lc;

        //         foreach(lc, options_list)
        //         {
        //             DefElem *def2 = (DefElem *) lfirst(lc); 
        //             if (strcmp(def2->defname, "stable_name") == 0)
        //             {
        //                 has_stable = true;
        //                 break;
        //             }
        //         }
                
        //         if (!has_stable)
        //             ereport(ERROR,
        //                     (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
        //                      errmsg("when using_stable is true, stable_name must be specified")));
        //     }
        // }
    }

    PG_RETURN_VOID();
}

/*
 * 检查给定的选项是否是TDengine FDW的有效选项
 * 
 * @param option 要检查的选项名称
 * @param context 选项所属的目录对象OID
 * @return 如果选项有效返回true，否则返回false
 */
bool tdengine_is_valid_option(const char *option, Oid context)
{
    struct TDengineFdwOption *opt;

    /* 遍历valid_options数组中的所有有效选项 */
    for (opt = valid_options; opt->optname; opt++)
    {
        /* 检查当前选项是否匹配给定的context和option名称 */
        if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
            return true;
    }
    return false;
}

/*
 * 获取TDengine外部表的配置选项
 *
 * @foreigntableid 外部表的OID
 */
tdengine_opt *tdengine_get_options(Oid foreigntableid, Oid userid)
{
    /* 声明变量 */
    ForeignTable *f_table;
    ForeignServer *f_server; 
    UserMapping *f_mapping;
    List *options;
    ListCell *lc;
    tdengine_opt *opt;

    /* 分配并初始化选项结构体 */
    opt = (tdengine_opt *) palloc0(sizeof(tdengine_opt));

    /* 
     * 尝试获取外部表和服务器信息
     * 如果失败(可能是服务器而不是表)，则只获取服务器信息
     */
    PG_TRY();
    {
        f_table = GetForeignTable(foreigntableid);
        f_server = GetForeignServer(f_table->serverid);
    }
    PG_CATCH();
    {
        f_table = NULL;
        f_server = GetForeignServer(foreigntableid);
    }
    PG_END_TRY();
    
    /* 获取用户映射信息 */
    f_mapping = GetUserMapping(GetUserId(), f_server->serverid);

    /* 合并所有选项 */
    options = NIL;
    if (f_table)
        options = list_concat(options, f_table->options);
    options = list_concat(options, f_server->options);
    options = list_concat(options, f_mapping->options);

    /* 遍历并解析每个选项 */
    foreach(lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        /* 表名选项 */
        if (strcmp(def->defname, "table") == 0)
            opt->svr_table = defGetString(def);

        /* 主机地址选项 */
        if (strcmp(def->defname, "host") == 0)
            opt->svr_address = defGetString(def);

        /* 端口选项 */
        if (strcmp(def->defname, "port") == 0)
            opt->svr_port = atoi(defGetString(def));

        /* 用户名选项 */
        if (strcmp(def->defname, "user") == 0)
            opt->svr_username = defGetString(def);

        /* 密码选项 */
        if (strcmp(def->defname, "password") == 0)
            opt->svr_password = defGetString(def);

        /* 数据库名选项 */
        if (strcmp(def->defname, "dbname") == 0)
            opt->svr_database = defGetString(def);

        /* 表名别名选项 */
        if (strcmp(def->defname, "table_name") == 0)
            opt->svr_table = defGetString(def);

        /* Tags列表选项 */
        if (strcmp(def->defname, "tags") == 0)
            opt->tags_list = tdengineExtractTagsList(defGetString(def));

        /* 无模式选项 */
        if (strcmp(def->defname, "schemaless") == 0)
            opt->schemaless = defGetBoolean(def);
    }

    /* 如果没有显式设置表名，使用PostgreSQL中的表名 */
    if (!opt->svr_table && f_table)
        opt->svr_table = get_rel_name(foreigntableid);

    /* 验证必填选项 */
    if (opt->svr_address == NULL)
        elog(ERROR, "tdengine_fdw: Server Host not specified");

    if (opt->svr_database == NULL || strcmp(opt->svr_database, "") == 0)
        elog(ERROR, "tdengine_fdw: Database not specified");

    /* 设置默认值 */
    if (opt->svr_username == NULL)
        opt->svr_username = "";

    if (opt->svr_password == NULL)
        opt->svr_password = "";

    /* 设置默认端口 */
    if (!opt->svr_port)
        opt->svr_port = 6041;  /* TDengine REST API默认端口 */

    return opt;
}

/*
 * tdengineExtractTagsList: 解析逗号分隔的字符串并返回标签键列表
 *   @in_string: 输入的逗号分隔的标签字符串
 */
static List *tdengineExtractTagsList(char *in_string)
{
    List       *tags_list = NIL;

    /* 由于SplitIdentifierString会修改输入，所以先复制字符串 */
    if (!SplitIdentifierString(pstrdup(in_string), ',', &tags_list))
    {
        /* 标签列表语法错误处理 */
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("parameter \"%s\" must be a list of tag keys","tags")));
    }

    return tags_list;
}
