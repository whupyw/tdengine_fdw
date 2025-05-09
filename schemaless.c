#include "postgres.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "tdengine_fdw.h"

typedef struct pull_slvars_context
{
    Index varno;
    schemaless_info *pslinfo;
    List *columns;
    bool extract_raw;
    List *remote_exprs;
} pull_slvars_context;

static bool tdengine_slvars_walker(Node *node, pull_slvars_context *context);
static bool tdengine_is_att_dropped(Oid relid, AttrNumber attnum);
static void tdengine_validate_foreign_table_sc(Oid reloid);

/*
 * 检查节点是否为无模式(schemaless)类型变量
 *
 * 参数说明:
 * @oid 列的数据类型OID
 * @attnum 列属性编号
 * @pslinfo 无模式信息结构体指针
 * @is_tags 输出参数，返回是否为tags列
 * @is_fields 输出参数，返回是否为fields列
 */
bool tdengine_is_slvar(Oid oid, int attnum, schemaless_info *pslinfo, bool *is_tags, bool *is_fields)
{
    List *options;
    ListCell *lc;
    bool tags_opt = false;
    bool fields_opt = false;

    if (!pslinfo->schemaless)
        return false;

    options = GetForeignColumnOptions(pslinfo->relid, attnum);

    /* 遍历选项检查tags/fields设置 */
    foreach (lc, options)
    {
        DefElem *def = (DefElem *)lfirst(lc);

        if (strcmp(def->defname, "tags") == 0)
        {
            tags_opt = defGetBoolean(def);
            break;
        }
        else if (strcmp(def->defname, "fields") == 0)
        {
            fields_opt = defGetBoolean(def);
            break;
        }
    }

    /* 设置输出参数 */
    if (is_tags)
        *is_tags = tags_opt;
    if (is_fields)
        *is_fields = fields_opt;

    if ((oid == pslinfo->slcol_type_oid) &&
        (tags_opt || fields_opt))
        return true;

    return false;
}

/*
 * 检查节点是否为无模式(schemaless)变量的提取操作
 *
 * 参数说明:
 * @node 要检查的表达式节点
 * @pslinfo 无模式信息结构体指针
 */
bool tdengine_is_slvar_fetch(Node *node, schemaless_info *pslinfo)
{
    /* 获取操作表达式 */
    OpExpr *oe = (OpExpr *)node;
    Node *arg1;
    Node *arg2;

    /* 检查无模式是否启用 */
    if (!pslinfo->schemaless)
        return false;

    if (IsA(node, CoerceViaIO))
    {
        node = (Node *)(((CoerceViaIO *)node)->arg);
        oe = (OpExpr *)node;
    }
    if (!IsA(node, OpExpr))
        return false;
    if (oe->opno != pslinfo->jsonb_op_oid)
        return false;
    if (list_length(oe->args) != 2)
        return false;

    arg1 = (Node *)linitial(oe->args);
    arg2 = (Node *)lsecond(oe->args);

    /* 检查参数类型: 第一个必须是Var, 第二个必须是Const */
    if (!IsA(arg1, Var) || !IsA(arg2, Const))
        return false;

    if (!tdengine_is_slvar(((Var *)arg1)->vartype, ((Var *)arg1)->varattno, pslinfo, NULL, NULL))
        return false;

    return true;
}

/*
 * 检查节点是否为无模式(schemaless)类型参数的提取操作
 * 参数:
 *   @node: 要检查的表达式节点
 *   @pslinfo: 无模式信息结构体指针
 */
bool tdengine_is_param_fetch(Node *node, schemaless_info *pslinfo)
{
    OpExpr *oe = (OpExpr *)node;
    Node *arg1;
    Node *arg2;

    /* 检查无模式是否启用 */
    if (!pslinfo->schemaless)
        return false;

    if (!IsA(node, OpExpr))
        return false;
    if (oe->opno != pslinfo->jsonb_op_oid)
        return false;
    if (list_length(oe->args) != 2)
        return false;

    arg1 = (Node *)linitial(oe->args);
    arg2 = (Node *)lsecond(oe->args);

    /* 检查参数类型: 第一个必须是Param, 第二个必须是Const */
    if (!IsA(arg1, Param) || !IsA(arg2, Const))
        return false;

    return true;
}

/*
 * 从表达式节点中提取无模式(schemaless)变量的远程列名
 * 参数:
 *   @node: 要处理的表达式节点
 *   @pslinfo: 无模式信息结构体指针
 */
char *tdengine_get_slvar(Expr *node, schemaless_info *pslinfo)
{
    /* 检查无模式是否启用 */
    if (!pslinfo->schemaless)
        return NULL;

    /* 检查是否为无模式变量提取操作 */
    if (tdengine_is_slvar_fetch((Node *)node, pslinfo))
    {
        OpExpr *oe;
        Const *cnst;

        if (IsA(node, CoerceViaIO))
            node = (Expr *)(((CoerceViaIO *)node)->arg);

        oe = (OpExpr *)node;
        cnst = lsecond_node(Const, oe->args);

        return TextDatumGetCString(cnst->constvalue);
    }

    return NULL;
}

/*
 * 获取并初始化无模式(schemaless)处理所需的信息
 *
 * 参数说明:
 * @pslinfo 无模式信息结构体指针(输出参数)
 * @schemaless 是否启用无模式
 * @reloid 外部表OID
 */
void tdengine_get_schemaless_info(schemaless_info *pslinfo, bool schemaless, Oid reloid)
{
    /* 设置无模式标志 */
    pslinfo->schemaless = schemaless;

    if (schemaless)
    {
        if (pslinfo->slcol_type_oid == InvalidOid)
            pslinfo->slcol_type_oid = JSONBOID;

        if (pslinfo->jsonb_op_oid == InvalidOid)
            pslinfo->jsonb_op_oid = LookupOperName(NULL, list_make1(makeString("->>")),
                                                   pslinfo->slcol_type_oid, TEXTOID, true, -1);

        tdengine_validate_foreign_table_sc(reloid);

        /* 保存外部表OID供后续使用 */
        pslinfo->relid = reloid;
    }
}

/*
 * 递归遍历表达式树提取无模式(schemaless)变量
 *
 * 参数说明:
 * @node 当前处理的表达式节点
 */
static bool tdengine_slvars_walker(Node *node, pull_slvars_context *context)
{
    /* 空节点直接返回 */
    if (node == NULL)
        return false;

    /* 检查是否为无模式变量提取操作 */
    if (tdengine_is_slvar_fetch(node, context->pslinfo))
    {
        if (IsA(node, CoerceViaIO))
            node = (Node *)(((CoerceViaIO *)node)->arg);

        if (context->extract_raw)
        {
            ListCell *temp;
            foreach (temp, context->columns)
            {
                if (equal(lfirst(temp), node))
                {
                    OpExpr *oe1 = (OpExpr *)lfirst(temp);
                    OpExpr *oe2 = (OpExpr *)node;
                    if (oe1->location == oe2->location)
                        return false;
                }
            }
            foreach (temp, context->remote_exprs)
            {
                if (equal(lfirst(temp), node))
                {
                    OpExpr *oe1 = (OpExpr *)lfirst(temp);
                    OpExpr *oe2 = (OpExpr *)node;
                    if (oe1->location == oe2->location)
                        return false;
                }
            }
            context->columns = lappend(context->columns, node);
        }
        else
        {
            /* 解析操作表达式获取变量和常量 */
            OpExpr *oe = (OpExpr *)node;
            Var *var = linitial_node(Var, oe->args);
            Const *cnst = lsecond_node(Const, oe->args);

            if (var->varno == context->varno && var->varlevelsup == 0)
            {
                char *const_str = TextDatumGetCString(cnst->constvalue);

                /* 检查列名是否已存在 */
                ListCell *temp;
                foreach (temp, context->columns)
                {
                    char *colname = strVal(lfirst(temp));
                    Assert(colname != NULL);

                    if (strcmp(colname, const_str) == 0)
                    {
                        return false;
                    }
                }
                context->columns = lappend(context->columns, makeString(const_str));
            }
        }
    }

    /* 递归遍历子节点 */
    return expression_tree_walker(node, tdengine_slvars_walker,(void *)context);
}

/*
 * 提取无模式(schemaless)变量中的远程列名
 *
 * 参数说明:
 * @expr 要分析的表达式树
 * @varno 变量编号
 * @columns 初始列名列表
 * @extract_raw 是否提取原始表达式
 * @remote_exprs 远程表达式列表
 * @pslinfo 无模式信息结构体指针
 */
List * tdengine_pull_slvars(Expr *expr, Index varno, List *columns, bool extract_raw, List *remote_exprs, schemaless_info *pslinfo)
{
    pull_slvars_context context;

    /* 初始化上下文结构体 */
    memset(&context, 0, sizeof(pull_slvars_context));

    context.varno = varno;
    context.columns = columns;
    context.pslinfo = pslinfo;
    context.extract_raw = extract_raw;
    context.remote_exprs = remote_exprs;

    (void)tdengine_slvars_walker((Node *)expr, &context);

    /* 返回收集到的列名列表 */
    return context.columns;
}

/*
 * tdengine_is_att_dropped: 检查表属性是否已被删除
 *
 * 参数:
 *   @relid: 表的关系OID
 *   @attnum: 属性编号
 */
static bool tdengine_is_att_dropped(Oid relid, AttrNumber attnum)
{
    HeapTuple tp;

    /* 在系统缓存中查找属性信息 */
    tp = SearchSysCache2(ATTNUM,
                         ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (HeapTupleIsValid(tp))
    {
        Form_pg_attribute att_tup = (Form_pg_attribute)GETSTRUCT(tp);

        bool result = att_tup->attisdropped;

        /* 释放系统缓存 */
        ReleaseSysCache(tp);
        return result;
    }

    return false;
}

/*
 * 验证无模式(schemaless)下外部表的格式
 *
 * 参数:
 * @reloid 外部表的OID
 */
static void tdengine_validate_foreign_table_sc(Oid reloid)
{
    int attnum = 1; /* 列索引从1开始 */

    while (true)
    {
        /* 获取列名和类型 */
        char *attname = get_attname(reloid, attnum, true);
        Oid atttype = get_atttype(reloid, attnum);
        bool att_is_dropped = tdengine_is_att_dropped(reloid, attnum);

        if (att_is_dropped)
        {
            attnum++;
            continue;
        }

        if (attname == NULL || atttype == InvalidOid)
            break;

        if (strcmp(attname, "time") == 0)
        {
            if (atttype != TIMESTAMPOID &&
                atttype != TIMESTAMPTZOID)
            {
                elog(ERROR, "tdengine fdw: invalid data type for time column");
            }
        }
        else if (strcmp(attname, "time_text") == 0)
        {
            if (atttype != TEXTOID)
            {
                elog(ERROR, "tdengine fdw: invalid data type for time_text column");
            }
        }
        else if (strcmp(attname, "tags") == 0 || strcmp(attname, "fields") == 0)
        {
            List *options = NIL;

            if (atttype != JSONBOID)
                elog(ERROR, "tdengine fdw: invalid data type for tags/fields column");

            options = GetForeignColumnOptions(reloid, attnum);
            if (options != NIL)
            {
                DefElem *def = (DefElem *)linitial(options);

                if (defGetBoolean(def) != true)
                    elog(ERROR, "tdengine fdw: invalid option value for tags/fields column");
            }
        }
        /* 验证其他列 */
        else
        {
            if (atttype == TIMESTAMPOID ||
                atttype == TIMESTAMPTZOID ||
                atttype == TEXTOID)
            {
                List *options = GetForeignColumnOptions(reloid, attnum);
                if (options != NIL)
                {
                    DefElem *def = (DefElem *)linitial(options);
                    if (strcmp(defGetString(def), "time") != 0)
                        elog(ERROR, "tdengine fdw: invalid option value for time/time_text column");
                }
                else
                    elog(ERROR, "tdengine fdw: invalid column name of time/time_text in schemaless mode");
            }
            else if (atttype == JSONBOID)
            {
                List *options = GetForeignColumnOptions(reloid, attnum);
                if (options != NIL)
                {
                    DefElem *def = (DefElem *)linitial(options);
                    if (defGetBoolean(def) != true)
                        elog(ERROR, "tdengine fdw: invalid option value for tags/fields column");
                }
                else
                    elog(ERROR, "tdengine fdw: invalid column name of tags/fields in schemaless mode");
            }
            /* 其他类型列不允许 */
            else
                elog(ERROR, "tdengine fdw: invalid column in schemaless mode. Only time, time_text, tags and fields columns are accepted.");
        }

        attnum++;
    }
}
