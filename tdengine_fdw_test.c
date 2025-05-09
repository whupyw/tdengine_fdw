#include "postgres.h"

#include "tdengine_fdw.h"

#include <stdio.h>

#include "access/reloptions.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/appendinfo.h"

#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/paths.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/typcache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

extern PGDLLEXPORT void _PG_init(void);

static void tdengine_fdw_exit(int code, Datum arg);

extern Datum tdengine_fdw_handler(PG_FUNCTION_ARGS);
extern Datum tdengine_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(tdengine_fdw_handler);
PG_FUNCTION_INFO_V1(tdengine_fdw_version);

// 用于估计外部表的大小和成本，为查询规划器提供必要的信息。
static void tdengineGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
// 生成访问外部表的不同路径，帮助规划器选择最优查询路径。
static void tdengineGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
// 根据选择的最佳路径生成外部扫描计划。
static ForeignScan *tdengineGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan);
// 获取执行ForeignScan算子所需的信息，并将它们组织并保存在ForeignScanState中
static void tdengineBeginForeignScan(ForeignScanState *node,
                                     int eflags);
// 读取外部数据源的一行数据，并将它组织为PG中的Tuple
static TupleTableSlot *tdengineIterateForeignScan(ForeignScanState *node);
// 将外部数据源的读取位置重置回最初的起始位置
static void tdengineReScanForeignScan(ForeignScanState *node);
// 释放整个ForeignScan算子执行过程中占用的外部资源或FDW中的资源
static void tdengineEndForeignScan(ForeignScanState *node);

static void tdengine_to_pg_type(StringInfo str, char *typname);

static void prepare_query_params(PlanState *node, List *fdw_exprs, List *remote_exprs, Oid foreigntableid, int numParams, FmgrInfo **param_flinfo, List **param_exprs, const char ***param_values, Oid **param_types, TDengineType **param_tdengine_types, TDengineValue **param_tdengine_values, TDengineColumnInfo **param_column_info);

static void process_query_params(ExprContext *econtext, FmgrInfo *param_flinfo, List *param_exprs, const char **param_values, Oid *param_types, TDengineType *param_tdengine_types, TDengineValue *param_tdengine_values, TDengineColumnInfo *param_column_info);

static void create_cursor(ForeignScanState *node);
static void execute_dml_stmt(ForeignScanState *node);
static TupleTableSlot **execute_foreign_insert_modify(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot **slots, TupleTableSlot **planSlots, int numSlots);
static int tdengine_get_batch_size_option(Relation rel);

/*
 * 此枚举描述了 ForeignPath 的 fdw_private 列表中存储的内容。
 */
enum FdwPathPrivateIndex
{
    FdwPathPrivateHasFinalSort,
    FdwPathPrivateHasLimit,
};

enum FdwModifyPrivateIndex
{
    FdwModifyPrivateUpdateSql,
    FdwModifyPrivateTargetAttnums,
};

enum FdwDirectModifyPrivateIndex
{
    FdwDirectModifyPrivateUpdateSql,
    FdwDirectModifyPrivateHasReturning,
    FdwDirectModifyPrivateRetrievedAttrs,
    FdwDirectModifyPrivateSetProcessed,
    FdwDirectModifyRemoteExprs
};

typedef struct TDengineFdwDirectModifyState
{
    Relation rel;
    UserMapping *user;
    AttInMetadata *attinmeta;

    char *query;
    bool has_returning;
    List *retrieved_attrs;
    bool set_processed;

    char **params;
    int numParams;
    FmgrInfo *param_flinfo;
    List *param_exprs;
    const char **param_values;
    Oid *param_types;
    TDengineType *param_tdengine_types;
    TDengineValue *param_tdengine_values;
    TDengineColumnInfo *param_column_info;

    tdengine_opt *tdengineFdwOptions;

    int num_tuples;
    int next_tuple;
    Relation resultRel;
    AttrNumber *attnoMap;
    AttrNumber ctidAttno;
    AttrNumber oidAttno;
    bool hasSystemCols;

    MemoryContext temp_cxt;
} TDengineFdwDirectModifyState;

/*
 * PostgreSQL扩展初始化函数
 */
void _PG_init(void)
{
    /* 注册进程退出回调函数 */
    on_proc_exit(&tdengine_fdw_exit, PointerGetDatum(NULL));
}

/*
 * TDengine FDW 退出回调函数
 *
 * @code 退出状态码
 * @arg 回调参数(未使用)
 */
static void tdengine_fdw_exit(int code, Datum arg)
{
    /* 清理TDengine C++客户端连接 */
    cleanup_cxx_client_connection();
}

Datum tdengine_fdw_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(CODE_VERSION);
}

/**
 * =====================注册回调函数======================
 */
Datum tdengine_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    fdwroutine->GetForeignRelSize = tdengineGetForeignRelSize;
    fdwroutine->GetForeignPaths = tdengineGetForeignPaths;
    fdwroutine->GetForeignPlan = tdengineGetForeignPlan;

    fdwroutine->BeginForeignScan = tdengineBeginForeignScan;
    fdwroutine->IterateForeignScan = tdengineIterateForeignScan;
    fdwroutine->ReScanForeignScan = tdengineReScanForeignScan;
    fdwroutine->EndForeignScan = tdengineEndForeignScan;

    PG_RETURN_POINTER(fdwroutine);
}

//========================= GetForeignRelSize ===========================
/*
 * 获取给定外部关系的外部扫描的成本和大小估计
 */
static void
estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel, List *param_join_conds, List *pathkeys, double *p_rows, int *p_width, Cost *p_startup_cost, Cost *p_total_cost)
{
    TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)foreignrel->fdw_private;
    double rows;
    double retrieved_rows;
    int width;
    Cost startup_cost;
    Cost total_cost;
    Cost cpu_per_tuple;

    if (fpinfo->use_remote_estimate)
    {
        ereport(ERROR, (errmsg("Remote estimation is unsupported")));
    }
    else
    {
        Cost run_cost = 0;
        /*
         */
        Assert(param_join_conds == NIL);
        /*
         * 对基本对外关系使用set_baserel_size_estimates（）进行的行/宽度估计，
         * 对外关系之间的连接使用set_joinrel_size_estimates（）进行的行/宽度估计。
         */
        rows = foreignrel->rows;
        width = foreignrel->reltarget->width;

        retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);

        /*
         * 如果已经缓存了成本，直接使用；
         */
        if (fpinfo->rel_startup_cost > 0 && fpinfo->rel_total_cost > 0)
        {
            startup_cost = fpinfo->rel_startup_cost;
            run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;
        }
        /* 否则将其视为顺序扫描来计算成本*/
        else
        {
            Assert(foreignrel->reloptkind != RELOPT_JOINREL);
            retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

            // 初始化启动成本为 0
            startup_cost = 0;
            // 初始化运行成本为 0
            run_cost = 0;
            // 计算顺序扫描页面的成本，即顺序扫描页面成本乘以页面数量
            run_cost += seq_page_cost * foreignrel->pages;

            // 将基础关系的限制条件的启动成本加到总启动成本中
            startup_cost += foreignrel->baserestrictcost.startup;
            // 计算每行的 CPU 成本，包括基础的元组 CPU 成本和基础关系限制条件的每行成本
            cpu_per_tuple =
                cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
            run_cost += cpu_per_tuple * foreignrel->tuples;
        }

        if (pathkeys != NIL)
        {

            startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;

            run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
        }

        total_cost = startup_cost + run_cost;
    }
    // 检查当前扫描是否没有指定排序键（pathkeys）且没有与外部关系的参数化连接条件（param_join_conds）
    // 若满足条件，则进行成本缓存操作
    if (pathkeys == NIL && param_join_conds == NIL)
    {
        // 将当前计算得到的启动成本（startup_cost）赋值给 TDengineFdwRelationInfo 结构体中的 rel_startup_cost 成员
        fpinfo->rel_startup_cost = startup_cost;
        // 将当前计算得到的总成本（total_cost）赋值给 TDengineFdwRelationInfo 结构体中的 rel_total_cost 成员
        fpinfo->rel_total_cost = total_cost;
    }
    /*
     * 额外开销
     */
    startup_cost += fpinfo->fdw_startup_cost;
    total_cost += fpinfo->fdw_startup_cost;
    total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
    total_cost += cpu_tuple_cost * retrieved_rows;
    /* 返回值. */
    *p_rows = rows;
    *p_width = width;
    *p_startup_cost = startup_cost;
    *p_total_cost = total_cost;
}

/*
 * 提取实际从远程 TDengine 服务器获取的列信息。
 */
static void
tdengine_extract_slcols(TDengineFdwRelationInfo *fpinfo, PlannerInfo *root, RelOptInfo *baserel, List *tlist)
{
    // 存储范围表项信息
    RangeTblEntry *rte;
    // 输入目标列表，若 tlist 不为空则使用 tlist，否则使用 baserel 的目标表达式列表
    List *input_tlist = (tlist) ? tlist : baserel->reltarget->exprs;
    // 指向 ListCell 的指针，用于遍历列表
    ListCell *lc = NULL;

    // 是否为无模式（schemaless），不是则直接返回，
    if (!fpinfo->slinfo.schemaless)
        return;

    rte = planner_rt_fetch(baserel->relid, root);
    // tdengine_is_select_all 函数判断是否为全列选择，并将结果存储在 fpinfo 的 all_fieldtag 成员中
    fpinfo->all_fieldtag = tdengine_is_select_all(rte, input_tlist, &fpinfo->slinfo);

    // 如果是全列选择，则直接返回，无需进一步提取列信息
    if (fpinfo->all_fieldtag)
        return;

    // 初始化 fpinfo 的 slcols 成员为空列表
    fpinfo->slcols = NIL;
    fpinfo->slcols = tdengine_pull_slvars((Expr *)input_tlist, baserel->relid, fpinfo->slcols, false, NULL, &(fpinfo->slinfo));

    // 遍历 fpinfo 的 local_conds 列表，其中存储了本地条件信息
    foreach (lc, fpinfo->local_conds)
    {
        // 从列表中取出当前的 RestrictInfo 结构体
        RestrictInfo *ri = (RestrictInfo *)lfirst(lc);

        fpinfo->slcols = tdengine_pull_slvars(ri->clause, baserel->relid, fpinfo->slcols, false, NULL, &(fpinfo->slinfo));
    }
}

/*
 * tdengineGetForeignRelSize: 为外部表的扫描创建一个 FdwPlan
 */
static void tdengineGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    TDengineFdwRelationInfo *fpinfo;
    tdengine_opt *options;
    ListCell *lc;
    Oid userid;

    // 从规划器的范围表中获取当前外部表的范围表条目(RTE)
    RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    fpinfo = (TDengineFdwRelationInfo *)palloc0(sizeof(TDengineFdwRelationInfo));
    baserel->fdw_private = (void *)fpinfo;

    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

    options = tdengine_get_options(foreigntableid, userid);

    // 无模式表不需要预定义严格的表结构
    tdengine_get_schemaless_info(&(fpinfo->slinfo), options->schemaless, foreigntableid);

    fpinfo->pushdown_safe = true;

    // 从系统目录中获取外部表定义信息
    fpinfo->table = GetForeignTable(foreigntableid);
    // 从系统目录中获取外部服务器定义信息
    fpinfo->server = GetForeignServer(fpinfo->table->serverid);

    /*
     * 分类处理限制条件子句：
     * 1. 可以下推到远程服务器执行的子句(remote_conds)
     * 2. 必须在本地执行的子句(local_conds)
     */
    foreach (lc, baserel->baserestrictinfo)
    {
        RestrictInfo *ri = (RestrictInfo *)lfirst(lc);

        if (tdengine_is_foreign_expr(root, baserel, ri->clause, false))
            fpinfo->remote_conds = lappend(fpinfo->remote_conds, ri);
        else
            fpinfo->local_conds = lappend(fpinfo->local_conds, ri);
    }
    pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);

    foreach (lc, fpinfo->local_conds)
    {
        RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);
        pull_varattnos((Node *)rinfo->clause, baserel->relid, &fpinfo->attrs_used);
    }

    fpinfo->local_conds_sel = clauselist_selectivity(root, fpinfo->local_conds, baserel->relid, JOIN_INNER, NULL);
    fpinfo->rel_startup_cost = -1;
    fpinfo->rel_total_cost = -1;
    if (fpinfo->use_remote_estimate)
    {
        ereport(ERROR, (errmsg("Remote estimation is unsupported")));
    }
    else
    {
        if (baserel->tuples < 0)
        {
            baserel->pages = 10;
            baserel->tuples = (10 * BLCKSZ) / (baserel->reltarget->width +
                                               MAXALIGN(SizeofHeapTupleHeader));
        }

        set_baserel_size_estimates(root, baserel);

        estimate_path_cost_size(root, baserel, NIL, NIL, &fpinfo->rows, &fpinfo->width, &fpinfo->startup_cost, &fpinfo->total_cost);
    }

    fpinfo->relation_name = psprintf("%u", baserel->relid);
}

//========================== GetForeignPaths ====================
/*
 *      为对外表的扫描创建可能的扫描路径
 */
static void
tdengineGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    // 启动成本初始化为 10
    Cost startup_cost = 10;
    Cost total_cost = baserel->rows + startup_cost;

    // 输出调试信息，显示当前函数名
    elog(DEBUG1, "tdengine_fdw : %s", __func__);
    // 重新设置总成本为表的行数
    total_cost = baserel->rows;

    /* 创建一个 ForeignPath 节点并将其作为唯一可能的路径添加 */
    add_path(baserel, (Path *)
    // 创建一个外部扫描路径
    create_foreignscan_path(root, baserel, NULL, baserel->rows, startup_cost, total_cost, NIL, baserel->lateral_relids, NULL, NULL));
}

//====================== GetForeignPlan ======================
/*
 * 获取一个外部扫描计划节点
 */
static ForeignScan *
tdengineGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
    // 将外部关系的私有数据转换为 TDengineFdwRelationInfo
    TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)baserel->fdw_private;
    // 获取扫描关系的ID
    Index scan_relid = baserel->relid;
    // 传递给执行器的私有数据列表
    List *fdw_private = NULL;
    // 本地执行的表达式列表
    List *local_exprs = NULL;
    // 远程执行的表达式列表
    List *remote_exprs = NULL;
    // 参数列表
    List *params_list = NULL;
    // 传递给外部服务器的目标列表
    List *fdw_scan_tlist = NIL;
    // 远程条件列表
    List *remote_conds = NIL;

    StringInfoData sql;
    // 从远程服务器检索的属性列表
    List *retrieved_attrs;
    ListCell *lc;
    List *fdw_recheck_quals = NIL;
    // 表示是否为 FOR UPDATE 操作的标志
    int for_update;
    // 表示查询是否有 LIMIT 子句的标志
    bool has_limit = false;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    // 决定是否在目标列表中支持函数下推
    fpinfo->is_tlist_func_pushdown = tdengine_is_foreign_function_tlist(root, baserel, tlist);

    /*
     * 获取由 tdengineGetForeignUpperPaths() 创建的 FDW 私有数据
     */
    if (best_path->fdw_private)
    {
        has_limit = boolVal(list_nth(best_path->fdw_private, FdwPathPrivateHasLimit));
    }

    // 初始化 SQL 查询字符串
    initStringInfo(&sql);

    if ((baserel->reloptkind == RELOPT_BASEREL ||
         baserel->reloptkind == RELOPT_OTHER_MEMBER_REL) &&
        fpinfo->is_tlist_func_pushdown == false)
    {
        // 提取实际要从远程 TDengine 获取的列
        tdengine_extract_slcols(fpinfo, root, baserel, tlist);

        // 遍历扫描子句列表
        foreach (lc, scan_clauses)
        {
            // 获取当前的限制信息节点
            RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);

            Assert(IsA(rinfo, RestrictInfo));

            if (rinfo->pseudoconstant)
                continue;

            if (list_member_ptr(fpinfo->remote_conds, rinfo))
            {
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            }
            else if (list_member_ptr(fpinfo->local_conds, rinfo))
            {
                local_exprs = lappend(local_exprs, rinfo->clause);
            }
            else if (tdengine_is_foreign_expr(root, baserel, rinfo->clause, false))
            {
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            }
            else
            {
                local_exprs = lappend(local_exprs, rinfo->clause);
            }

            fdw_recheck_quals = remote_exprs;
        }
    }
    else
    {
        scan_relid = 0;

        if (fpinfo->is_tlist_func_pushdown == false)
        {
            // 确保扫描子句列表为空
            Assert(!scan_clauses);
        }

        // 提取实际的远程条件子句
        remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
        // 提取实际的本地条件子句
        local_exprs = extract_actual_clauses(fpinfo->local_conds, false);
        if (fpinfo->is_tlist_func_pushdown == true)
        {
            // 遍历目标列表
            foreach (lc, tlist)
            {
                // 获取当前的目标项
                TargetEntry *tle = lfirst_node(TargetEntry, lc);
                */
                    if (fpinfo->is_tlist_func_pushdown == true && IsA((Node *)tle->expr, FieldSelect))
                {
                    // 将提取的函数添加到 fdw_scan_tlist 中
                    fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist, tdengine_pull_func_clause((Node *)tle->expr));
                }
                else
                {
                    // 否则将目标项添加到 fdw_scan_tlist 中
                    fdw_scan_tlist = lappend(fdw_scan_tlist, tle);
                }
            }

            // 遍历本地条件列表
            foreach (lc, fpinfo->local_conds)
            {
                // 获取当前的限制信息节点
                RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
                // 存储变量列表
                List *varlist = NIL;

                // 从条件子句中提取无模式变量
                varlist = tdengine_pull_slvars(rinfo->clause, baserel->relid,
                                               varlist, true, NULL, &(fpinfo->slinfo));

                if (varlist == NIL)
                {
                    varlist = pull_var_clause((Node *)rinfo->clause,
                                              PVC_RECURSE_PLACEHOLDERS);
                }

                fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist, varlist);
            }
        }
        else
        {
            // 构建要解析的目标列表
            fdw_scan_tlist = tdengine_build_tlist_to_deparse(baserel);
        }

        if (outer_plan)
        {
            /*
             * 目前，我们只考虑连接之外的分组和聚合。
             * 涉及聚合或分组的查询不需要 EPQ 机制，因此这里不应该有外部计划。
             */
            Assert(baserel->reloptkind != RELOPT_UPPER_REL);
            // 设置外部计划的目标列表
            outer_plan->targetlist = fdw_scan_tlist;

            // 遍历本地表达式列表
            foreach (lc, local_exprs)
            {
                // 将外部计划转换为连接计划
                Join *join_plan = (Join *)outer_plan;
                // 获取当前的条件子句
                Node *qual = lfirst(lc);

                // 从外部计划的条件中移除当前条件子句
                outer_plan->qual = list_delete(outer_plan->qual, qual);

                if (join_plan->jointype == JOIN_INNER)
                {
                    // 从连接计划的连接条件中移除当前条件子句
                    join_plan->joinqual = list_delete(join_plan->joinqual,
                                                      qual);
                }
            }
        }
    }

    // 重新初始化 SQL 查询字符串
    initStringInfo(&sql);
    // 为关系解析 SELECT 语句
    tdengine_deparse_select_stmt_for_rel(&sql, root, baserel, fdw_scan_tlist,
                                         remote_exprs, best_path->path.pathkeys,
                                         false, &retrieved_attrs, &params_list, has_limit);

    // 记住远程表达式，供 tdenginePlanDirectModify 可能使用
    fpinfo->final_remote_exprs = remote_exprs;

    for_update = false;
    if (baserel->relid == root->parse->resultRelation &&
        (root->parse->commandType == CMD_UPDATE ||
         root->parse->commandType == CMD_DELETE))
    {
        /* 关系是 UPDATE/DELETE 目标，因此使用 FOR UPDATE */
        for_update = true;
    }

    // 获取远程条件
    if (baserel->reloptkind == RELOPT_UPPER_REL)
    {
        TDengineFdwRelationInfo *ofpinfo;

        ofpinfo = (TDengineFdwRelationInfo *)fpinfo->outerrel->fdw_private;
        remote_conds = ofpinfo->remote_conds;
    }
    else
        remote_conds = remote_exprs;

    // 创建包含 SQL 查询字符串、检索的属性和 FOR UPDATE 标志的列表
    fdw_private = list_make3(makeString(sql.data), retrieved_attrs, makeInteger(for_update));
    // 将 fdw_scan_tlist 添加到 fdw_private 列表中
    fdw_private = lappend(fdw_private, fdw_scan_tlist);
    fdw_private = lappend(fdw_private, makeInteger(fpinfo->is_tlist_func_pushdown));
    fdw_private = lappend(fdw_private, makeInteger(fpinfo->slinfo.schemaless));
    fdw_private = lappend(fdw_private, remote_conds);

    /*
     * 根据目标列表、本地过滤表达式、远程参数表达式和 FDW 私有信息创建 ForeignScan 节点。
     */
    return make_foreignscan(tlist, local_exprs, scan_relid, params_list, fdw_private, fdw_scan_tlist, fdw_recheck_quals, outer_plan);
}

//========================== BeginForeignScan =====================
/*
 * tdengineBeginForeignScan - 初始化外部表扫描
 * 功能: 为TDengine外部表扫描初始化执行状态,准备查询参数和连接信息
 * 参数:
 *   @node: ForeignScanState节点,包含扫描状态信息
 *   @eflags: 执行标志位,指示扫描的执行方式
 */
static void
tdengineBeginForeignScan(ForeignScanState *node, int eflags)
{
    // 执行状态结构体
    TDengineFdwExecState *festate = NULL;
    // 执行状态
    EState *estate = node->ss.ps.state;
    // 外部扫描计划节点
    ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
    // 范围表条目
    RangeTblEntry *rte;
    // 参数数量
    int numParams;
    // 扫描关系ID
    int rtindex;
    // 无模式标志
    bool schemaless;
    // 用户ID
    Oid userid;

    ForeignTable *ftable;

    List *remote_exprs;

    // 调试日志
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* 初始化执行状态 */
    festate = (TDengineFdwExecState *)palloc0(sizeof(TDengineFdwExecState));
    node->fdw_state = (void *)festate;
    festate->rowidx = 0;

    /* 从计划节点中提取信息 */
    festate->query = strVal(list_nth(fsplan->fdw_private, 0));                                 // SQL查询字符串
    festate->retrieved_attrs = list_nth(fsplan->fdw_private, 1);                               // 需要检索的属性
    festate->for_update = intVal(list_nth(fsplan->fdw_private, 2)) ? true : false;             // FOR UPDATE标志
    festate->tlist = (List *)list_nth(fsplan->fdw_private, 3);                                 // 目标列表
    festate->is_tlist_func_pushdown = intVal(list_nth(fsplan->fdw_private, 4)) ? true : false; // 函数下推标志
    schemaless = intVal(list_nth(fsplan->fdw_private, 5)) ? true : false;                      // 无模式标志
    remote_exprs = (List *)list_nth(fsplan->fdw_private, 6);                                   // 远程表达式列表

    festate->cursor_exists = false;

    /* 确定扫描关系ID */
    if (fsplan->scan.scanrelid > 0)
        rtindex = fsplan->scan.scanrelid;
    else
        rtindex = bms_next_member(fsplan->fs_relids, -1);

    // 获取范围表条目
    rte = exec_rt_fetch(rtindex, estate);

    /* 获取用户ID */
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

    /* 获取连接选项和用户映射 */
    festate->tdengineFdwOptions = tdengine_get_options(rte->relid, userid);
    ftable = GetForeignTable(rte->relid);
    festate->user = GetUserMapping(userid, ftable->serverid);

    /* 初始化无模式信息 */
    tdengine_get_schemaless_info(&(festate->slinfo), schemaless, rte->relid);

    /* 准备查询参数 */
    numParams = list_length(fsplan->fdw_exprs);
    festate->numParams = numParams;
    if (numParams > 0)
    {
        prepare_query_params((PlanState *)node, fsplan->fdw_exprs, remote_exprs, rte->relid, numParams, &festate->param_flinfo, &festate->param_exprs, &festate->param_values, &festate->param_types, &festate->param_tdengine_types, &festate->param_tdengine_values, &festate->param_column_info);
    }
}

//======================== IterateForeignScan ==================
/*
 * 逐个迭代从 TDengine 获取行，并将其放入元组槽中
 */
static TupleTableSlot *
tdengineIterateForeignScan(ForeignScanState *node)
{
    // 获取执行状态
    TDengineFdwExecState *festate = (TDengineFdwExecState *)node->fdw_state;
    TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
    EState *estate = node->ss.ps.state;
    TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
    tdengine_opt *options;
    // 存储查询返回结果
    struct TDengineQuery_return volatile ret;
    // 存储查询结果
    struct TDengineResult volatile *result = NULL;
    ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
    RangeTblEntry *rte;
    int rtindex;
    // 是否为聚合操作
    bool is_agg;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);
    if (fsplan->scan.scanrelid > 0)
    {
        rtindex = fsplan->scan.scanrelid;
        is_agg = false;
    }
    else
    {
        rtindex = bms_next_member(fsplan->fs_relids, -1);
        is_agg = true;
    }
    rte = rt_fetch(rtindex, estate->es_range_table);

    // 获取 TDengine 选项
    options = festate->tdengineFdwOptions;
    if (!festate->cursor_exists)
        create_cursor(node);

    // 初始化元组槽的值为 0
    memset(tupleSlot->tts_values, 0, sizeof(Datum) * tupleDescriptor->natts);
    // 初始化元组槽的空值标记为 true
    memset(tupleSlot->tts_isnull, true, sizeof(bool) * tupleDescriptor->natts);
    // 清空元组槽
    ExecClearTuple(tupleSlot);

    if (festate->rowidx == 0)
    {
        // 保存旧的内存上下文
        MemoryContext oldcontext = NULL;

        // 异常处理开始
        PG_TRY();
        {
            oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
            ret = TDengineQuery(festate->query, festate->user, options, festate->param_tdengine_types, festate->param_tdengine_values, festate->numParams);
            if (ret.r1 != NULL)
            {
                // 复制错误信息
                char *err = pstrdup(ret.r1);
                // 释放原错误信息
                free(ret.r1);
                ret.r1 = err;
                // 打印错误信息
                elog(ERROR, "tdengine_fdw : %s", err);
            }

            result = ret.r0;
            festate->temp_result = (void *)result;

            // 获取结果集的行数
            festate->row_nums = result->nrow;
            // 打印查询信息
            elog(DEBUG1, "tdengine_fdw : query: %s", festate->query);

            // 切换回旧的内存上下文
            MemoryContextSwitchTo(oldcontext);
            // 释放结果集
            TDengineFreeResult((TDengineResult *)result);
        }
        // 异常处理捕获部分
        PG_CATCH();
        {
            if (ret.r1 == NULL)
            {
                // 释放结果集
                TDengineFreeResult((TDengineResult *)result);
            }

            if (oldcontext)
                // 切换回旧的内存上下文
                MemoryContextSwitchTo(oldcontext);

            // 重新抛出异常
            PG_RE_THROW();
        }
        // 异常处理结束
        PG_END_TRY();
    }

    if (festate->rowidx < festate->row_nums)
    {
        // 保存旧的内存上下文
        MemoryContext oldcontext = NULL;

        // 获取结果集
        result = (TDengineResult *)festate->temp_result;
        // 从结果行创建元组
        make_tuple_from_result_row(&(result->rows[festate->rowidx]),(TDengineResult *)result,tupleDescriptor,tupleSlot->tts_values,tupleSlot->tts_isnull,rte->relid,festate,is_agg);
        // 切换到查询上下文
        oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

        // 释放结果行
        freeTDengineResultRow(festate, festate->rowidx);

        if (festate->rowidx == (festate->row_nums - 1))
        {
            // 释放结果集
            freeTDengineResult(festate);
        }

        // 切换回旧的内存上下文
        MemoryContextSwitchTo(oldcontext);

        // 存储虚拟元组
        ExecStoreVirtualTuple(tupleSlot);
        // 行索引加 1
        festate->rowidx++;
    }

    // 返回元组槽
    return tupleSlot;
}

//===================== ReScanForeignScan =====================
/*
 * 从扫描的起始位置重新开始扫描
 */
static void
tdengineReScanForeignScan(ForeignScanState *node)
{

    TDengineFdwExecState *festate = (TDengineFdwExecState *)node->fdw_state;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    festate->cursor_exists = false;
    festate->rowidx = 0;
}

//===================== EndForeignScan =======================
/*
 * 结束对外部表的扫描，并释放本次扫描所使用的对象
 */
static void
tdengineEndForeignScan(ForeignScanState *node)
{
    TDengineFdwExecState *festate = (TDengineFdwExecState *)node->fdw_state;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    if (festate != NULL)
    {
        festate->cursor_exists = false;
        festate->rowidx = 0;
    }
}

/*
 * tdengineAddForeignUpdateTargets为外部表的更新/删除操作添加所需的resjunk列
 *
 * @root: 规划器信息
 * @rtindex: 范围表索引
 * @target_rte: 目标范围表条目
 * @target_relation: 目标关系(表)
 */
static void
tdengineAddForeignUpdateTargets(PlannerInfo *root, Index rtindex, RangeTblEntry *target_rte, Relation target_relation)
{
    // 获取目标关系的OID
    Oid relid = RelationGetRelid(target_relation);
    // 获取目标关系的元组描述符
    TupleDesc tupdesc = target_relation->rd_att;
    int i;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* 遍历外部表的所有列 */
    for (i = 0; i < tupdesc->natts; i++)
    {
        // 获取当前列的属性信息
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        // 获取当前列的属性编号
        AttrNumber attrno = att->attnum;
        // 获取当前列的名称
        char *colname = tdengine_get_column_name(relid, attrno);

        /* 如果是时间列或标签列 */
        if (TDENGINE_IS_TIME_COLUMN(colname) || tdengine_is_tag_key(colname, relid))
        {
            Var *var;

            /* 创建一个Var节点表示所需的值 */
            var = makeVar(rtindex, attrno, att->atttypid, att->atttypmod, att->attcollation, 0);

            /* 将其注册为该目标关系需要的行标识列 */
            add_row_identity_var(root, var, rtindex, pstrdup(NameStr(att->attname)));
        }
    }
}

/*
 * tdenginePlanForeignModify  规划对外部表的插入/更新/删除操作
 *    参数:
 *      @root: 规划器信息
 *      @plan: 修改表操作计划
 *      @resultRelation: 结果关系索引
 *      @subplan_index: 子计划索引
 */
static List * tdenginePlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index)
{
    // 获取操作类型(INSERT/UPDATE/DELETE)
    CmdType operation = plan->operation;
    // 获取范围表条目
    RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
    Relation rel;
    StringInfoData sql;      // SQL语句的缓冲区
    List *targetAttrs = NIL; // 目标属性列表
    TupleDesc tupdesc;       // 元组描述符

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    initStringInfo(&sql); // 初始化SQL语句缓冲区

    /*
     * 核心代码已经对每个关系加锁，这里可以使用NoLock
     */
    rel = table_open(rte->relid, NoLock); // 打开关系表
    tupdesc = RelationGetDescr(rel);      // 获取关系表的元组描述符

    // 根据操作类型处理
    if (operation == CMD_INSERT)
    {
        // INSERT操作: 收集所有非删除列
        int attnum;
        for (attnum = 1; attnum <= tupdesc->natts; attnum++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
            if (!attr->attisdropped)
                targetAttrs = lappend_int(targetAttrs, attnum);
        }
    }
    else if (operation == CMD_UPDATE)
        elog(ERROR, "UPDATE is not supported"); // 不支持UPDATE操作
    else if (operation == CMD_DELETE)
    {
        // DELETE操作: 收集时间列和所有标签列
        int i;
        Oid foreignTableId = RelationGetRelid(rel);
        for (i = 0; i < tupdesc->natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            AttrNumber attrno = attr->attnum;
            char *colname = tdengine_get_column_name(foreignTableId, attrno);
            // 只添加时间列和标签列
            if (TDENGINE_IS_TIME_COLUMN(colname) || tdengine_is_tag_key(colname, rte->relid))
                if (!attr->attisdropped)
                    targetAttrs = lappend_int(targetAttrs, attrno);
        }
    }
    else
        elog(ERROR, "Not supported"); // 不支持其他操作类型

    // 检查不支持的特性
    if (plan->returningLists)
        elog(ERROR, "RETURNING is not supported");
    if (plan->onConflictAction != ONCONFLICT_NONE)
        elog(ERROR, "ON CONFLICT is not supported");

    /*
     * 构建SQL语句
     */
    switch (operation)
    {
    case CMD_INSERT:
    case CMD_UPDATE:
        break; // INSERT和UPDATE操作暂不处理SQL构建
    case CMD_DELETE:
        // 构建DELETE语句
        tdengine_deparse_delete(&sql, root, resultRelation, rel, targetAttrs);
        break;
    default:
        elog(ERROR, "unexpected operation: %d", (int)operation);
        break;
    }

    table_close(rel, NoLock); // 关闭关系表

    // 返回SQL语句和目标属性列表
    return list_make2(makeString(sql.data), targetAttrs);
}

/*
 * tdengineBeginForeignModify - 初始化外部表修改操作
 * 参数:
 *   @mtstate: 修改表操作状态
 *   @resultRelInfo: 结果关系信息
 *   @fdw_private: FDW私有数据列表
 *   @subplan_index: 子计划索引
 *   @eflags: 执行标志位
 */
static void tdengineBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo, List *fdw_private, int subplan_index, int eflags)
{
    // 执行状态结构体
    TDengineFdwExecState *fmstate = NULL;
    // 执行状态
    EState *estate = mtstate->ps.state;
    // 目标关系
    Relation rel = resultRelInfo->ri_RelationDesc;
    // 参数数量
    AttrNumber n_params = 0;
    // 类型输出函数OID
    Oid typefnoid = InvalidOid;
    // 用户ID
    Oid userid;
    // 是否为变长类型标志
    bool isvarlena = false;
    // 列表迭代器
    ListCell *lc = NULL;
    // 外部表OID
    Oid foreignTableId = InvalidOid;
    // 子计划
    Plan *subplan;
    int i;
    // 范围表条目
    RangeTblEntry *rte;
    // 外部表信息
    ForeignTable *ftable;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* 如果是EXPLAIN ONLY模式，直接返回 */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    // 获取外部表OID
    foreignTableId = RelationGetRelid(rel);
    // 获取子计划
    subplan = outerPlanState(mtstate)->plan;

    // 初始化执行状态结构体
    fmstate = (TDengineFdwExecState *)palloc0(sizeof(TDengineFdwExecState));
    fmstate->rowidx = 0;

    /* 获取用户身份 */
    rte = exec_rt_fetch(resultRelInfo->ri_RangeTableIndex,
                        mtstate->ps.state);
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

    /* 获取连接选项和用户映射 */
    fmstate->tdengineFdwOptions = tdengine_get_options(foreignTableId, userid);
    ftable = GetForeignTable(foreignTableId);
    fmstate->user = GetUserMapping(userid, ftable->serverid);

    // 设置查询语句和检索属性
    fmstate->rel = rel;
    fmstate->query = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));
    fmstate->retrieved_attrs = (List *)list_nth(fdw_private, FdwModifyPrivateTargetAttnums);

    /* 为INSERT/DELETE操作准备列信息 */
    if (mtstate->operation == CMD_INSERT || mtstate->operation == CMD_DELETE)
    {
        fmstate->column_list = NIL;

        if (fmstate->retrieved_attrs)
        {
            foreach (lc, fmstate->retrieved_attrs)
            {
                int attnum = lfirst_int(lc);
                struct TDengineColumnInfo *col = (TDengineColumnInfo *)palloc0(sizeof(TDengineColumnInfo));

                /* 获取列名并设置列类型 */
                col->column_name = tdengine_get_column_name(foreignTableId, attnum);
                if (TDENGINE_IS_TIME_COLUMN(col->column_name))
                    col->column_type = TDENGINE_TIME_KEY;
                else if (tdengine_is_tag_key(col->column_name, foreignTableId))
                    col->column_type = TDENGINE_TAG_KEY;
                else
                    col->column_type = TDENGINE_FIELD_KEY;

                /* 将列信息添加到列列表中 */
                fmstate->column_list = lappend(fmstate->column_list, col);
            }
        }
        // 设置批量大小选项
        fmstate->batch_size = tdengine_get_batch_size_option(rel);
    }

    /* 计算参数总数(检索属性数+1) */
    n_params = list_length(fmstate->retrieved_attrs) + 1;

    /* 分配并初始化各种参数信息的内存空间 */
    fmstate->p_flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * n_params);                              
    fmstate->p_nums = 0;                                                                               
    fmstate->param_flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * n_params);                          
    fmstate->param_types = (Oid *)palloc0(sizeof(Oid) * n_params);                                     
    fmstate->param_tdengine_types = (TDengineType *)palloc0(sizeof(TDengineType) * n_params);          
    fmstate->param_tdengine_values = (TDengineValue *)palloc0(sizeof(TDengineValue) * n_params);       
    fmstate->param_column_info = (TDengineColumnInfo *)palloc0(sizeof(TDengineColumnInfo) * n_params); 

    /* 创建临时内存上下文用于每行数据处理 */
    fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                              "tdengine_fdw temporary data",
                                              ALLOCSET_SMALL_SIZES);

    /* 设置可传输参数 */
    foreach (lc, fmstate->retrieved_attrs)
    {
        int attnum = lfirst_int(lc);                                               // 获取属性编号
        Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1); // 获取属性信息

        Assert(!attr->attisdropped); // 确保属性未被删除

        /* 获取类型输出函数信息并初始化 */
        getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
        fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
        fmstate->p_nums++; // 递增参数计数器
    }
    Assert(fmstate->p_nums <= n_params); // 验证参数数量不超过总数

    /* 分配并初始化junk属性索引数组 */
    fmstate->junk_idx = palloc0(RelationGetDescr(rel)->natts * sizeof(AttrNumber));

    /* 遍历表的所有列 */
    for (i = 0; i < RelationGetDescr(rel)->natts; i++)
    {

        fmstate->junk_idx[i] =
            ExecFindJunkAttributeInTlist(subplan->targetlist,get_attname(foreignTableId, i + 1,false));
    }

    fmstate->aux_fmstate = NULL;

    resultRelInfo->ri_FdwState = fmstate;
}

/*
 * tdengineExecForeignInsert - 执行外部表单行插入操作
 * 参数:
 *   @estate: 执行状态
 *   @resultRelInfo: 结果关系信息
 *   @slot: 包含待插入数据的元组槽
 *   @planSlot: 计划元组槽
 */
static TupleTableSlot * tdengineExecForeignInsert(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    // 获取执行状态
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;
    // 用于存储返回的元组槽
    TupleTableSlot **rslot;
    // 插入的元组数量(单行插入固定为1)
    int numSlots = 1;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /*
     * 如果存在辅助执行状态(aux_fmstate)，则临时替换当前执行状态
     * 用于处理特殊插入场景
     */
    if (fmstate->aux_fmstate)
        resultRelInfo->ri_FdwState = fmstate->aux_fmstate;

    // 执行实际的插入操作
    rslot = execute_foreign_insert_modify(estate, resultRelInfo, &slot, &planSlot, numSlots)
            /* 恢复原始执行状态 */
            if (fmstate->aux_fmstate)
                resultRelInfo->ri_FdwState = fmstate;

    return rslot ? *rslot : NULL;
}

/*
 * tdengineExecForeignBatchInsert - 批量插入多行数据到外部表
 *   @estate: 执行状态，包含查询执行环境信息
 *   @resultRelInfo: 结果关系信息，描述目标表的结构和状态
 *   @slots: 元组槽数组，包含待插入的多行数据
 *   @planSlots: 计划元组槽数组(当前未使用，保留参数)
 *   @numSlots: 指向插入行数的指针，表示要处理的元组数量
 */
static TupleTableSlot **
tdengineExecForeignBatchInsert(EState *estate,
                               ResultRelInfo *resultRelInfo,
                               TupleTableSlot **slots,
                               TupleTableSlot **planSlots,
                               int *numSlots)
{
    // 获取执行状态
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;
    // 用于存储返回结果
    TupleTableSlot **rslot;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    if (fmstate->aux_fmstate)
        resultRelInfo->ri_FdwState = fmstate->aux_fmstate;

    rslot = execute_foreign_insert_modify(estate, resultRelInfo, slots,
                                          planSlots, *numSlots);

    if (fmstate->aux_fmstate)
        resultRelInfo->ri_FdwState = fmstate;

    return rslot;
}

static int tdengineGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo)
{
    int batch_size;
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* 验证函数是否首次调用 */
    Assert(resultRelInfo->ri_BatchSize == 0);

    /*
     * 检查是否在UPDATE操作的目标关系上执行插入
     */
    Assert(fmstate == NULL || fmstate->aux_fmstate == NULL);

    /*
     * 确定批量大小
     */
    if (fmstate)
        batch_size = fmstate->batch_size;
    else
        batch_size = tdengine_get_batch_size_option(resultRelInfo->ri_RelationDesc);

    /*
     * 检查禁用批量操作的条件:
     */
    if (resultRelInfo->ri_projectReturning != NULL ||
        resultRelInfo->ri_WithCheckOptions != NIL ||
        (resultRelInfo->ri_TrigDesc &&
         (resultRelInfo->ri_TrigDesc->trig_insert_before_row ||
          resultRelInfo->ri_TrigDesc->trig_insert_after_row)))
        return 1; // 满足任一条件则禁用批量

    /*
     * 确保批量参数不超过65535限制
     * 计算最大可能的批量大小
     */
    if (fmstate && fmstate->p_nums > 0)
        batch_size = Min(batch_size, 65535 / fmstate->p_nums);

    return batch_size;
}

static void bindJunkColumnValue(TDengineFdwExecState *fmstate,
                                TupleTableSlot *slot,
                                TupleTableSlot *planSlot,
                                Oid foreignTableId,
                                int bindnum)
{
    int i;       // 列索引计数器
    Datum value; // 列值

    /* 遍历元组槽的所有属性 */
    for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
    {
        // 获取当前列类型
        Oid type = TupleDescAttr(slot->tts_tupleDescriptor, i)->atttypid;
        bool is_null = false; // NULL值标志

        /* 检查是否为有效的junk列 */
        if (fmstate->junk_idx[i] == InvalidAttrNumber)
            continue; // 非junk列则跳过

        /* 从planSlot获取junk列值 */
        value = ExecGetJunkAttribute(planSlot, fmstate->junk_idx[i], &is_null);

        /* 处理NULL值 */
        if (is_null)
        {
            // 设置NULL类型标记
            fmstate->param_tdengine_types[bindnum] = TDENGINE_NULL;
            // 设置NULL值(0)
            fmstate->param_tdengine_values[bindnum].i = 0;
        }
        else
        {
            /* 获取列信息并绑定到TDengine参数 */
            struct TDengineColumnInfo *col = list_nth(fmstate->column_list, (int)bindnum);
            // 设置列类型
            fmstate->param_column_info[bindnum].column_type = col->column_type;
            // 绑定SQL变量到TDengine参数
            tdengine_bind_sql_var(type, bindnum, value, fmstate->param_column_info,
                                  fmstate->param_tdengine_types, fmstate->param_tdengine_values);
        }
        bindnum++; // 递增绑定位置
    }
}

static TupleTableSlot *tdengineExecForeignDelete(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    // 获取执行状态
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;
    // 获取表关系
    Relation rel = resultRelInfo->ri_RelationDesc;
    // 获取外部表OID
    Oid foreignTableId = RelationGetRelid(rel);
    // 存储查询返回结果(volatile防止优化)
    struct TDengineQuery_return volatile ret;

    // 记录调试日志
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    // 绑定junk列值(用于标识要删除的行)
    bindJunkColumnValue(fmstate, slot, planSlot, foreignTableId, 0);

    /* 执行查询 */

    ret = TDengineQuery(fmstate->query, fmstate->user, fmstate->tdengineFdwOptions, fmstate->param_tdengine_types, fmstate->param_tdengine_values, fmstate->p_nums);

    // 错误处理
    if (ret.r1 != NULL)
    {
        // 复制错误信息
        char *err = pstrdup(ret.r1);
        // 释放原始错误信息
        free(ret.r1);
        ret.r1 = err;
        elog(ERROR, "tdengine_fdw : %s", err);
    }

    // 释放查询结果
    TDengineFreeResult((TDengineResult *)&ret.r0);

    /* 返回元组槽 */
    return slot;
}

/*
 * tdengineEndForeignModify - 结束对外部表的修改操作
 * 参数:
 *   @estate: 执行状态，包含查询执行环境信息
 *   @resultRelInfo: 结果关系信息，描述目标表的结构和状态
 */
static void tdengineEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo)
{
    // 获取执行状态
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    // 检查并重置执行状态
    if (fmstate != NULL)
    {
        fmstate->cursor_exists = false; // 重置游标状态
        fmstate->rowidx = 0;            // 重置行索引
    }
}
/*
 * tdengineBeginDirectModify - 准备直接修改外部表
 * 功能: 初始化直接修改外部表所需的执行状态和参数
 *
 * 参数:
 *   @node: ForeignScanState节点，包含执行计划信息
 *   @eflags: 执行标志位，用于控制执行行为
 */
static void tdengineBeginDirectModify(ForeignScanState *node, int eflags)
{
    // 获取执行计划节点
    ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
    // 获取执行状态
    EState *estate = node->ss.ps.state;
    // 直接修改状态结构
    TDengineFdwDirectModifyState *dmstate;
    // 范围表索引
    Index rtindex;
    // 用户ID
    Oid userid;
    // 范围表条目
    RangeTblEntry *rte;
    // 参数数量
    int numParams;
    ForeignTable *ftable;
    List *remote_exprs;

    // 记录调试日志
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* EXPLAIN ONLY模式直接返回 */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* 分配并初始化直接修改状态结构 */
    dmstate = (TDengineFdwDirectModifyState *)palloc0(sizeof(TDengineFdwDirectModifyState));
    node->fdw_state = (void *)dmstate;

    /* 确定远程访问用户身份 */
    userid = GetUserId();
    rtindex = node->resultRelInfo->ri_RangeTableIndex;

    /* 获取范围表条目和关系描述符 */
    rte = exec_rt_fetch(rtindex, estate);
    if (fsplan->scan.scanrelid == 0)
        dmstate->rel = ExecOpenScanRelation(estate, rtindex, eflags);
    else
        dmstate->rel = node->ss.ss_currentRelation;

    /* 获取连接选项和用户映射 */
    dmstate->tdengineFdwOptions = tdengine_get_options(rte->relid, userid);

    ftable = GetForeignTable(RelationGetRelid(dmstate->rel));
    dmstate->user = GetUserMapping(userid, ftable->serverid);

    /* 处理外连接相关字段 */
    if (fsplan->scan.scanrelid == 0)
    {
        /* 保存外部表信息 */
        dmstate->resultRel = dmstate->rel;


        dmstate->rel = NULL;
    }

    /* 初始化状态变量 */
    dmstate->num_tuples = -1;

    /* 从计划节点获取私有信息 */
    dmstate->query = strVal(list_nth(fsplan->fdw_private, FdwDirectModifyPrivateUpdateSql));
    dmstate->has_returning = boolVal(list_nth(fsplan->fdw_private, FdwDirectModifyPrivateHasReturning));
    dmstate->retrieved_attrs = (List *)list_nth(fsplan->fdw_private, FdwDirectModifyPrivateRetrievedAttrs);
    dmstate->set_processed = boolVal(list_nth(fsplan->fdw_private, FdwDirectModifyPrivateSetProcessed));

    // 从计划节点获取远程表达式列表
    remote_exprs = (List *)list_nth(fsplan->fdw_private,FdwDirectModifyRemoteExprs);

    /*
     * 准备远程查询参数处理
     */
    numParams = list_length(fsplan->fdw_exprs);
    dmstate->numParams = numParams;

    /* 如果有参数需要处理 */
    if (numParams > 0)
        prepare_query_params((PlanState *)node, fsplan->fdw_exprs, remote_exprs, rte->relid, numParams, &dmstate->param_flinfo, &dmstate->param_exprs, &dmstate->param_values, &dmstate->param_types, &dmstate->param_tdengine_types, &dmstate->param_tdengine_values, &dmstate->param_column_info);
}

/*
 * tdengineIterateDirectModify - 执行直接外部表修改操作
 * 功能: 处理直接修改外部表的迭代操作，包括执行DML语句和更新统计信息
 *
 * 参数:
 *   @node: ForeignScanState节点，包含执行状态和计划信息
 */
static TupleTableSlot *tdengineIterateDirectModify(ForeignScanState *node)
{
    // 获取直接修改状态
    TDengineFdwDirectModifyState *dmstate = (TDengineFdwDirectModifyState *)node->fdw_state;
    // 获取执行状态
    EState *estate = node->ss.ps.state;
    // 获取扫描元组槽
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    // 获取性能统计信息
    Instrumentation *instr = node->ss.ps.instrument;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* 首次调用时执行DML语句 */
    if (dmstate->num_tuples == -1)
        execute_dml_stmt(node);

    Assert(!dmstate->has_returning);

    /* 更新命令处理计数 */
    if (dmstate->set_processed)
        estate->es_processed += dmstate->num_tuples;

    if (instr)
        instr->tuplecount += dmstate->num_tuples;

    // 返回清空的元组槽
    return ExecClearTuple(slot);
}

int tdengine_set_transmission_modes(void)
{
    int nestlevel = NewGUCNestLevel();

    /* 设置日期格式为ISO标准 */
    if (DateStyle != USE_ISO_DATES)
        (void)set_config_option("datestyle", "ISO", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

    /* 设置间隔样式为Postgres风格 */
    if (IntervalStyle != INTSTYLE_POSTGRES)
        (void)set_config_option("intervalstyle", "postgres", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

    /* 设置浮点精度至少为3位 */
    if (extra_float_digits < 3)
        (void)set_config_option("extra_float_digits", "3", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

    /* 强制设置搜索路径为pg_catalog */
    (void)set_config_option("search_path", "pg_catalog", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

    return nestlevel;
}

void tdengine_reset_transmission_modes(int nestlevel)
{
    AtEOXact_GUC(true, nestlevel);
}

static void prepare_query_params(PlanState *node, List *fdw_exprs, List *remote_exprs, Oid foreigntableid, int numParams, FmgrInfo **param_flinfo, List **param_exprs, const char ***param_values, Oid **param_types, TDengineType **param_tdengine_types, TDengineValue **param_tdengine_values, TDengineColumnInfo **param_column_info)
{
    int i;
    ListCell *lc;

    /* 验证参数数量必须大于0 */
    Assert(numParams > 0);

    /* 分配各种参数信息的内存空间 */
    *param_flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * numParams);
    *param_types = (Oid *)palloc0(sizeof(Oid) * numParams);
    *param_tdengine_types = (TDengineType *)palloc0(sizeof(TDengineType) * numParams);
    *param_tdengine_values = (TDengineValue *)palloc0(sizeof(TDengineValue) * numParams);
    *param_column_info = (TDengineColumnInfo *)palloc0(sizeof(TDengineColumnInfo) * numParams);

    i = 0;
    foreach (lc, fdw_exprs)
    {
        Node *param_expr = (Node *)lfirst(lc);
        Oid typefnoid;
        bool isvarlena;

        /* 获取参数表达式类型 */
        (*param_types)[i] = exprType(param_expr);
        /* 获取类型输出函数信息 */
        getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
        fmgr_info(typefnoid, &(*param_flinfo)[i]);

        /* 如果是时间类型参数 */
        if (TDENGINE_IS_TIME_TYPE((*param_types)[i]))
        {
            ListCell *expr_cell;

            foreach (expr_cell, remote_exprs)
            {
                Node *qual = (Node *)lfirst(expr_cell);

                if (tdengine_param_belong_to_qual(qual, param_expr))
                {
                    Var *col;
                    char *column_name;
                    List *column_list = pull_var_clause(qual, PVC_RECURSE_PLACEHOLDERS);

                    /* 提取相关列信息 */
                    col = linitial(column_list);
                    column_name = tdengine_get_column_name(foreigntableid, col->varattno);

                    if (TDENGINE_IS_TIME_COLUMN(column_name))
                        (*param_column_info)[i].column_type = TDENGINE_TIME_KEY;
                    else if (tdengine_is_tag_key(column_name, foreigntableid))
                        (*param_column_info)[i].column_type = TDENGINE_TAG_KEY;
                    else
                        (*param_column_info)[i].column_type = TDENGINE_FIELD_KEY;
                }
            }
        }
        i++;
    }

    /* 初始化参数表达式列表 */
    *param_exprs = (List *)ExecInitExprList(fdw_exprs, node);
    /* 分配参数值缓冲区 */
    *param_values = (const char **)palloc0(numParams * sizeof(char *));
}

/*
 * 检查参数是否属于条件表达式
 *
 * 参数:
 *   @qual: 条件表达式树节点
 *   @param: 要检查的参数节点
 */
static bool tdengine_param_belong_to_qual(Node *qual, Node *param)
{
    /* 空条件直接返回false */
    if (qual == NULL)
        return false;

    /* 当前节点与参数匹配则返回true */
    if (equal(qual, param))
        return true;

    /* 递归检查表达式树的所有子节点 */
    return expression_tree_walker(qual, tdengine_param_belong_to_qual, param);
}

static void process_query_params(ExprContext *econtext, FmgrInfo *param_flinfo, List *param_exprs, const char **param_values, Oid *param_types, TDengineType *param_tdengine_types, TDengineValue *param_tdengine_values, TDengineColumnInfo *param_column_info)
{
    int nestlevel;
    int i;
    ListCell *lc;

    nestlevel = tdengine_set_transmission_modes();

    i = 0;
    foreach (lc, param_exprs)
    {
        ExprState *expr_state = (ExprState *)lfirst(lc);
        Datum expr_value;
        bool isNull;

        expr_value = ExecEvalExpr(expr_state, econtext, &isNull);

        if (isNull)
        {
            elog(ERROR, "tdengine_fdw : cannot bind NULL due to TDengine does not support to filter NULL value");
        }
        else
        {
            /* Bind parameters */
            tdengine_bind_sql_var(param_types[i], i, expr_value, param_column_info, param_tdengine_types, param_tdengine_values);
            param_values[i] = OutputFunctionCall(&param_flinfo[i], expr_value);
        }
        i++;
    }
    tdengine_reset_transmission_modes(nestlevel);
}

static void create_cursor(ForeignScanState *node)
{
    // 获取执行状态
    TDengineFdwExecState *festate = (TDengineFdwExecState *)node->fdw_state;
    // 获取表达式上下文
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    // 获取参数数量
    int numParams = festate->numParams;
    // 获取参数值数组
    const char **values = festate->param_values;

    /* 如果有查询参数需要处理 */
    if (numParams > 0)
    {
        MemoryContext oldcontext;

        /* 切换到每元组内存上下文 */
        oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
        // 分配参数存储空间
        festate->params = palloc(numParams);
        // 处理查询参数(类型转换和绑定)
        process_query_params(econtext, festate->param_flinfo, festate->param_exprs, values, festate->param_types, festate->param_tdengine_types, festate->param_tdengine_values, festate->param_column_info);

        /* 切换回原始内存上下文 */
        MemoryContextSwitchTo(oldcontext);
    }

    festate->cursor_exists = true;
}

/*
 * execute_dml_stmt - 执行直接UPDATE/DELETE语句
 *
 * 参数:
 *   @node: ForeignScanState节点，包含执行状态和计划信息
 */
static void
execute_dml_stmt(ForeignScanState *node)
{
    // 获取直接修改状态
    TDengineFdwDirectModifyState *dmstate = (TDengineFdwDirectModifyState *)node->fdw_state;
    // 获取表达式上下文
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    // 获取参数数量
    int numParams = dmstate->numParams;
    // 获取参数值数组
    const char **values = dmstate->param_values;
    // 存储查询返回结果(volatile防止优化)
    struct TDengineQuery_return volatile ret;

    /* 处理查询参数 */
    if (numParams > 0)
    {
        MemoryContext oldcontext;

        // 切换到每元组内存上下文
        oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
        // 分配参数存储空间
        dmstate->params = palloc(numParams);
        // 处理查询参数(类型转换和绑定)
        process_query_params(econtext, dmstate->param_flinfo, dmstate->param_exprs, values, dmstate->param_types, dmstate->param_tdengine_types, dmstate->param_tdengine_values, dmstate->param_column_info);

        // 切换回原始内存上下文
        MemoryContextSwitchTo(oldcontext);
    }

    /* 执行查询 */

    ret = TDengineQuery(dmstate->query, dmstate->user, dmstate->tdengineFdwOptions, dmstate->param_tdengine_types, dmstate->param_tdengine_values, dmstate->numParams);

    // 错误处理
    if (ret.r1 != NULL)
    {
        // 复制错误信息
        char *err = pstrdup(ret.r1);
        // 释放原始错误信息
        free(ret.r1);
        ret.r1 = err;
        elog(ERROR, "tdengine_fdw : %s", err);
    }

    // 释放查询结果
    TDengineFreeResult((TDengineResult *)&ret.r0);

    dmstate->num_tuples = 0;
}

static TupleTableSlot **execute_foreign_insert_modify(EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot **slots, TupleTableSlot **planSlots, int numSlots)
{
    // 获取执行状态
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;
    uint32_t bindnum = 0;
    char *ret;
    int i;
    // 获取表信息和描述符
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    char *tablename = tdengine_get_table_name(rel);
    bool time_had_value = false;
    int bind_num_time_column = 0;
    MemoryContext oldcontext;

    // 切换到临时内存上下文处理参数
    oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

    // 重新分配参数存储空间以适应批量操作
    fmstate->param_tdengine_types = (TDengineType *)repalloc(fmstate->param_tdengine_types, sizeof(TDengineType) * fmstate->p_nums * numSlots);
    fmstate->param_tdengine_values = (TDengineValue *)repalloc(fmstate->param_tdengine_values, sizeof(TDengineValue) * fmstate->p_nums * numSlots);
    fmstate->param_column_info = (TDengineColumnInfo *)repalloc(fmstate->param_column_info, sizeof(TDengineColumnInfo) * fmstate->p_nums * numSlots);

    /* 从元组槽获取参数并绑定 */
    if (slots != NULL && fmstate->retrieved_attrs != NIL)
    {
        int nestlevel;
        ListCell *lc;

        // 设置数据传输模式
        nestlevel = tdengine_set_transmission_modes();

        // 遍历每个元组槽
        for (i = 0; i < numSlots; i++)
        {
            /* 绑定值到参数 */
            foreach (lc, fmstate->retrieved_attrs)
            {
                int attnum = lfirst_int(lc) - 1;
                Oid type = TupleDescAttr(slots[i]->tts_tupleDescriptor, attnum)->atttypid;
                Datum value;
                bool is_null;
                // 获取列信息
                struct TDengineColumnInfo *col = list_nth(fmstate->column_list, (int)bindnum % fmstate->p_nums);

                // 设置列名和类型
                fmstate->param_column_info[bindnum].column_name = col->column_name;
                fmstate->param_column_info[bindnum].column_type = col->column_type;
                // 获取属性值
                value = slot_getattr(slots[i], attnum + 1, &is_null);

                /* 检查值是否为空 */
                if (is_null)
                {
                    // 检查非空约束
                    if (TupleDescAttr(tupdesc, attnum)->attnotnull)
                        elog(ERROR, "tdengine_fdw : null value in column \"%s\" of relation \"%s\" violates not-null constraint", col->column_name, tablename);
                    // 设置空值标记
                    fmstate->param_tdengine_types[bindnum] = TDENGINE_NULL;
                    fmstate->param_tdengine_values[bindnum].i = 0;
                }
                else
                {
                    if (TDENGINE_IS_TIME_COLUMN(col->column_name))
                    {
                        if (!time_had_value)
                        {
                            tdengine_bind_sql_var(type, bindnum, value, fmstate->param_column_info,
                                                  fmstate->param_tdengine_types, fmstate->param_tdengine_values);
                            bind_num_time_column = bindnum;
                            time_had_value = true;
                        }
                        else
                        {
                            elog(WARNING, "Inserting value has both \'time_text\' and \'time\' columns specified. The \'time\' will be ignored.");
                            if (strcmp(col->column_name, TDENGINE_TIME_TEXT_COLUMN) == 0)
                            {
                                tdengine_bind_sql_var(type, bind_num_time_column, value, fmstate->param_column_info, fmstate->param_tdengine_types, fmstate->param_tdengine_values);
                            }
                            // 忽略重复时间列
                            fmstate->param_tdengine_types[bindnum] = TDENGINE_NULL;
                            fmstate->param_tdengine_values[bindnum].i = 0;
                        }
                    }
                    else
                    {
                        // 绑定普通列值
                        tdengine_bind_sql_var(type, bindnum, value, fmstate->param_column_info, fmstate->param_tdengine_types, fmstate->param_tdengine_values);
                    }
                }
                bindnum++; // 递增绑定计数器
            }
        }
        tdengine_reset_transmission_modes(nestlevel);
    }

    Assert(bindnum == fmstate->p_nums * numSlots);

    ret = TDengineInsert(tablename, fmstate->user, fmstate->tdengineFdwOptions,
                         fmstate->param_column_info, fmstate->param_tdengine_types, fmstate->param_tdengine_values, fmstate->p_nums, numSlots);
    if (ret != NULL)
        elog(ERROR, "tdengine_fdw : %s", ret);

    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(fmstate->temp_cxt);

    return slots;
}

static int tdengine_get_batch_size_option(Relation rel)
{
    Oid foreigntableid = RelationGetRelid(rel);
    List *options = NIL;
    ListCell *lc;
    ForeignTable *table;
    ForeignServer *server;

    /* 默认批量大小为1(不启用批量操作) */
    int batch_size = 1;

    /*
     * 加载表和服务器选项:
     * 表选项优先于服务器选项
     */
    table = GetForeignTable(foreigntableid);
    server = GetForeignServer(table->serverid);

    options = list_concat(options, table->options);
    options = list_concat(options, server->options);

    foreach (lc, options)
    {
        DefElem *def = (DefElem *)lfirst(lc);

        if (strcmp(def->defname, "batch_size") == 0)
        {
            (void)parse_int(defGetString(def), &batch_size, 0, NULL);
            break;
        }
    }

    return batch_size;
}
