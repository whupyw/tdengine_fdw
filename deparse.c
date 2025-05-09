#include "postgres.h"
#include "tdengine_fdw.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/tlist.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#define QUOTE '"'

// TODO: TDengine支持的函数列表

static const char *TDengineStableStarFunction[] = {
	"tdengine_count_all",
	"tdengine_mode_all",
	"tdengine_max_all",
	"tdengine_min_all",
	"tdengine_sum_all",
	NULL};

static const char *TDengineUniqueFunction[] = {
	"bottom",
	"percentile",
	"top",
	"cumulative_sum",
	"derivative",
	"difference",
	"elapsed",
	"log2",
	"log10", /* Use for PostgreSQL old version */

	"tdengine_time",
	"tdengine_fill_numeric",
	"tdengine_fill_option",
	NULL};

static const char *TDengineSupportedBuiltinFunction[] = {
	"now",
	"sqrt",
	"abs",
	"acos",
	"asin",
	"atan",
	"atan2",
	"ceil",
	"cos",
	"exp",
	"floor",
	"ln",
	"log",
	"log10",
	"pow",
	"round",
	"sin",
	"tan",
	NULL};

/*
 * foreign_glob_cxt: 表达式树遍历的全局上下文结构
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;
	RelOptInfo *foreignrel;
	Relids relids;
	Oid relid;
	unsigned int mixing_aggref_status;
	bool for_tlist;
	bool is_inner_func;
} foreign_glob_cxt;

/*
 * FDWCollateState: 排序规则状态枚举
 */
typedef enum
{
	FDW_COLLATE_NONE,
	FDW_COLLATE_SAFE,
	FDW_COLLATE_UNSAFE,
} FDWCollateState;

/*
 * foreign_loc_cxt: 表达式树遍历的局部上下文结构
 */
typedef struct foreign_loc_cxt
{
	Oid collation;
	FDWCollateState state;
	bool can_skip_cast;
	bool can_pushdown_stable;
	bool can_pushdown_volatile;
	bool tdengine_fill_enable;
	bool have_otherfunc_tdengine_time_tlist;
	bool has_time_key;
	bool has_sub_or_add_operator;
	bool is_comparison;
} foreign_loc_cxt;

/*
 * PatternMatchingOperator - 模式匹配操作符类型枚举
 */
typedef enum
{
	UNKNOWN_OPERATOR = 0,
	LIKE_OPERATOR,
	NOT_LIKE_OPERATOR,
	ILIKE_OPERATOR,
	NOT_ILIKE_OPERATOR,
	REGEX_MATCH_CASE_SENSITIVE_OPERATOR,
	REGEX_NOT_MATCH_CASE_SENSITIVE_OPERATOR,
	REGEX_MATCH_CASE_INSENSITIVE_OPERATOR,
	REGEX_NOT_MATCH_CASE_INSENSITIVE_OPERATOR
} PatternMatchingOperator;

/*
 * deparse_expr_cxt - 表达式反解析上下文结构体ith time zone)，
 *     是否需要转换为不带时区的时间戳(timestamp without time zone)
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;
	RelOptInfo *foreignrel;
	RelOptInfo *scanrel;

	StringInfo buf;
	List **params_list;
	PatternMatchingOperator op_type;
	bool is_tlist;
	bool can_skip_cast;
	bool can_delete_directly;

	bool has_bool_cmp;
	FuncExpr *tdengine_fill_expr;

	bool convert_to_timestamp;
} deparse_expr_cxt;

typedef struct pull_func_clause_context
{
	List *funclist;
} pull_func_clause_context;

static void tdengine_deparse_expr(Expr *node, deparse_expr_cxt *context);
static void tdengine_deparse_var(Var *node, deparse_expr_cxt *context);
static void tdengine_deparse_const(Const *node, deparse_expr_cxt *context, int showtype);
static void tdengine_deparse_param(Param *node, deparse_expr_cxt *context);
static void tdengine_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context);
static void tdengine_deparse_fill_option(StringInfo buf, const char *val);
static void tdengine_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context);
static void tdengine_deparse_operator_name(StringInfo buf, Form_pg_operator opform, PatternMatchingOperator *op_type);
static void tdengine_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node,
												  deparse_expr_cxt *context);
static void tdengine_deparse_relabel_type(RelabelType *node, deparse_expr_cxt *context);
static void tdengine_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context);
static void tdengine_deparse_null_test(NullTest *node, deparse_expr_cxt *context);
static void tdengine_deparse_array_expr(ArrayExpr *node, deparse_expr_cxt *context);
static void tdengine_deparse_coerce_via_io(CoerceViaIO *cio, deparse_expr_cxt *context);

static void tdengine_print_remote_param(int paramindex, Oid paramtype, int32 paramtypmod, deparse_expr_cxt *context);
static void tdengine_print_remote_placeholder(Oid paramtype, int32 paramtypmod, deparse_expr_cxt *context);

static void tdengine_deparse_relation(StringInfo buf, Relation rel);
static void tdengine_deparse_target_list(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, Bitmapset *attrs_used, List **retrieved_attrs);
static void tdengine_deparse_target_list_schemaless(StringInfo buf, Relation rel, Oid reloid, Bitmapset *attrs_used, List **retrieved_attrs, bool all_fieldtag, List *slcols);
static void tdengine_deparse_slvar(Node *node, Var *var, Const *cnst, deparse_expr_cxt *context);
static void tdengine_deparse_column_ref(StringInfo buf, int varno, int varattno, Oid vartype, PlannerInfo *root, bool convert, bool *can_delete_directly);

static void tdengine_deparse_select(List *tlist, List **retrieved_attrs, deparse_expr_cxt *context);
static void tdengine_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel, bool use_alias, List **params_list);
static void tdengine_deparse_from_expr(List *quals, deparse_expr_cxt *context);
static void tdengine_deparse_aggref(Aggref *node, deparse_expr_cxt *context);
static void tdengine_append_conditions(List *exprs, deparse_expr_cxt *context);
static void tdengine_append_group_by_clause(List *tlist, deparse_expr_cxt *context);
static void tdengine_append_order_by_clause(List *pathkeys, deparse_expr_cxt *context);
static Node *tdengine_deparse_sort_group_clause(Index ref, List *tlist, deparse_expr_cxt *context);

static void tdengine_deparse_explicit_target_list(List *tlist, List **retrieved_attrs, deparse_expr_cxt *context);
static Expr *tdengine_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);

static bool tdengine_contain_time_column(List *exprs, schemaless_info *pslinfo);
static bool tdengine_contain_time_key_column(Oid relid, List *exprs);
static bool tdengine_contain_time_expr(List *exprs);
static bool tdengine_contain_time_function(List *exprs);
static bool tdengine_contain_time_param(List *exprs);
static bool tdengine_contain_time_const(List *exprs);

static void tdengine_append_field_key(TupleDesc tupdesc, StringInfo buf, Index rtindex, PlannerInfo *root, bool first);
static void tdengine_append_limit_clause(deparse_expr_cxt *context);
static bool tdengine_is_string_type(Node *node, schemaless_info *pslinfo);
static char *tdengine_quote_identifier(const char *s, char q);
static bool tdengine_contain_functions_walker(Node *node, void *context);

bool tdengine_is_grouping_target(TargetEntry *tle, Query *query);
bool tdengine_is_builtin(Oid objectId);
bool tdengine_is_regex_argument(Const *node, char **extval);
char *tdengine_replace_function(char *in);
bool tdengine_is_star_func(Oid funcid, char *in);
static bool tdengine_is_unique_func(Oid funcid, char *in);
static bool tdengine_is_supported_builtin_func(Oid funcid, char *in);
static bool exist_in_function_list(char *funcname, const char **funclist);

static void add_backslash(StringInfo buf, const char *ptr, const char *regex_special);
static bool tdengine_last_percent_sign_check(const char *val);
static void tdengine_deparse_string_like_pattern(StringInfo buf, const char *val, PatternMatchingOperator op_type);
static void tdengine_deparse_string_regex_pattern(StringInfo buf, const char *val, PatternMatchingOperator op_type);

static char *cur_opname = NULL;

/*
 * 反解析关系名称到SQL语句
 */
static void tdengine_deparse_relation(StringInfo buf, Relation rel)
{
	/* 获取表名(考虑FDW选项) */
	char *relname = tdengine_get_table_name(rel);

	/* 添加带引号的表名到输出缓冲区 */
	appendStringInfo(buf, "%s", tdengine_quote_identifier(relname, QUOTE));
}

/*
 * tdengine_quote_identifier - 为标识符添加引号
 */
static char *tdengine_quote_identifier(const char *s, char q)
{
	/* 分配足够内存: 原始长度*2(考虑转义) + 3(两个引号和结束符) */
	char *result = palloc(strlen(s) * 2 + 3);
	char *r = result;

	/* 添加起始引号 */
	*r++ = q;

	/* 处理字符串内容 */
	while (*s)
	{
		/* 转义引号字符 */
		if (*s == q)
			*r++ = *s;
		/* 复制字符 */
		*r++ = *s;
		s++;
	}

	/* 添加结束引号和终止符 */
	*r++ = q;
	*r++ = '\0';

	return result;
}

/*
 * pull_func_clause_walker - 递归遍历语法树并收集函数表达式节点
 */
static bool tdengine_pull_func_clause_walker(Node *node, pull_func_clause_context *context)
{
	/* 空节点直接返回false，不继续遍历 */
	if (node == NULL)
		return false;

	/* 检查节点是否为函数表达式类型 */
	if (IsA(node, FuncExpr))
	{
		/* 将函数表达式节点添加到上下文的链表中 */
		context->funclist = lappend(context->funclist, node);
		/* 返回false表示不需要继续遍历当前节点的子节点 */
		return false;
	}

	/* 使用PostgreSQL的expression_tree_walker递归遍历所有子节点 */
	return expression_tree_walker(node, tdengine_pull_func_clause_walker,
								  (void *)context);
}

/*
 * pull_func_clause - 从语法树中提取所有函数表达
 */
List *
tdengine_pull_func_clause(Node *node)
{
	/* 初始化上下文结构 */
	pull_func_clause_context context;
	/* 初始化空链表 */
	context.funclist = NIL;

	/* 调用walker函数开始遍历语法树 */
	tdengine_pull_func_clause_walker(node, &context);

	/* 返回收集到的函数表达式链表 */
	return context.funclist;
}

/*
 * 判断给定表达式是否可以在外部服务器上安全执行
 */
bool tdengine_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr, bool for_tlist)
{
	foreign_glob_cxt glob_cxt; // 全局上下文
	foreign_loc_cxt loc_cxt;   // 局部上下文
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)(baserel->fdw_private);

	/*
	 * 初始化全局上下文
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	glob_cxt.relid = fpinfo->table->relid;
	glob_cxt.mixing_aggref_status = TDENGINE_TARGETS_MIXING_AGGREF_SAFE;
	glob_cxt.for_tlist = for_tlist;
	glob_cxt.is_inner_func = false;

	/*
	 * 设置关系ID集合(relids)
	 */
	if (baserel->reloptkind == RELOPT_UPPER_REL)
		glob_cxt.relids = fpinfo->outerrel->relids;
	else
		glob_cxt.relids = baserel->relids;

	/*
	 * 初始化局部上下文
	 */
	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	loc_cxt.can_skip_cast = false;
	loc_cxt.tdengine_fill_enable = false;
	loc_cxt.has_time_key = false;
	loc_cxt.has_sub_or_add_operator = false;
	loc_cxt.is_comparison = false;

	/* 递归遍历表达式树进行检查 */
	if (!tdengine_foreign_expr_walker((Node *)expr, &glob_cxt, &loc_cxt))
		return false;

	/*
	 * 检查排序规则
	 */
	if (loc_cxt.state == FDW_COLLATE_UNSAFE)
		return false;

	/* 表达式可以安全地在远程服务器上执行 */
	return true;
}

/*
 * is_valid_type: 检查给定的OID是否为TDengine支持的有效数据类型
 */
static bool is_valid_type(Oid type)
{
	switch (type)
	{
	case INT2OID:
	case INT4OID:
	case INT8OID:
	case OIDOID:
	case FLOAT4OID:
	case FLOAT8OID:
	case NUMERICOID:
	case VARCHAROID:
	case TEXTOID:
	case TIMEOID:
	case TIMESTAMPOID:
	case TIMESTAMPTZOID:
		return true;
	}
	return false;
}

/*
 * tdengine_foreign_expr_walker: 递归检查表达式节点是否可下推到TDengine执行
 */
static bool tdengine_foreign_expr_walker(Node *node, foreign_glob_cxt *glob_cxt, foreign_loc_cxt *outer_cxt)
{
	bool check_type = true;
	foreign_loc_cxt inner_cxt;
	Oid collation;
	FDWCollateState state;
	HeapTuple tuple;
	Form_pg_operator form;
	char *cur_opname;
	static bool is_time_column = false;

	/* 获取FDW关系信息 */
	TDengineFdwRelationInfo *fpinfo =
		(TDengineFdwRelationInfo *)(glob_cxt->foreignrel->fdw_private);

	/* 空节点直接返回true */
	if (node == NULL)
		return true;

	/* 初始化内层上下文 */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;
	inner_cxt.can_skip_cast = false;
	inner_cxt.can_pushdown_stable = false;
	inner_cxt.can_pushdown_volatile = false;
	inner_cxt.tdengine_fill_enable = false;
	inner_cxt.has_time_key = false;
	inner_cxt.has_sub_or_add_operator = false;
	inner_cxt.is_comparison = false;

	/* 根据节点类型进行不同处理 */
	switch (nodeTag(node))
	{
	case T_Var:
		/* 处理变量节点 */
		{
			Var *var = (Var *)node;

			if (bms_is_member(var->varno, glob_cxt->relids) &&
				var->varlevelsup == 0)
			{
				if (var->varattno < 0)
					return false;

				/* 检查是否为时间类型列 */
				if (TDENGINE_IS_TIME_TYPE(var->vartype))
				{
					is_time_column = true;
					if (outer_cxt->is_comparison &&
						outer_cxt->has_sub_or_add_operator &&
						outer_cxt->has_time_key)
						return false;
				}

				/* 标记当前目标是字段/标签列 */
				glob_cxt->mixing_aggref_status |= TDENGINE_TARGETS_MARK_COLUMN;

				collation = var->varcollid;
				state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
			}
			else
			{

				collation = var->varcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
				{

					state = FDW_COLLATE_NONE;
				}
				else
				{

					state = FDW_COLLATE_UNSAFE;
				}
			}
		}
		break;

	/* 处理常量节点(Const) */
	case T_Const:
	{
		char *type_name;
		Const *c = (Const *)node;

		/* 处理INTERVAL类型常量 */
		if (c->consttype == INTERVALOID)
		{
			Interval *interval = DatumGetIntervalP(c->constvalue);
			struct pg_tm tm;
			fsec_t fsec;
			interval2tm(*interval, &tm, &fsec);

			if (tm.tm_mon != 0 || tm.tm_year != 0)
			{
				return false;
			}
		}

		type_name = tdengine_get_data_type_name(c->consttype);
		if (strcmp(type_name, "tdengine_fill_enum") == 0)
			check_type = false;

		if (c->constcollid != InvalidOid &&
			c->constcollid != DEFAULT_COLLATION_OID)
			return false;

		/* 默认情况下认为常量不设置排序规则 */
		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
	}
	break;

	/*
	 * 处理参数节点(T_Param)
	 */
	case T_Param:
	{
		Param *p = (Param *)node;

		/* 检查参数类型是否有效 */
		if (!is_valid_type(p->paramtype))
			return false;

		if (TDENGINE_IS_TIME_TYPE(p->paramtype))
		{
			if (outer_cxt->is_comparison &&
				outer_cxt->has_sub_or_add_operator &&
				outer_cxt->has_time_key)
				return false;
		}

		collation = p->paramcollid;
		if (collation == InvalidOid ||
			collation == DEFAULT_COLLATION_OID)
			state = FDW_COLLATE_NONE;
		else
			state = FDW_COLLATE_UNSAFE;
	}
	break;
	case T_FieldSelect:
	{
		if (!(glob_cxt->foreignrel->reloptkind == RELOPT_BASEREL ||
			  glob_cxt->foreignrel->reloptkind == RELOPT_OTHER_MEMBER_REL))
			return false;

		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
		check_type = false;
	}
	break;

	case T_FuncExpr:
	{
		FuncExpr *fe = (FuncExpr *)node;
		char *opername = NULL;
		bool is_cast_func = false;
		bool is_star_func = false;
		bool can_pushdown_func = false;
		bool is_regex = false;

		/* 从系统缓存获取函数名称 */
		tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fe->funcid));
		if (!HeapTupleIsValid(tuple))
		{
			elog(ERROR, "cache lookup failed for function %u", fe->funcid);
		}
		opername = pstrdup(((Form_pg_proc)GETSTRUCT(tuple))->proname.data);
		ReleaseSysCache(tuple);

		/* 处理时间类型函数的特殊限制 */
		if (TDENGINE_IS_TIME_TYPE(fe->funcresulttype))
		{
			if (outer_cxt->is_comparison)
			{
				if (strcmp(opername, "now") != 0)
				{
					return false;
				}
				else if (!outer_cxt->has_time_key)
				{
					return false;
				}
			}
		}

		/* 检查是否为类型转换函数(float8/numeric) */
		if (strcmp(opername, "float8") == 0 || strcmp(opername, "numeric") == 0)
		{
			is_cast_func = true;
		}

		/* 下推到 TDengine */
		if (tdengine_is_star_func(fe->funcid, opername))
		{
			is_star_func = true;
			outer_cxt->can_pushdown_stable = true;
		}

		if (tdengine_is_unique_func(fe->funcid, opername) ||
			tdengine_is_supported_builtin_func(fe->funcid, opername))
		{
			can_pushdown_func = true;
			inner_cxt.can_skip_cast = true;
			outer_cxt->can_pushdown_volatile = true;
		}

		if (!(is_star_func || can_pushdown_func || is_cast_func))
			return false;

		// TODO: fill()函数相关
		/* fill() must be inside tdengine_time() */
		if (strcmp(opername, "tdengine_fill_numeric") == 0 ||
			strcmp(opername, "tdengine_fill_option") == 0)
		{
			if (outer_cxt->tdengine_fill_enable == false)
				elog(ERROR, "tdengine_fdw: syntax error tdengine_fill_numeric() or tdengine_fill_option() must be embedded inside tdengine_time() function\n");
		}
		if (is_cast_func)
		{
			/* 类型转换函数必须在外层允许跳过转换检查时才可下推 */
			if (outer_cxt->can_skip_cast == false)
				return false;
		}
		else
		{
			if (!glob_cxt->for_tlist && glob_cxt->is_inner_func)
				return false;

			glob_cxt->is_inner_func = true;
		}

		if (strcmp(opername, "tdengine_time") == 0)
		{
			inner_cxt.tdengine_fill_enable = true;
		}
		else
		{
			outer_cxt->have_otherfunc_tdengine_time_tlist = true;
		}

		if (!tdengine_foreign_expr_walker((Node *)fe->args,
										  glob_cxt, &inner_cxt))
			return false;

		inner_cxt.tdengine_fill_enable = false;

		if (!is_cast_func)
			glob_cxt->is_inner_func = false;

		if (list_length(fe->args) > 0)
		{
			ListCell *funclc;
			Node *firstArg;

			funclc = list_head(fe->args);
			firstArg = (Node *)lfirst(funclc);

			if (IsA(firstArg, Const))
			{
				Const *arg = (Const *)firstArg;
				char *extval;

				if (arg->consttype == TEXTOID)
					is_regex = tdengine_is_regex_argument(arg, &extval);
			}
		}

		if (is_regex)
		{
			collation = InvalidOid;
			state = FDW_COLLATE_NONE;
			check_type = false;
			outer_cxt->can_pushdown_stable = true;
		}
		else
		{
			if (fe->inputcollid == InvalidOid)
				/* OK, inputs are all noncollatable */;
			else if (inner_cxt.state != FDW_COLLATE_SAFE ||
					 fe->inputcollid != inner_cxt.collation)
				return false;
			collation = fe->funccollid;
			if (collation == InvalidOid)
				state = FDW_COLLATE_NONE;
			else if (inner_cxt.state == FDW_COLLATE_SAFE &&
					 collation == inner_cxt.collation)
				state = FDW_COLLATE_SAFE;
			else if (collation == DEFAULT_COLLATION_OID)
				state = FDW_COLLATE_NONE;
			else
				state = FDW_COLLATE_UNSAFE;
		}
	}
	break;
	case T_OpExpr:
	{
		OpExpr *oe = (OpExpr *)node;
		bool is_slvar = false;
		bool is_param = false;
		bool has_time_key = false;
		bool has_time_column = false;
		bool has_time_tags_or_fields_column = false;

		if (tdengine_is_slvar_fetch(node, &(fpinfo->slinfo)))
			is_slvar = true;

		if (tdengine_is_param_fetch(node, &(fpinfo->slinfo)))
			is_param = true;
		if (!tdengine_is_builtin(oe->opno) && !is_slvar && !is_param)
			return false;

		tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oe->opno));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for operator %u", oe->opno);
		form = (Form_pg_operator)GETSTRUCT(tuple);

		cur_opname = pstrdup(NameStr(form->oprname));
		ReleaseSysCache(tuple);

		if (strcmp(cur_opname, "=") == 0 ||
			strcmp(cur_opname, ">") == 0 ||
			strcmp(cur_opname, "<") == 0 ||
			strcmp(cur_opname, ">=") == 0 ||
			strcmp(cur_opname, "<=") == 0 ||
			strcmp(cur_opname, "!=") == 0 ||
			strcmp(cur_opname, "<>") == 0)
		{
			inner_cxt.is_comparison = true;
		}

		if (inner_cxt.is_comparison &&
			exprType((Node *)linitial(oe->args)) == INTERVALOID &&
			exprType((Node *)lsecond(oe->args)) == INTERVALOID)
		{
			return false;
		}

		has_time_key = tdengine_contain_time_key_column(glob_cxt->relid, oe->args);

		if (inner_cxt.is_comparison &&
			!has_time_key &&
			tdengine_contain_time_expr(oe->args))
		{
			return false;
		}

		if (strcmp(cur_opname, "!=") == 0 ||
			strcmp(cur_opname, "<>") == 0)
		{
			if (has_time_key)
				return false;
		}

		has_time_column = tdengine_contain_time_column(oe->args, &(fpinfo->slinfo));

		has_time_tags_or_fields_column = (has_time_column && !has_time_key);

		if (inner_cxt.is_comparison &&
			has_time_tags_or_fields_column &&
			tdengine_contain_time_function(oe->args))
		{
			return false;
		}

		if (strcmp(cur_opname, ">") == 0 || strcmp(cur_opname, "<") == 0 || strcmp(cur_opname, ">=") == 0 || strcmp(cur_opname, "<=") == 0 || strcmp(cur_opname, "=") == 0)
		{
			List *first = list_make1(linitial(oe->args));
			List *second = list_make1(lsecond(oe->args));
			bool has_both_time_colum = tdengine_contain_time_column(first, &(fpinfo->slinfo)) &&
									   tdengine_contain_time_column(second, &(fpinfo->slinfo));
			if (has_time_key && has_both_time_colum)
			{
				return false;
			}

			if (strcmp(cur_opname, "=") != 0)
			{
				bool has_first_time_key = tdengine_contain_time_key_column(glob_cxt->relid, first);
				bool has_second_time_key = tdengine_contain_time_key_column(glob_cxt->relid, second);
				bool has_both_tags_or_fields_column = (has_both_time_colum && !has_first_time_key && !has_second_time_key);

				if (has_both_tags_or_fields_column)
					return false;

				if (has_time_tags_or_fields_column &&
					(tdengine_contain_time_const(oe->args) ||
					 tdengine_contain_time_param(oe->args)))
				{
					return false;
				}

				if (tdengine_is_string_type((Node *)linitial(oe->args), &(fpinfo->slinfo)))
				{
					return false;
				}
			}
		}

		if (strcmp(cur_opname, "+") == 0 ||
			strcmp(cur_opname, "-") == 0)
		{
			inner_cxt.has_time_key = outer_cxt->has_time_key;
			inner_cxt.is_comparison = outer_cxt->is_comparison;
			inner_cxt.has_sub_or_add_operator = true;
		}
		else
		{
			inner_cxt.has_time_key = has_time_key;
		}

		if (is_slvar || is_param)
		{
			collation = oe->inputcollid;
			check_type = false;

			state = FDW_COLLATE_SAFE;

			break;
		}

		if (!tdengine_foreign_expr_walker((Node *)oe->args,
										  glob_cxt, &inner_cxt))
			return false;

		if ((glob_cxt->mixing_aggref_status & TDENGINE_TARGETS_MIXING_AGGREF_UNSAFE) ==TDENGINE_TARGETS_MIXING_AGGREF_UNSAFE)
		{
			return false;
		}

		if (oe->inputcollid == InvalidOid)
			;
		else if (inner_cxt.state != FDW_COLLATE_SAFE || oe->inputcollid != inner_cxt.collation)
			return false;

		collation = oe->opcollid;
		if (collation == InvalidOid)
			state = FDW_COLLATE_NONE;
		else if (inner_cxt.state == FDW_COLLATE_SAFE &&collation == inner_cxt.collation)
			state = FDW_COLLATE_SAFE;
		else
			state = FDW_COLLATE_UNSAFE;
	}
	break;
		/*
		 * 处理标量数组操作表达式节点(T_ScalarArrayOpExpr)
		 */
	case T_ScalarArrayOpExpr:
	{
		ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *)node;

		/* 从系统缓存获取操作符信息 */
		tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oe->opno));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for operator %u", oe->opno);
		form = (Form_pg_operator)GETSTRUCT(tuple);

		cur_opname = pstrdup(NameStr(form->oprname));
		ReleaseSysCache(tuple);

		/* 检查字符串类型比较操作是否合法 */
		if (tdengine_is_string_type((Node *)linitial(oe->args), &(fpinfo->slinfo)))
		{
			if (strcmp(cur_opname, "<") == 0 ||strcmp(cur_opname, ">") == 0 ||strcmp(cur_opname, "<=") == 0 ||strcmp(cur_opname, ">=") == 0)
			{
				return false;
			}
		}

		/* 检查是否为内置操作符 */
		if (!tdengine_is_builtin(oe->opno))
			return false;

		/* 检查是否包含时间列 */
		if (tdengine_contain_time_column(oe->args, &(fpinfo->slinfo)))
		{
			return false;
		}

		/* 递归检查子表达式 */
		if (!tdengine_foreign_expr_walker((Node *)oe->args,
										  glob_cxt, &inner_cxt))
			return false;

		/* 检查输入排序规则是否合法 */
		if (oe->inputcollid == InvalidOid);
		else if (inner_cxt.state != FDW_COLLATE_SAFE ||
				 oe->inputcollid != inner_cxt.collation)
			return false;

		/* 输出总是布尔类型且无排序规则 */
		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
	}
	break;
		/*
		 * 处理类型重标记节点(T_RelabelType)
		 */
	case T_RelabelType:
	{
		RelabelType *r = (RelabelType *)node;

		/* 递归检查子表达式 */
		if (!tdengine_foreign_expr_walker((Node *)r->arg,glob_cxt, &inner_cxt))
			return false;

		/* 获取并检查类型转换后的排序规则 */
		collation = r->resultcollid;
		if (collation == InvalidOid)
			state = FDW_COLLATE_NONE;
		else if (inner_cxt.state == FDW_COLLATE_SAFE &&collation == inner_cxt.collation)
			state = FDW_COLLATE_SAFE;
		else
			state = FDW_COLLATE_UNSAFE;
	}
	break;
		/*
		 * 处理布尔表达式节点(T_BoolExpr)
		 */
	case T_BoolExpr:
	{
		BoolExpr *b = (BoolExpr *)node;

		is_time_column = false;

		/* 不支持NOT操作符 */
		if (b->boolop == NOT_EXPR)
		{
			return false;
		}

		/* 递归检查子表达式 */
		if (!tdengine_foreign_expr_walker((Node *)b->args,glob_cxt, &inner_cxt))
			return false;

		/* 特殊处理OR表达式: 包含时间列则拒绝下推 */
		if (b->boolop == OR_EXPR && is_time_column)
		{
			is_time_column = false;
			return false;
		}

		/* 布尔类型无排序规则 */
		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
	}
	break;
		/*
		 * 处理列表节点(T_List)
		 */
	case T_List:
	{
		List *l = (List *)node;
		ListCell *lc;

		/* 继承外层上下文标志 */
		inner_cxt.can_skip_cast = outer_cxt->can_skip_cast;
		inner_cxt.tdengine_fill_enable = outer_cxt->tdengine_fill_enable;
		inner_cxt.has_time_key = outer_cxt->has_time_key;
		inner_cxt.has_sub_or_add_operator = outer_cxt->has_sub_or_add_operator;
		inner_cxt.is_comparison = outer_cxt->is_comparison;

		/* 递归检查每个子表达式 */
		foreach (lc, l)
		{
			if (!tdengine_foreign_expr_walker((Node *)lfirst(lc),glob_cxt, &inner_cxt))
				return false;
		}

		/* 从子表达式继承排序规则状态 */
		collation = inner_cxt.collation;
		state = inner_cxt.state;

		/* 不检查列表本身的类型 */
		check_type = false;
	}
	break;
	case T_Aggref:
	{
		Aggref *agg = (Aggref *)node;
		ListCell *lc;
		char *opername = NULL;
		bool old_val;
		int index_const = -1;
		int index;
		bool is_regex = false;
		bool is_star_func = false;
		bool is_not_star_func = false;
		Oid agg_inputcollid = agg->inputcollid;

		/* get function name and schema */
		opername = get_func_name(agg->aggfnoid);

		// TODO:
		/* these function can be passed to TDengine */
		if ((strcmp(opername, "sum") == 0 ||
			 strcmp(opername, "max") == 0 ||
			 strcmp(opername, "min") == 0 ||
			 strcmp(opername, "count") == 0 ||
			 strcmp(opername, "tdengine_distinct") == 0 || 
			 strcmp(opername, "spread") == 0 ||
			 strcmp(opername, "sample") == 0 ||
			 strcmp(opername, "first") == 0 ||
			 strcmp(opername, "last") == 0 ||
			 strcmp(opername, "integral") == 0 ||
			 strcmp(opername, "mean") == 0 ||
			 strcmp(opername, "median") == 0 ||
			 strcmp(opername, "tdengine_count") == 0 ||
			 strcmp(opername, "tdengine_mode") == 0 ||
			 strcmp(opername, "stddev") == 0 ||
			 strcmp(opername, "tdengine_sum") == 0 || 
			 strcmp(opername, "tdengine_max") == 0 || 
			 strcmp(opername, "tdengine_min") == 0))  
		{
			is_not_star_func = true;
		}

		is_star_func = tdengine_is_star_func(agg->aggfnoid, opername);

		if (!(is_star_func || is_not_star_func))
			return false;

		if (strcmp(opername, "sample") == 0 ||
			strcmp(opername, "integral") == 0)
			index_const = 1;

		if (strcmp(opername, "sum") == 0 ||
			strcmp(opername, "spread") == 0 ||
			strcmp(opername, "count") == 0)
		{
		}

		if (glob_cxt->foreignrel->reloptkind != RELOPT_UPPER_REL)
			return false;

		// 只有简单的、未分割的聚合函数(AGGSPLIT_SIMPLE模式)才能被下推到远程执行。
		if (agg->aggsplit != AGGSPLIT_SIMPLE)
			return false;
		old_val = is_time_column;
		is_time_column = false;

		index = -1;
		foreach (lc, agg->args)
		{
			Node *n = (Node *)lfirst(lc);
			OpExpr *oe = (OpExpr *)NULL;
			Oid resulttype = InvalidOid;
			bool is_slvar = false;

			index++;

			if (IsA(n, TargetEntry))
			{
				TargetEntry *tle = (TargetEntry *)n;

				n = (Node *)tle->expr;

				if (IsA(n, Var) ||
					((index == index_const) && IsA(n, Const)))
				else if (IsA(n, Const))
				{
					Const *arg = (Const *)n;
					char *extval;

					if (arg->consttype == TEXTOID)
					{
						is_regex = tdengine_is_regex_argument(arg, &extval);
						if (is_regex)
						else
							return false;
					}
					else
						return false;
				}
				else if (fpinfo->slinfo.schemaless &&
						 (IsA(n, CoerceViaIO) || IsA(n, OpExpr)))
				{
					if (IsA(n, OpExpr))
					{
						oe = (OpExpr *)n;
						resulttype = oe->opresulttype;
					}
					else
					{
						CoerceViaIO *cio = (CoerceViaIO *)n;
						oe = (OpExpr *)cio->arg;
						resulttype = cio->resulttype;
					}

					if (tdengine_is_slvar_fetch((Node *)oe, &(fpinfo->slinfo)))
						is_slvar = true;
					else
						return false;
				}
				else if (is_star_func)
				else
					return false;
			}

			if (IsA(n, Var) || is_slvar)
			{
				Var *var;
				char *colname;

				if (is_slvar)
				{
					Const *cnst;

					var = linitial_node(Var, oe->args);
					cnst = lsecond_node(Const, oe->args);
					colname = TextDatumGetCString(cnst->constvalue);
					agg_inputcollid = var->varcollid;
				}
				else
				{
					var = (Var *)n;

					colname = tdengine_get_column_name(glob_cxt->relid, var->varattno);
					resulttype = var->vartype;
				}

				if (tdengine_is_tag_key(colname, glob_cxt->relid))
					return false;
				if ((strcmp(opername, "max") == 0 || strcmp(opername, "min") == 0) && (resulttype == TEXTOID || resulttype == InvalidOid))
					return false;
			}

			if (!tdengine_foreign_expr_walker(n, glob_cxt, &inner_cxt))
				return false;
			if (is_time_column && !(strcmp(opername, "last") == 0 || strcmp(opername, "first") == 0))
			{
				is_time_column = false;
				return false;
			}
		}
		is_time_column = old_val;

		if (agg->aggorder || agg->aggfilter)
		{
			return false;
		}

		if (agg->aggdistinct && (strcmp(opername, "count") != 0))
			return false;

		if (is_regex)
			check_type = false;
		else
		{
			if (agg_inputcollid == InvalidOid)
			else if (inner_cxt.state != FDW_COLLATE_SAFE ||
					 agg_inputcollid != inner_cxt.collation)
				return false;
		}
		collation = agg->aggcollid;
		if (collation == InvalidOid)
			state = FDW_COLLATE_NONE;
		else if (inner_cxt.state == FDW_COLLATE_SAFE &&
				 collation == inner_cxt.collation)
			state = FDW_COLLATE_SAFE;
		else if (collation == DEFAULT_COLLATION_OID)
			state = FDW_COLLATE_NONE;
		else
			state = FDW_COLLATE_UNSAFE;
	}
	break;
		/*
		 * 处理类型转换节点(T_CoerceViaIO)
		 */
	case T_CoerceViaIO:
	{
		CoerceViaIO *cio = (CoerceViaIO *)node;
		Node *arg = (Node *)cio->arg;

		/* 检查时间键与无模式变量的时间类型比较 */
		if (tdengine_is_slvar_fetch(arg, &(fpinfo->slinfo)))
		{
			if (TDENGINE_IS_TIME_TYPE(cio->resulttype))
			{
				/* 如果是比较操作且包含时间键和加减操作符则拒绝下推 */
				if (outer_cxt->is_comparison &&
					outer_cxt->has_sub_or_add_operator &&
					outer_cxt->has_time_key)
				{
					return false;
				}
			}
		}

		/* 只允许无模式变量或参数获取表达式下推 */
		if (tdengine_is_slvar_fetch(arg, &(fpinfo->slinfo)) ||
			tdengine_is_param_fetch(arg, &(fpinfo->slinfo)))
		{
			/* 递归检查子表达式 */
			if (!tdengine_foreign_expr_walker(arg, glob_cxt, &inner_cxt))
				return false;
		}
		else
		{
			return false;
		}

		/* 类型转换无排序规则 */
		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
	}
	break;
		/*
		 */
	case T_NullTest:
	{
		NullTest *nt = (NullTest *)node;
		char *colname;

		/* 获取无模式变量列名 */
		colname = tdengine_get_slvar(nt->arg, &(fpinfo->slinfo));

		/* 检查是否为标签键 */
		if (colname == NULL || !tdengine_is_tag_key(colname, glob_cxt->relid))
			return false;

		/* 布尔类型无排序规则 */
		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
	}
	break;
		/*
		 * 处理数组表达式节点(T_ArrayExpr)
		 * 功能: 检查数组表达式是否可下推到远程执行
		 *
		 * 处理流程:
		 *   1. 递归检查数组元素子表达式
		 *   2. 获取数组的排序规则ID(array_collid)
		 *   3. 检查排序规则状态:
		 *      - 无排序规则(InvalidOid): 标记为FDW_COLLATE_NONE
		 *      - 排序规则与子表达式一致: 标记为FDW_COLLATE_SAFE
		 *      - 默认排序规则: 标记为FDW_COLLATE_NONE
		 *      - 其他情况: 标记为FDW_COLLATE_UNSAFE
		 *
		 * 注意事项:
		 *   - 数组表达式必须从输入变量继承排序规则
		 *   - 与函数表达式使用相同的排序规则检查逻辑
		 *   - 默认排序规则被视为无排序规则
		 */
	case T_ArrayExpr:
	{
		ArrayExpr *a = (ArrayExpr *)node;

		/* 递归检查数组元素 */
		if (!tdengine_foreign_expr_walker((Node *)a->elements,
										  glob_cxt, &inner_cxt))
			return false;

		/* 检查数组排序规则 */
		collation = a->array_collid;
		if (collation == InvalidOid)
			state = FDW_COLLATE_NONE;
		else if (inner_cxt.state == FDW_COLLATE_SAFE &&
				 collation == inner_cxt.collation)
			state = FDW_COLLATE_SAFE;
		else if (collation == DEFAULT_COLLATION_OID)
			state = FDW_COLLATE_NONE;
		else
			state = FDW_COLLATE_UNSAFE;
	}
	break;

	case T_DistinctExpr:
		return false;
	default:
		return false;
	}

	/*
	 * 如果表达式的返回类型不是内置类型，则不能下推到远程执行，
	 * 因为可能在远程端有不兼容的语义
	 */
	if (check_type && !tdengine_is_builtin(exprType(node)))
		return false;

	/*
	 * 将当前节点的排序规则信息合并到父节点的状态中
	 */
	if (state > outer_cxt->state)
	{
		/* 覆盖父节点之前的排序规则状态 */
		outer_cxt->collation = collation;
		outer_cxt->state = state;
	}
	else if (state == outer_cxt->state)
	{
		switch (state)
		{
		case FDW_COLLATE_NONE:
			break;
		case FDW_COLLATE_SAFE:
			if (collation != outer_cxt->collation)
			{
				if (outer_cxt->collation == DEFAULT_COLLATION_OID)
				{
					outer_cxt->collation = collation;
				}
				else if (collation != DEFAULT_COLLATION_OID)
				{
					outer_cxt->state = FDW_COLLATE_UNSAFE;
				}
			}
			break;
		case FDW_COLLATE_UNSAFE:
			break;
		}
	}

	return true;
}

/*
 * tdengine_build_tlist_to_deparse - 构建用于反解析为SELECT子句的目标列表
 */
List *tdengine_build_tlist_to_deparse(RelOptInfo *foreignrel)
{
	/* 初始化空目标列表 */
	List *tlist = NIL;
	/* 获取外部表私有信息 */
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)foreignrel->fdw_private;
	ListCell *lc;

	if (foreignrel->reloptkind == RELOPT_UPPER_REL)
		return fpinfo->grouped_tlist;

	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *)foreignrel->reltarget->exprs,
											  PVC_RECURSE_PLACEHOLDERS));

	foreach (lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		tlist = add_to_flat_tlist(tlist,
								  pull_var_clause((Node *)rinfo->clause,
												  PVC_RECURSE_PLACEHOLDERS));
	}
	return tlist;
}

/*
 * 反解析远程DELETE语句
 */
void tdengine_deparse_delete(StringInfo buf, PlannerInfo *root,
							 Index rtindex, Relation rel,
							 List *attname)
{
	int i = 0;	  
	ListCell *lc; 

	/* 添加DELETE FROM关键字 */
	appendStringInfoString(buf, "DELETE FROM ");

	/* 反解析表名并添加到缓冲区 */
	tdengine_deparse_relation(buf, rel);

	/* 遍历属性列表构建WHERE条件 */
	foreach (lc, attname)
	{
		int attnum = lfirst_int(lc); // 获取当前属性编号

		/* 添加WHERE或AND连接符(第一个条件用WHERE，后续用AND) */
		appendStringInfo(buf, i == 0 ? " WHERE " : " AND ");

		/* 反解析列引用(表名.列名形式) */
		tdengine_deparse_column_ref(buf, rtindex, attnum, -1, root, false, false);

		/* 添加参数占位符($1, $2等) */
		appendStringInfo(buf, "=$%d", i + 1);
		i++; // 递增参数计数器
	}

	elog(DEBUG1, "delete:%s", buf->data);
}

/*
 * 反解析SELECT语句
 */
void tdengine_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel,List *tlist, List *remote_conds, List *pathkeys,bool is_subquery, List **retrieved_attrs,List **params_list,bool has_limit)
{
	// 反解析上下文结构体
	deparse_expr_cxt context;
	// 获取FDW关系私有信息
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)rel->fdw_private;
	// 条件表达式列表
	List *quals;

	Assert(rel->reloptkind == RELOPT_JOINREL ||rel->reloptkind == RELOPT_BASEREL ||rel->reloptkind == RELOPT_OTHER_MEMBER_REL ||rel->reloptkind == RELOPT_UPPER_REL);

	/* 初始化反解析上下文 */
	context.buf = buf;		  
	context.root = root;	  
	context.foreignrel = rel; 
	// 上层关系使用外部关系作为扫描关系，其他使用自身
	context.scanrel = (rel->reloptkind == RELOPT_UPPER_REL) ? fpinfo->outerrel : rel;
	context.params_list = params_list;	  
	context.op_type = UNKNOWN_OPERATOR;	  
	context.is_tlist = false;			  
	context.can_skip_cast = false;		  
	context.convert_to_timestamp = false; 
	context.has_bool_cmp = false;		  

	/* 构建SELECT子句 */
	tdengine_deparse_select(tlist, retrieved_attrs, &context);

	/*
	 * 处理条件表达式:
	 */
	if (rel->reloptkind == RELOPT_UPPER_REL)
	{
		// 获取外部关系的FDW信息
		TDengineFdwRelationInfo *ofpinfo = (TDengineFdwRelationInfo *)fpinfo->outerrel->fdw_private;
		quals = ofpinfo->remote_conds; // 使用外部关系的远程条件
	}
	else
	{
		quals = remote_conds; // 直接使用传入的条件
	}

	/* 构建FROM和WHERE子句 */
	tdengine_deparse_from_expr(quals, &context);

	/* 处理上层关系的特殊子句 */
	if (rel->reloptkind == RELOPT_UPPER_REL)
	{
		/* 添加GROUP BY子句 */
		tdengine_append_group_by_clause(tlist, &context);
	}

	/* 添加ORDER BY子句 */
	if (pathkeys)
		tdengine_append_order_by_clause(pathkeys, &context);

	/* 添加LIMIT子句 */
	if (has_limit)
		tdengine_append_limit_clause(&context);
}

/**
 * get_proname - 根据函数OID获取函数名称并添加到输出缓冲区
 */
static void get_proname(Oid oid, StringInfo proname)
{
	HeapTuple proctup;	   // 函数元组指针
	Form_pg_proc procform; // 函数元组结构体
	const char *name;	   // 函数名称字符串

	// 在系统缓存中查找函数元组
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(oid));
	// 检查元组有效性
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", oid);

	procform = (Form_pg_proc)GETSTRUCT(proctup);

	name = NameStr(procform->proname);
	appendStringInfoString(proname, name);

	ReleaseSysCache(proctup);
}

/*
 * 反解析SELECT语句
 */
static void tdengine_deparse_select(List *tlist, List **retrieved_attrs, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;														 
	PlannerInfo *root = context->root;													 
	RelOptInfo *foreignrel = context->foreignrel;										 
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)foreignrel->fdw_private;

	/* 添加SELECT关键字 */
	appendStringInfoString(buf, "SELECT ");

	/* 处理连接关系或上层关系 */
	if (foreignrel->reloptkind == RELOPT_JOINREL ||fpinfo->is_tlist_func_pushdown == true ||foreignrel->reloptkind == RELOPT_UPPER_REL)
	{
		/*
		 * 对于连接关系或上层关系，直接使用输入的目标列表
		 * 因为这些关系已经确定了需要从远程服务器获取的列
		 */
		tdengine_deparse_explicit_target_list(tlist, retrieved_attrs, context);
	}
	else
	{

		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root); // 获取范围表条目

		Relation rel = table_open(rte->relid, NoLock); // 打开表

		/* 根据是否为无模式表选择不同的反解析方式 */
		if (fpinfo->slinfo.schemaless)
			tdengine_deparse_target_list_schemaless(buf, rel, rte->relid,fpinfo->attrs_used, retrieved_attrs,fpinfo->all_fieldtag,fpinfo->slcols);
		else
			tdengine_deparse_target_list(buf, root, foreignrel->relid, rel, fpinfo->attrs_used, retrieved_attrs);

		table_close(rel, NoLock); 
	}
}

/*
 * 反解析FROM子句表达式
 */
static void
tdengine_deparse_from_expr(List *quals, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;			// 输出缓冲区
	RelOptInfo *scanrel = context->scanrel; // 扫描关系信息

	/* 验证上层关系的扫描关系类型 */
	Assert(context->foreignrel->reloptkind != RELOPT_UPPER_REL ||scanrel->reloptkind == RELOPT_JOINREL ||scanrel->reloptkind == RELOPT_BASEREL);

	/* 构建FROM子句 */
	appendStringInfoString(buf, " FROM ");
	tdengine_deparse_from_expr_for_rel(buf, context->root, scanrel,(bms_num_members(scanrel->relids) > 1),context->params_list);

	if (quals != NIL)
	{
		appendStringInfo(buf, " WHERE ");
		tdengine_append_conditions(quals, context);
	}
}

/*
 * 反解析条件表达式列表
 */
static void
tdengine_append_conditions(List *exprs, deparse_expr_cxt *context)
{
	int nestlevel;				   // GUC嵌套级别
	ListCell *lc;				   // 列表迭代器
	bool is_first = true;		   // 是否是第一个条件
	StringInfo buf = context->buf; // 输出缓冲区

	/* 设置传输模式确保常量值可移植输出 */
	nestlevel = tdengine_set_transmission_modes();

	/* 遍历条件表达式列表 */
	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc); // 获取当前条件表达式

		if (IsA(expr, RestrictInfo))
			expr = ((RestrictInfo *)expr)->clause;

		if (!is_first)
			appendStringInfoString(buf, " AND ");


		appendStringInfoChar(buf, '(');
		tdengine_deparse_expr(expr, context);
		appendStringInfoChar(buf, ')');

		context->has_bool_cmp = false;

		is_first = false; // 标记已处理第一个条件
	}

	/* 重置传输模式 */
	tdengine_reset_transmission_modes(nestlevel);
}

/*
 * 反解析显式目标列表到SQL SELECT语句
 */
static void tdengine_deparse_explicit_target_list(List *tlist, List **retrieved_attrs,deparse_expr_cxt *context)
{
	ListCell *lc;																				   
	StringInfo buf = context->buf;																   
	int i = 0;																					   
	bool first = true;																			   
	bool is_col_grouping_target = false;														   
	bool need_field_key = true;																	   
	bool is_need_comma = false;																	   
	bool selected_all_fieldtag = false;															   
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)context->foreignrel->fdw_private; 

	*retrieved_attrs = NIL; // 初始化返回的属性索引列表

	context->is_tlist = true; // 标记当前正在处理目标列表

	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc); // 获取当前目标条目
		bool is_slvar = false;							 // 是否是无模式变量

		/* 检查是否是无模式变量 */
		if (tdengine_is_slvar_fetch((Node *)tle->expr, &(fpinfo->slinfo)))
			is_slvar = true;

		/* 检查是否是分组目标列 */
		if (!fpinfo->is_tlist_func_pushdown && IsA((Expr *)tle->expr, Var))
		{
			is_col_grouping_target = tdengine_is_grouping_target(tle, context->root->parse);
		}

		/* 处理无模式变量的分组目标检查 */
		if (is_slvar)
		{
			is_col_grouping_target = tdengine_is_grouping_target(tle, context->root->parse);
		}

		/* 处理不同类型的表达式 */
		if (IsA((Expr *)tle->expr, Aggref) ||										// 聚合函数
			(IsA((Expr *)tle->expr, OpExpr) && !is_slvar) ||						// 操作符表达式(非无模式变量)
			IsA((Expr *)tle->expr, FuncExpr) ||										// 函数调用
			((IsA((Expr *)tle->expr, Var) || is_slvar) && !is_col_grouping_target)) // 变量引用(非分组目标)
		{
			bool is_skip_expr = false; // 是否跳过当前表达式

			/* 特殊处理某些函数调用 */
			if (IsA((Expr *)tle->expr, FuncExpr))
			{
				FuncExpr *fe = (FuncExpr *)tle->expr;
				StringInfo func_name = makeStringInfo();

				get_proname(fe->funcid, func_name);
				/* 跳过特定函数 */
				if (strcmp(func_name->data, "tdengine_time") == 0 ||
					strcmp(func_name->data, "tdengine_fill_numeric") == 0 ||
					strcmp(func_name->data, "tdengine_fill_option") == 0)
					is_skip_expr = true;
			}

			if (is_need_comma && !is_skip_expr)
				appendStringInfoString(buf, ", ");
			need_field_key = false; // 标记不需要额外字段键

			if (!is_skip_expr)
			{
				if (fpinfo->is_tlist_func_pushdown && fpinfo->all_fieldtag)
					selected_all_fieldtag = true; // 标记选择了所有字段标签
				else
				{
					first = false;
					/* 反解析表达式到输出缓冲区 */
					tdengine_deparse_expr((Expr *)tle->expr, context);
					is_need_comma = true;
				}
			}
		}

		if (IsA((Expr *)tle->expr, Var) && need_field_key)
		{
			RangeTblEntry *rte = planner_rt_fetch(context->scanrel->relid, context->root);
			char *colname = tdengine_get_column_name(rte->relid, ((Var *)tle->expr)->varattno);

			if (!tdengine_is_tag_key(colname, rte->relid))
				need_field_key = false; // 发现非标签键列，不需要额外字段键
		}

		/* 将属性索引添加到返回列表 */
		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
		i++;
	}
	context->is_tlist = false; // 结束目标列表处理

	if (i == 0 || selected_all_fieldtag)
	{
		appendStringInfoString(buf, "*"); // 空列表或全字段标签时使用*
		return;
	}

	if (need_field_key)
	{
		RangeTblEntry *rte = planner_rt_fetch(context->scanrel->relid, context->root);
		Relation rel = table_open(rte->relid, NoLock);
		TupleDesc tupdesc = RelationGetDescr(rel);

		/* 添加字段键到输出 */
		tdengine_append_field_key(tupdesc, context->buf, context->scanrel->relid, context->root, first);

		table_close(rel, NoLock);
		return;
	}
}

/*
 * 反解析FROM子句表达式
 */
static void
tdengine_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel,
								   bool use_alias, List **params_list)
{
	Assert(!use_alias); // 确保不使用别名(当前实现限制)
	if (foreignrel->reloptkind == RELOPT_JOINREL)
	{
		Assert(false); // 触发断言失败
	}
	else
	{
		/* 获取范围表条目 */
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

		Relation rel = table_open(rte->relid, NoLock); // 打开表

		/* 反解析关系名称到输出缓冲区 */
		tdengine_deparse_relation(buf, rel);

		table_close(rel, NoLock); // 关闭表
	}
}

void tdengine_deparse_analyze(StringInfo sql, char *dbname, char *relname)
{
	appendStringInfo(sql, "SELECT");
	appendStringInfo(sql, " round(((data_length + index_length)), 2)");
	appendStringInfo(sql, " FROM information_schema.TABLES");
	appendStringInfo(sql, " WHERE table_schema = '%s' AND table_name = '%s'", dbname, relname);
}

/*
 * 反解析目标列列表，生成SELECT语句中的列名列表
 */
static void
tdengine_deparse_target_list(StringInfo buf,PlannerInfo *root,Index rtindex,Relation rel,Bitmapset *attrs_used,List **retrieved_attrs)
{
	// 获取关系的元组描述符
	TupleDesc tupdesc = RelationGetDescr(rel);
	bool have_wholerow; // 是否有整行引用
	bool first;			// 是否是第一个列
	int i;
	bool need_field_key; // 是否需要添加字段键

	/* 检查是否有整行引用(如SELECT *) */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,attrs_used);

	first = true;
	need_field_key = true;

	*retrieved_attrs = NIL; // 初始化返回的属性列表

	/* 遍历所有属性列 */
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		if (attr->attisdropped)
			continue;

		/* 检查列是否被使用(整行引用或位图集中标记) */
		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,attrs_used))
		{
			RangeTblEntry *rte = planner_rt_fetch(rtindex, root);
			char *name = tdengine_get_column_name(rte->relid, i);

			if (!TDENGINE_IS_TIME_COLUMN(name))
			{
				// 如果列不是标签键，则不需要额外添加字段键
				if (!tdengine_is_tag_key(name, rte->relid))
					need_field_key = false;

				// 如果不是第一个列，添加逗号分隔符
				if (!first)
					appendStringInfoString(buf, ", ");
				first = false;

				// 反解析列引用并添加到缓冲区
				tdengine_deparse_column_ref(buf, rtindex, i, -1, root, false, false);
			}

			// 将属性编号添加到返回列表
			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	/* 如果没有找到任何列，使用'*'代替NULL */
	if (first)
	{
		appendStringInfoString(buf, "*");
		return;
	}

	/* 如果目标列表全是标签键，需要额外添加一个字段键 */
	if (need_field_key)
	{
		tdengine_append_field_key(tupdesc, buf, rtindex, root, first);
	}
}

/*
 * tdengine_deparse_column_ref - 反解析列引用并输出到缓冲区
 */
static void tdengine_deparse_column_ref(StringInfo buf, int varno, int varattno, Oid vartype,
							PlannerInfo *root, bool convert, bool *can_delete_directly)
{
	RangeTblEntry *rte;
	char *colname = NULL;

	/* varno必须不是OUTER_VAR、INNER_VAR或INDEX_VAR等特殊变量号 */
	Assert(!IS_SPECIAL_VARNO(varno));

	/* 从规划器信息中获取范围表条目 */
	rte = planner_rt_fetch(varno, root);

	/* 获取列名 */
	colname = tdengine_get_column_name(rte->relid, varattno);

	/*
	 * 检查DELETE语句是否能直接下推:
	 * 如果WHERE子句包含非时间列且非标签键的字段，则不能直接下推
	 */
	if (can_delete_directly)
		if (!TDENGINE_IS_TIME_COLUMN(colname) && !tdengine_is_tag_key(colname, rte->relid))
			*can_delete_directly = false;

	/* 处理布尔类型转换 */
	if (convert && vartype == BOOLOID)
	{
		appendStringInfo(buf, "(%s=true)", tdengine_quote_identifier(colname, QUOTE));
	}
	else
	{
		/* 特殊处理时间列 */
		if (TDENGINE_IS_TIME_COLUMN(colname))
			appendStringInfoString(buf, "time");
		else
			/* 普通列添加引号 */
			appendStringInfoString(buf, tdengine_quote_identifier(colname, QUOTE));
	}
}

/*
 * 添加反斜杠转义特殊字符到字符串缓冲区
 */
static void
add_backslash(StringInfo buf, const char *ptr, const char *regex_special)
{
	char ch = *ptr; // 获取当前字符

	/* 检查是否是正则表达式特殊字符 */
	if (strchr(regex_special, ch) != NULL)
	{
		/* 添加反斜杠转义 */
		appendStringInfoChar(buf, '\\'); // 先添加反斜杠
		appendStringInfoChar(buf, ch);	 // 再添加字符本身
	}
	else
	{
		/* 非特殊字符直接添加 */
		appendStringInfoChar(buf, ch);
	}
}
/*
 * 检查字符串中最后一个百分号(%)是否被转义
 */
static bool tdengine_last_percent_sign_check(const char *val)
{
	int len;				 // 字符串长度索引
	int count_backslash = 0; // 连续反斜杠计数器

	// 处理空指针情况
	if (val == NULL)
		return false;

	// 定位到字符串最后一个字符
	len = strlen(val) - 1;

	// 如果最后一个字符不是百分号，直接返回true
	if (val[len] != '%')
		return true;

	// 从倒数第二个字符开始向前扫描反斜杠
	len--;
	while (len >= 0 && val[len] == '\\')
	{
		count_backslash++; // 统计连续反斜杠数量
		len--;
	}

	// 根据反斜杠数量的奇偶性判断是否转义
	if (count_backslash % 2 == 0)
		return false; // 偶数个反斜杠，百分号未被转义

	return true; // 奇数个反斜杠，百分号被转义
}
/*
 * 将PostgreSQL的LIKE模式转换为TDengine兼容的正则表达式模式
 */
static void tdengine_deparse_string_like_pattern(StringInfo buf, const char *val, PatternMatchingOperator op_type)
{
	// 定义需要转义的正则表达式特殊字符
	const char *regex_special = "\\^$.|?*+()[{%";
	const char *ptr = val;

	// 添加正则表达式开始分隔符'/'
	appendStringInfoChar(buf, '/');

	// 处理大小写不敏感操作符(ILIKE)
	if (op_type == ILIKE_OPERATOR || op_type == NOT_ILIKE_OPERATOR)
		appendStringInfoString(buf, "(?i)");

	if (val[0] != '%')
		appendStringInfoChar(buf, '^');

	while (*ptr != '\0')
	{
		switch (*ptr)
		{
		case '%':
			appendStringInfoString(buf, "(.*)");
			break;
		case '_':
			appendStringInfoString(buf, "(.{1})");
			break;
		case '\\':
			ptr++; // 跳过反斜杠

			if (*ptr == '\0')
			{
				elog(ERROR, "invalid pattern matching");
			}
			else
			{
				add_backslash(buf, ptr, regex_special);
			}
			break;
		default:
			add_backslash(buf, ptr, regex_special);
			break;
		}

		ptr++; // 移动到下一个字符
	}

	if (tdengine_last_percent_sign_check(val))
		appendStringInfoChar(buf, '$');

	appendStringInfoChar(buf, '/');

	return;
}
/*
 * 将PostgreSQL的正则表达式模式转换为TDengine兼容的格式
 */
static void tdengine_deparse_string_regex_pattern(StringInfo buf, const char *val, PatternMatchingOperator op_type)
{
	// 添加正则表达式开始分隔符'/'
	appendStringInfoChar(buf, '/');

	// 处理大小写不敏感操作符
	if (op_type == REGEX_MATCH_CASE_INSENSITIVE_OPERATOR ||op_type == REGEX_NOT_MATCH_CASE_INSENSITIVE_OPERATOR)appendStringInfoString(buf, "(?i)");

	// 添加原始正则表达式内容
	appendStringInfoString(buf, val);

	// 添加正则表达式结束分隔符'/'
	appendStringInfoChar(buf, '/');
	return;
}

/*
 * 反解析填充选项值到字符串缓冲区
 */
static void tdengine_deparse_fill_option(StringInfo buf, const char *val)
{
	// 直接将填充选项值格式化为字符串添加到缓冲区
	appendStringInfo(buf, "%s", val);
}
/*
 * 将字符串转换为SQL字面量格式并追加到缓冲区
 */
void tdengine_deparse_string_literal(StringInfo buf, const char *val)
{
	const char *valptr;

	// 添加起始单引号
	appendStringInfoChar(buf, '\'');

	// 遍历字符串每个字符
	for (valptr = val; *valptr; valptr++)
	{
		char ch = *valptr;

		// 对需要转义的字符进行双重转义
		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoChar(buf, ch); // 添加转义字符

		// 添加字符本身
		appendStringInfoChar(buf, ch);
	}

	// 添加结束单引号
	appendStringInfoChar(buf, '\'');
}

/*
 * 反解析表达式主函数
 */
static void tdengine_deparse_expr(Expr *node, deparse_expr_cxt *context)
{
	// 保存外部上下文的状态标志
	bool outer_can_skip_cast = context->can_skip_cast;
	bool outer_convert_to_timestamp = context->convert_to_timestamp;

	// 空节点直接返回
	if (node == NULL)
		return;

	// 重置上下文标志
	context->can_skip_cast = false;
	context->convert_to_timestamp = false;

	// 根据节点类型分发处理
	switch (nodeTag(node))
	{
	case T_Var:
		context->convert_to_timestamp = outer_convert_to_timestamp;
		tdengine_deparse_var((Var *)node, context);
		break;
	case T_Const:
		context->convert_to_timestamp = outer_convert_to_timestamp;
		tdengine_deparse_const((Const *)node, context, 0);
		break;
	case T_Param:
		tdengine_deparse_param((Param *)node, context);
		break;
	case T_FuncExpr:
		context->can_skip_cast = outer_can_skip_cast;
		tdengine_deparse_func_expr((FuncExpr *)node, context);
		break;
	case T_OpExpr:
		context->convert_to_timestamp = outer_convert_to_timestamp;
		tdengine_deparse_op_expr((OpExpr *)node, context);
		break;
	case T_ScalarArrayOpExpr:
		tdengine_deparse_scalar_array_op_expr((ScalarArrayOpExpr *)node, context);
		break;
	case T_RelabelType:
		tdengine_deparse_relabel_type((RelabelType *)node, context);
		break;
	case T_BoolExpr:
		tdengine_deparse_bool_expr((BoolExpr *)node, context);
		break;
	case T_NullTest:
		tdengine_deparse_null_test((NullTest *)node, context);
		break;
	case T_ArrayExpr:
		tdengine_deparse_array_expr((ArrayExpr *)node, context);
		break;
	case T_Aggref:
		tdengine_deparse_aggref((Aggref *)node, context);
		break;
	case T_CoerceViaIO:
		tdengine_deparse_coerce_via_io((CoerceViaIO *)node, context);
		break;
	default:
		// 不支持的表达式类型报错
		elog(ERROR, "unsupported expression type for deparse: %d",
			 (int)nodeTag(node));
		break;
	}
}

/*
 * 反解析Var节点到context->buf缓冲区
 */
static void
tdengine_deparse_var(Var *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;			  // 输出缓冲区
	Relids relids = context->scanrel->relids; // 扫描关系ID集合

	if (bms_is_member(node->varno, relids) && node->varlevelsup == 0)
	{
		bool convert = context->has_bool_cmp;

		tdengine_deparse_column_ref(buf, node->varno, node->varattno,
									node->vartype, context->root,
									convert, &context->can_delete_directly);
	}
	else
	{
		/* 作为参数处理 */
		if (context->params_list) // 如果有参数列表
		{
			int pindex = 0;
			ListCell *lc;

			/* 在参数列表中查找当前Var的索引 */
			foreach (lc, *context->params_list)
			{
				pindex++;
				if (equal(node, (Node *)lfirst(lc)))
					break;
			}
			if (lc == NULL) // 如果不在列表中
			{
				pindex++;
				*context->params_list = lappend(*context->params_list, node);
			}
			tdengine_print_remote_param(pindex, node->vartype, node->vartypmod, context);
		}
		else
		{
			tdengine_print_remote_placeholder(node->vartype, node->vartypmod, context);
		}
	}
}

/*
 * 反解析常量值到输出缓冲区
 */
static void
tdengine_deparse_const(Const *node, deparse_expr_cxt *context, int showtype)
{
	StringInfo buf = context->buf; // 输出缓冲区
	Oid typoutput;				   // 类型输出函数OID
	bool typIsVarlena;			   // 是否为变长类型
	char *extval;				   // 转换后的字符串值
	char *type_name;			   // 类型名称

	// 处理NULL值
	if (node->constisnull)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}

	// 获取类型的输出函数信息
	getTypeOutputInfo(node->consttype,&typoutput, &typIsVarlena);

	// 根据不同类型进行特殊处理
	switch (node->consttype)
	{
	case INT2OID:
	case INT4OID:
	case INT8OID:
	case OIDOID:
	case FLOAT4OID:
	case FLOAT8OID:
	case NUMERICOID:
	{
		// 调用类型输出函数转换数值
		extval = OidOutputFunctionCall(typoutput, node->constvalue);

		if (strspn(extval, "0123456789+-eE.") == strlen(extval))
		{
			if (extval[0] == '+' || extval[0] == '-')
				appendStringInfo(buf, "(%s)", extval);
			else
				appendStringInfoString(buf, extval);
		}
		else
			appendStringInfo(buf, "'%s'", extval);
	}
	break;
	case BITOID:
	case VARBITOID:
		extval = OidOutputFunctionCall(typoutput, node->constvalue);
		appendStringInfo(buf, "B'%s'", extval);
		break;
	case BOOLOID:
		extval = OidOutputFunctionCall(typoutput, node->constvalue);
		if (strcmp(extval, "t") == 0)
			appendStringInfoString(buf, "true");
		else
			appendStringInfoString(buf, "false");
		break;

	case BYTEAOID:
		extval = OidOutputFunctionCall(typoutput, node->constvalue);
		appendStringInfo(buf, "X\'%s\'", extval + 2);
		break;
	case TIMESTAMPTZOID:
	{
		Datum datum;
		if (context->convert_to_timestamp)
		{
			// 转换为UTC时区
			datum = DirectFunctionCall2(timestamptz_zone, CStringGetTextDatum("UTC"), node->constvalue);
			// 获取TIMESTAMP类型的输出函数
			getTypeOutputInfo(TIMESTAMPOID, &typoutput, &typIsVarlena);
		}
		else
		{
			// 保持原样，不转换时区
			datum = node->constvalue;
			getTypeOutputInfo(TIMESTAMPTZOID, &typoutput, &typIsVarlena);
		}

		// 转换为字符串并添加单引号
		extval = OidOutputFunctionCall(typoutput, datum);
		appendStringInfo(buf, "'%s'", extval);
		break;
	}
	case INTERVALOID:
	{
		// 处理时间间隔类型
		Interval *interval = DatumGetIntervalP(node->constvalue);
		struct pg_itm tm;

		interval2itm(*interval, &tm);
		appendStringInfo(buf, "%dd%ldh%dm%ds%du", tm.tm_mday, tm.tm_hour,tm.tm_min, tm.tm_sec, tm.tm_usec
		);
		break;
	}
	default:
		// 处理其他未明确列出的数据类型
		extval = OidOutputFunctionCall(typoutput, node->constvalue);

		// 获取数据类型名称
		type_name = tdengine_get_data_type_name(node->consttype);
		if (strcmp(type_name, "tdengine_fill_enum") == 0)
		{
			tdengine_deparse_fill_option(buf, extval);
		}
		else if (context->op_type != UNKNOWN_OPERATOR)
		{
			// 根据操作符类型进行特殊处理
			switch (context->op_type)
			{
			case LIKE_OPERATOR:
			case NOT_LIKE_OPERATOR:
			case ILIKE_OPERATOR:
			case NOT_ILIKE_OPERATOR:
				// 将LIKE模式转换为正则表达式
				tdengine_deparse_string_like_pattern(buf, extval, context->op_type);
				break;
			case REGEX_MATCH_CASE_SENSITIVE_OPERATOR:
			case REGEX_NOT_MATCH_CASE_SENSITIVE_OPERATOR:
			case REGEX_MATCH_CASE_INSENSITIVE_OPERATOR:
			case REGEX_NOT_MATCH_CASE_INSENSITIVE_OPERATOR:
				// 处理正则表达式匹配
				tdengine_deparse_string_regex_pattern(buf, extval, context->op_type);
				break;
			default:
				elog(ERROR, "OPERATOR is not supported");
				break;
			}
		}
		else
		{
			// 默认处理：转换为字符串字面量
			tdengine_deparse_string_literal(buf, extval);
		}
		break;
	}
}

/*
 * 反解析Param节点到输出缓冲区
 */
static void tdengine_deparse_param(Param *node, deparse_expr_cxt *context)
{
	// 检查是否存在参数列表(实际查询场景)
	if (context->params_list)
	{
		int pindex = 0;
		ListCell *lc;

		/* 在参数列表中查找当前Param节点的索引 */
		foreach (lc, *context->params_list)
		{
			pindex++;
			if (equal(node, (Node *)lfirst(lc)))
				break;
		}

		// 如果参数不在列表中
		if (lc == NULL)
		{
			/* 添加到列表末尾并递增索引 */
			pindex++;
			*context->params_list = lappend(*context->params_list, node);
		}

		// 打印远程参数占位符(如$1)
		tdengine_print_remote_param(pindex, node->paramtype, node->paramtypmod, context);
	}
	else
	{
		// 无参数列表时打印通用占位符
		tdengine_print_remote_placeholder(node->paramtype, node->paramtypmod, context);
	}
}

// TODO:
/*
 * 将PostgreSQL函数名转换为TDengine对应的等效函数名
 */
char *
tdengine_replace_function(char *in)
{
	
}

/*
 * 反解析函数表达式(FuncExpr节点)
 */
static void tdengine_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; 
	char *proname;				   
	bool first;					   
	ListCell *arg;				   
	bool arg_swap = false;		   
	bool can_skip_cast = false;	   
	bool is_star_func = false;	   
	List *args = node->args;	   

	/* 获取函数名称 */
	proname = get_func_name(node->funcid);

	if (strcmp(proname, "tdengine_fill_numeric") == 0 ||
		strcmp(proname, "tdengine_fill_option") == 0)
	{
		Assert(list_length(args) == 1); // 确保fill函数只有一个参数


		if (context->is_tlist)
			return;

		buf->len = buf->len - 2;


		context->tdengine_fill_expr = node;
		return;
	}

	if (strcmp(proname, "tdengine_time") == 0)
	{
		int idx = 0; // 参数索引

		// 在SELECT目标列表中不反解析此函数
		if (context->is_tlist)
			return;

		appendStringInfo(buf, "time("); // 输出函数名开始
		first = true;
		foreach (arg, args)
		{
			if (idx == 0)
			{
				idx++;
				continue;
			}
			if (idx >= 2)
				appendStringInfoString(buf, ", "); // 参数分隔符

			tdengine_deparse_expr((Expr *)lfirst(arg), context);
			idx++;
		}
		appendStringInfoChar(buf, ')'); // 结束函数调用
		return;
	}

	if (context->can_skip_cast == true &&
		(strcmp(proname, "float8") == 0 || strcmp(proname, "numeric") == 0))
	{
		arg = list_head(args);
		context->can_skip_cast = false;						 // 重置标志
		tdengine_deparse_expr((Expr *)lfirst(arg), context); // 直接反解析参数
		return;
	}

	if (strcmp(proname, "log") == 0)
	{
		arg_swap = true; // 标记需要交换参数顺序
	}
	if (tdengine_is_unique_func(node->funcid, proname) ||
		tdengine_is_supported_builtin_func(node->funcid, proname))
		can_skip_cast = true; // 标记可以跳过类型转换

	is_star_func = tdengine_is_star_func(node->funcid, proname);
	/* 将PostgreSQL函数名转换为TDengine兼容的函数名 */
	proname = tdengine_replace_function(proname);

	appendStringInfo(buf, "%s(", proname);

	if (arg_swap && list_length(args) == 2)
	{
		args = list_make2(lfirst(list_tail(args)), lfirst(list_head(args)));
	}

	first = true; // 标记是否是第一个参数

	if (is_star_func)
	{
		appendStringInfoChar(buf, '*');
		first = false; // 已经处理了一个参数
	}

	// 遍历所有参数
	foreach (arg, args)
	{
		Expr *exp = (Expr *)lfirst(arg); // 获取当前参数表达式

		if (!first)
			appendStringInfoString(buf, ", ");

		if (IsA((Node *)exp, Const))
		{
			Const *arg = (Const *)exp;
			char *extval;

			if (arg->consttype == TEXTOID)
			{
				bool is_regex = tdengine_is_regex_argument(arg, &extval);

				if (is_regex == true)
				{
					appendStringInfo(buf, "%s", extval);
					first = false;
					continue; // 跳过后续处理
				}
			}
		}

		if (can_skip_cast)
			context->can_skip_cast = true;
		tdengine_deparse_expr((Expr *)exp, context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * 反解析操作符表达式(OpExpr节点)
 */
static void
tdengine_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;	   
	HeapTuple tuple;				   
	Form_pg_operator form;			   
	char oprkind;					   
	TDengineFdwRelationInfo *fpinfo =  
		(TDengineFdwRelationInfo *)(context->foreignrel->fdw_private);

	// 获取当前扫描关系的范围表条目
	RangeTblEntry *rte = planner_rt_fetch(context->scanrel->relid, context->root);

	/* 从系统目录获取操作符信息 */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator)GETSTRUCT(tuple);
	oprkind = form->oprkind; // 获取操作符类型(一元/二元)

	Assert((oprkind == 'l' && list_length(node->args) == 1) ||
		   (oprkind == 'b' && list_length(node->args) == 2));

	if (tdengine_is_slvar_fetch((Node *)node, &(fpinfo->slinfo)))
	{
		tdengine_deparse_slvar((Node *)node, linitial_node(Var, node->args),lsecond_node(Const, node->args), context);
		ReleaseSysCache(tuple);
		return;
	}

	if (oprkind == 'b' &&
		tdengine_contain_time_key_column(rte->relid, node->args))
	{
		context->convert_to_timestamp = true; // 标记需要转换为时间戳
	}

	{
		tdengine_deparse_expr(linitial(node->args), context);
		appendStringInfoChar(buf, ' '); // 操作数后添加空格
	}

	tdengine_deparse_operator_name(buf, form, &context->op_type);

	appendStringInfoChar(buf, ' ');

	tdengine_deparse_expr(llast(node->args), context);

	context->op_type = UNKNOWN_OPERATOR;

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}


/*
 * 反解析操作符名称
 */
static void
tdengine_deparse_operator_name(StringInfo buf, Form_pg_operator opform, PatternMatchingOperator *op_type)
{
	cur_opname = NameStr(opform->oprname);
	*op_type = UNKNOWN_OPERATOR; // 初始化操作符类型

	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
	{
		const char *opnspname;

		opnspname = get_namespace_name(opform->oprnamespace);
		appendStringInfo(buf, "OPERATOR(%s.%s)",
						 tdengine_quote_identifier(opnspname, QUOTE), cur_opname);
	}
	else
	{
		if (strcmp(cur_opname, "~~") == 0) // LIKE操作符
		{
			appendStringInfoString(buf, "=~");
			*op_type = LIKE_OPERATOR;
		}
		else if (strcmp(cur_opname, "!~~") == 0) // NOT LIKE操作符
		{
			appendStringInfoString(buf, "!~");
			*op_type = NOT_LIKE_OPERATOR;
		}
		else if (strcmp(cur_opname, "~~*") == 0) // ILIKE操作符
		{
			appendStringInfoString(buf, "=~");
			*op_type = ILIKE_OPERATOR;
		}
		else if (strcmp(cur_opname, "!~~*") == 0) // NOT ILIKE操作符
		{
			appendStringInfoString(buf, "!~");
			*op_type = NOT_ILIKE_OPERATOR;
		}
		else
		{
			appendStringInfoString(buf, cur_opname);
		}
	}
}

/*
 * 反解析ScalarArrayOpExpr表达式(数组操作表达式)
 */
static void
tdengine_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; 
	HeapTuple tuple;			   
	Expr *arg1;					   
	Expr *arg2;					   
	Form_pg_operator form;		   
	char *opname = NULL;		   
	Oid typoutput;				   
	bool typIsVarlena;			   

	/* 从系统目录获取操作符信息 */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator)GETSTRUCT(tuple);

	opname = pstrdup(NameStr(form->oprname));
	ReleaseSysCache(tuple);

	arg1 = linitial(node->args); // 第一个参数(左操作数)
	arg2 = lsecond(node->args);	 // 第二个参数(右操作数)

	/* 根据右操作数类型进行不同处理 */
	switch (nodeTag((Node *)arg2))
	{
	case T_Const: // 右操作数是常量数组
	{
		char *extval;			 
		Const *c;				 
		bool isstr;				 
		const char *valptr;		 
		int i = -1;				 
		bool deparseLeft = true; 
		bool inString = false;	 
		bool isEscape = false;	 

		c = (Const *)arg2;
		if (!c->constisnull) // 只处理非NULL常量
		{
			/* 获取类型的输出函数信息 */
			getTypeOutputInfo(c->consttype, &typoutput, &typIsVarlena);

			/* 调用输出函数将数组常量转换为字符串形式 */
			extval = OidOutputFunctionCall(typoutput, c->constvalue);

			/* 判断数组元素类型是否为字符串 */
			switch (c->consttype)
			{
			case BOOLARRAYOID:	 // 布尔数组
			case INT8ARRAYOID:	 // bigint数组
			case INT2ARRAYOID:	 // smallint数组
			case INT4ARRAYOID:	 // integer数组
			case OIDARRAYOID:	 // OID数组
				isstr = false;
				break;
			default: // 其他类型视为字符串数组
				isstr = true;
				break;
			}

			/* 遍历数组字符串，逐个处理数组元素 */
			for (valptr = extval; *valptr; valptr++)
			{
				char ch = *valptr; // 当前字符
				i++;

				if (deparseLeft)
				{
					if (c->consttype == BOOLARRAYOID) // 布尔数组特殊处理
					{
						if (arg1 != NULL && IsA(arg1, Var)) // 列引用
						{
							Var *var = (Var *)arg1;
							/* 反解析列引用，不进行类型转换 */
							tdengine_deparse_column_ref(buf, var->varno,var->varattno, var->vartype,context->root, false, false);
						}
						else if (arg1 != NULL && IsA(arg1, CoerceViaIO)) // 类型转换
						{
							bool has_bool_cmp = context->has_bool_cmp;
							context->has_bool_cmp = false;
							tdengine_deparse_expr(arg1, context);
							context->has_bool_cmp = has_bool_cmp;
						}
					}
					else // 非布尔数组
					{
						tdengine_deparse_expr(arg1, context);
					}

					/* 添加操作符和空格 */
					appendStringInfo(buf, " %s ", opname);

					/* 字符串类型数组需要添加引号 */
					if (isstr)
						appendStringInfoChar(buf, '\'');

					deparseLeft = false; // 标记左操作数已处理
				}

				/* 跳过数组的大括号 */
				if ((ch == '{' && i == 0) || (ch == '}' && (i == (strlen(extval) - 1))))
					continue;

				/* 处理字符串常量中的双引号 */
				if (ch == '\"' && !isEscape)
				{
					inString = !inString; // 切换字符串状态
					continue;
				}

				/* 处理字符串中的单引号(需要转义) */
				if (ch == '\'')
					appendStringInfoChar(buf, '\'');

				/* 处理转义字符 */
				if (ch == '\\' && !isEscape)
				{
					isEscape = true; // 标记遇到转义字符
					continue;
				}
				isEscape = false; // 重置转义标记

				/* 遇到逗号且不在字符串中，表示数组元素分隔符 */
				if (ch == ',' && !inString)
				{
					/* 如果是字符串类型，添加右引号 */
					if (isstr)
						appendStringInfoChar(buf, '\'');

					/* 根据IN/NOT IN添加连接符(OR/AND) */
					if (node->useOr)
						appendStringInfo(buf, " OR ");
					else
						appendStringInfo(buf, " AND ");

					deparseLeft = true; // 下一个元素需要重新输出左操作数
					continue;
				}

				/* 布尔数组特殊处理(true/false) */
				if (c->consttype == BOOLARRAYOID)
				{
					if (ch == 't')
						appendStringInfo(buf, "true");
					else
						appendStringInfo(buf, "false");
					continue;
				}

				/* 输出当前字符 */
				appendStringInfoChar(buf, ch);
			}

			if (isstr)
				appendStringInfoChar(buf, '\'');
		}
		break;
	}
	case T_ArrayExpr: // 右操作数是数组表达式(非常量)
	{
		bool first = true; // 是否是第一个元素
		ListCell *lc;	   // 数组元素列表迭代器

		foreach (lc, ((ArrayExpr *)arg2)->elements)
		{
			if (!first) // 非第一个元素需要添加连接符
			{
				if (node->useOr)
					appendStringInfoString(buf, " OR ");
				else
					appendStringInfoString(buf, " AND ");
			}

			appendStringInfoChar(buf, '(');
			tdengine_deparse_expr(arg1, context);

			appendStringInfo(buf, " %s ", opname);

			tdengine_deparse_expr(lfirst(lc), context);
			appendStringInfoChar(buf, ')');

			first = false; // 标记已处理第一个元素
		}
		break;
	}
	default:
		elog(ERROR, "unsupported expression type for deparse: %d",
			 (int)nodeTag(node));
		break;
	}
}

/*
 * 反解析RelabelType节点(二进制兼容的类型转换)
 */
static void
tdengine_deparse_relabel_type(RelabelType *node, deparse_expr_cxt *context)
{
	tdengine_deparse_expr(node->arg, context);
}

/*
 * 反解析布尔表达式(BoolExpr节点)
 */
static void
tdengine_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; 
	const char *op = NULL;		   
	bool first;					   
	ListCell *lc;				   

	/* 根据布尔操作类型处理 */
	switch (node->boolop)
	{
	case AND_EXPR: // AND表达式
		op = "AND";
		break;
	case OR_EXPR: // OR表达式
		op = "OR";
		break;
	case NOT_EXPR:											  
		appendStringInfoString(buf, "(NOT ");				  
		tdengine_deparse_expr(linitial(node->args), context); 
		appendStringInfoChar(buf, ')');						  
		return;												  
	}

	appendStringInfoChar(buf, '('); // 添加左括号
	first = true;					// 标记第一个参数
	foreach (lc, node->args)		// 遍历所有参数
	{
		if (!first) // 非第一个参数需要添加操作符
			appendStringInfo(buf, " %s ", op);
		tdengine_deparse_expr((Expr *)lfirst(lc), context); // 反解析参数
		first = false;										// 标记已处理第一个参数
	}
	appendStringInfoChar(buf, ')'); // 添加右括号
}

/*
 * 反解析NULL测试表达式(NullTest节点)
 */
static void
tdengine_deparse_null_test(NullTest *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 获取输出缓冲区

	appendStringInfoChar(buf, '(');			   // 添加左括号
	tdengine_deparse_expr(node->arg, context); // 反解析测试参数

	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(buf, " = '')"); // IS NULL → = ''
	else
		appendStringInfoString(buf, " <> '')"); // IS NOT NULL → <> ''
}

/*
 * 反解析数组表达式(ArrayExpr节点)
 */
static void
tdengine_deparse_array_expr(ArrayExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 获取输出缓冲区
	bool first = true;			   // 标记是否是第一个元素
	ListCell *lc;				   // 数组元素列表迭代器

	appendStringInfoString(buf, "ARRAY[");

	foreach (lc, node->elements)
	{
		// 非第一个元素需要添加逗号分隔符
		if (!first)
			appendStringInfoString(buf, ", ");

		// 递归反解析当前数组元素
		tdengine_deparse_expr(lfirst(lc), context);
		first = false; // 标记已处理第一个元素
	}

	appendStringInfoChar(buf, ']');
}

/*
 * tdengine_print_remote_param - 打印远程参数
 */
static void tdengine_print_remote_param(int paramindex, Oid paramtype, int32 paramtypmod,deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 获取输出缓冲区

	appendStringInfo(buf, "$%d", paramindex);
}
/*
 * tdengine_print_remote_placeholder - 生成远程参数占位符(用于EXPLAIN场景)
 */
static void
tdengine_print_remote_placeholder(Oid paramtype, int32 paramtypmod,
								  deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 获取输出缓冲区
	appendStringInfo(buf, "(SELECT null)");
}

/*
 * 检查给定的OID是否属于PostgreSQL内置对象
 */
bool tdengine_is_builtin(Oid oid)
{
	return (oid < FirstGenbkiObjectId);

}

/*
 * 检查常量节点是否为正则表达式参数(以'/'开头和结尾)
 */
bool tdengine_is_regex_argument(Const *node, char **extval)
{
	Oid typoutput;	   
	bool typIsVarlena; 
	const char *first; 
	const char *last;  

	getTypeOutputInfo(node->consttype,&typoutput, &typIsVarlena);

	/* 调用输出函数将常量值转换为字符串 */
	(*extval) = OidOutputFunctionCall(typoutput, node->constvalue);
	first = *extval;
	last = *extval + strlen(*extval) - 1;

	/* 检查字符串是否以'/'开头和结尾 */
	if (*first == '/' && *last == '/')
		return true;
	else
		return false;
}

/*
 * 检查函数是否为星号函数(需要添加*作为第一个参数)
 */
bool tdengine_is_star_func(Oid funcid, char *in)
{
	char *eof = "_all"; /* 函数名后缀 */
	size_t func_len = strlen(in);
	size_t eof_len = strlen(eof);

	if (tdengine_is_builtin(funcid))
		return false;

	if (func_len > eof_len && strcmp(in + func_len - eof_len, eof) == 0 &&
		exist_in_function_list(in, TDengineStableStarFunction))
		return true;

	return false;
}
/*
 * 检查函数是否为唯一函数
 */
static bool
tdengine_is_unique_func(Oid funcid, char *in)
{
	if (tdengine_is_builtin(funcid))
		return false;

	if (exist_in_function_list(in, TDengineUniqueFunction))
		return true;

	return false;
}
/*
 * 检查函数是否为支持的TDengine内置函数
 */
static bool
tdengine_is_supported_builtin_func(Oid funcid, char *in)
{
	if (!tdengine_is_builtin(funcid))
		return false;

	if (exist_in_function_list(in, TDengineSupportedBuiltinFunction))
		return true;

	return false;
}

/*
 * 反解析聚合函数节点(Aggref)
 */
static void
tdengine_deparse_aggref(Aggref *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 输出缓冲区
	bool use_variadic;			   // 是否使用VARIADIC修饰符
	char *func_name;			   // 函数名称
	bool is_star_func;			   // 是否是星号函数

	Assert(node->aggsplit == AGGSPLIT_SIMPLE);

	use_variadic = node->aggvariadic;

	func_name = get_func_name(node->aggfnoid);

	if (!node->aggstar)
	{
		if ((strcmp(func_name, "last") == 0 || strcmp(func_name, "first") == 0) &&
			list_length(node->args) == 2)
		{
			appendStringInfo(buf, "%s(", func_name);
			tdengine_deparse_expr((Expr *)(((TargetEntry *)list_nth(node->args, 1))->expr), context);
			appendStringInfoChar(buf, ')');
			return;
		}
	}

	is_star_func = tdengine_is_star_func(node->aggfnoid, func_name);
	func_name = tdengine_replace_function(func_name);
	appendStringInfo(buf, "%s", func_name);

	appendStringInfoChar(buf, '(');

	appendStringInfo(buf, "%s", (node->aggdistinct != NIL) ? "DISTINCT " : "");

	if (node->aggstar)
		appendStringInfoChar(buf, '*');
	else
	{
		ListCell *arg;	   // 参数列表迭代器
		bool first = true; // 是否是第一个参数

		/* 如果是星号函数，添加*作为第一个参数 */
		if (is_star_func)
		{
			appendStringInfoChar(buf, '*');
			first = false;
		}

		/* 遍历所有参数 */
		foreach (arg, node->args)
		{
			TargetEntry *tle = (TargetEntry *)lfirst(arg);
			Node *n = (Node *)tle->expr;

			/* 处理正则表达式参数 */
			if (IsA(n, Const))
			{
				Const *arg = (Const *)n;
				char *extval;

				if (arg->consttype == TEXTOID)
				{
					bool is_regex = tdengine_is_regex_argument(arg, &extval);

					/* 如果是正则表达式，直接输出 */
					if (is_regex == true)
					{
						appendStringInfo(buf, "%s", extval);
						first = false;
						continue;
					}
				}
			}

			/* 跳过标记为resjunk的参数 */
			if (tle->resjunk)
				continue;

			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			if (use_variadic && lnext(node->args, arg) == NULL)
				appendStringInfoString(buf, "VARIADIC ");

			/* 反解析参数表达式 */
			tdengine_deparse_expr((Expr *)n, context);
		}
	}

	// 添加右括号
	appendStringInfoChar(buf, ')');
}

/*
 * 反解析GROUP BY子句
 */
static void
tdengine_append_group_by_clause(List *tlist, deparse_expr_cxt *context)
{
	// 获取输出缓冲区和查询树
	StringInfo buf = context->buf;		 // 字符串输出缓冲区
	Query *query = context->root->parse; // 查询解析树
	ListCell *lc;						 // 列表迭代器
	bool first = true;					 // 标记是否是第一个分组项

	/* 检查查询是否有GROUP BY子句，没有则直接返回 */
	if (!query->groupClause)
		return;

	appendStringInfo(buf, " GROUP BY ");
	Assert(!query->groupingSets);

	context->tdengine_fill_expr = NULL;
	foreach (lc, query->groupClause)
	{
		SortGroupClause *grp = (SortGroupClause *)lfirst(lc); // 获取当前分组项

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false; // 标记已处理第一个分组项

		tdengine_deparse_sort_group_clause(grp->tleSortGroupRef, tlist, context);
	}

	if (context->tdengine_fill_expr)
	{
		ListCell *arg; // 参数列表迭代器

		appendStringInfo(buf, " fill(");

		foreach (arg, context->tdengine_fill_expr->args)
		{
			tdengine_deparse_expr((Expr *)lfirst(arg), context);
		}

		/* 添加右括号 */
		appendStringInfoChar(buf, ')');
	}
}
/*
 * 反解析LIMIT/OFFSET子句
 */
static void
tdengine_append_limit_clause(deparse_expr_cxt *context)
{
	PlannerInfo *root = context->root; // 查询规划信息
	StringInfo buf = context->buf;	   // 输出缓冲区


	/* 处理LIMIT子句(如果存在) */
	if (root->parse->limitCount)
	{
		appendStringInfoString(buf, " LIMIT ");							 // 添加LIMIT关键字
		tdengine_deparse_expr((Expr *)root->parse->limitCount, context); // 反解析LIMIT值
	}

	/* 处理OFFSET子句(如果存在) */
	if (root->parse->limitOffset)
	{
		appendStringInfoString(buf, " OFFSET ");						  // 添加OFFSET关键字
		tdengine_deparse_expr((Expr *)root->parse->limitOffset, context); // 反解析OFFSET值
	}
}
/*
 * 查找等价类中完全来自指定关系的成员表达式
 */
static Expr *tdengine_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell *lc_em; // 等价成员列表迭代器

	// 遍历等价类的所有成员
	foreach (lc_em, ec->ec_members)
	{
		EquivalenceMember *em = lfirst(lc_em); // 获取当前等价成员

		if (bms_is_subset(em->em_relids, rel->relids))
		{
			return em->em_expr;
		}
	}

	/* 没有找到符合条件的等价类成员表达式 */
	return NULL;
}
/*
 * 反解析ORDER BY子句
 */
static void
tdengine_append_order_by_clause(List *pathkeys, deparse_expr_cxt *context)
{
	ListCell *lcell;						
	char *delim = " ";						
	RelOptInfo *baserel = context->scanrel; 
	StringInfo buf = context->buf;			

	/* 添加ORDER BY关键字 */
	appendStringInfo(buf, " ORDER BY");

	foreach (lcell, pathkeys)
	{
		PathKey *pathkey = lfirst(lcell); // 当前路径键
		Expr *em_expr;					  // 等价成员表达式

		em_expr = tdengine_find_em_expr_for_rel(pathkey->pk_eclass, baserel);
		Assert(em_expr != NULL); // 必须找到有效表达式

		appendStringInfoString(buf, delim);
		tdengine_deparse_expr(em_expr, context);

		if (pathkey->pk_strategy == BTLessStrategyNumber)
			appendStringInfoString(buf, " ASC"); // 升序
		else
			appendStringInfoString(buf, " DESC"); // 降序

		if (pathkey->pk_nulls_first)
			elog(ERROR, "NULLS FIRST not supported");

		delim = ", ";
	}
}

/*
 * 反解析排序或分组子句
 */
static Node *
tdengine_deparse_sort_group_clause(Index ref, List *tlist, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 输出缓冲区
	TargetEntry *tle;			   // 目标条目
	Expr *expr;					   // 表达式节点

	tle = get_sortgroupref_tle(ref, tlist);
	expr = tle->expr;

	if (expr && IsA(expr, Const))
	{
		tdengine_deparse_const((Const *)expr, context, 1);
	}
	else if (!expr || IsA(expr, Var))
	{
		tdengine_deparse_expr(expr, context);
	}
	else
	{
		appendStringInfoString(buf, "(");
		tdengine_deparse_expr(expr, context);
		appendStringInfoString(buf, ")");
	}

	return (Node *)expr; // 返回表达式树节点
}

/*
 * tdengine_get_data_type_name: 根据数据类型OID获取类型名称
 */
char *tdengine_get_data_type_name(Oid data_type_id)
{
	HeapTuple tuple;   /* 系统缓存元组 */
	Form_pg_type type; /* 类型信息结构体 */
	char *type_name;   /* 返回的类型名称 */

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(data_type_id));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for data type id %u", data_type_id);

	type_name = pstrdup(type->typname.data);

	ReleaseSysCache(tuple);
	return type_name;
}

/*
 * 检查表达式列表中是否包含时间列
 */
static bool tdengine_contain_time_column(List *exprs, schemaless_info *pslinfo)
{
	ListCell *lc;

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);

		if (IsA(expr, Var))
		{
			Var *var = (Var *)expr;

			if (TDENGINE_IS_TIME_TYPE(var->vartype))
			{
				return true;
			}
		}
		else if (IsA(expr, CoerceViaIO))
		{
			CoerceViaIO *cio = (CoerceViaIO *)expr;
			Node *arg = (Node *)cio->arg;

			if (tdengine_contain_time_key_column(arg, pslinfo))
			{
				if (TDENGINE_IS_TIME_TYPE(cio->resulttype))
				{
					return true;
				}
			}
		}
	}
	return false;
}
/*
 * 检查表达式列表中是否包含时间键列
 */
static bool
tdengine_contain_time_key_column(Oid relid, List *exprs)
{
	ListCell *lc;
	/* 遍历表达式列表 */
	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		Var *var;

		if (!IsA(expr, Var))
			continue;

		var = (Var *)expr;

		if (TDENGINE_IS_TIME_TYPE(var->vartype))
		{
			char *column_name = tdengine_get_column_name(relid, var->varattno);

			if (TDENGINE_IS_TIME_COLUMN(column_name))
				return true;
		}
	}

	return false;
}
/*
 * 检查表达式列表中是否包含时间表达式(排除Var/Const/Param/FuncExpr类型节点)
 */
static bool tdengine_contain_time_expr(List *exprs)
{
	ListCell *lc;

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		Oid type;

		if (IsA(expr, Var) ||
			IsA(expr, Const) ||
			IsA(expr, Param) ||
			IsA(expr, FuncExpr))
		{
			continue;
		}

		type = exprType((Node *)expr);

		if (TDENGINE_IS_TIME_TYPE(type))
		{
			return true;
		}
	}
	return false;
}
/*
 * 检查表达式列表中是否包含时间函数
 */
static bool
tdengine_contain_time_function(List *exprs)
{
	ListCell *lc;

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		FuncExpr *func_expr;

		if (!IsA(expr, FuncExpr))
			continue;

		func_expr = (FuncExpr *)expr;

		if (TDENGINE_IS_TIME_TYPE(func_expr->funcresulttype))
		{
			return true;
		}
	}
	return false;
}
/*
 * 检查表达式列表中是否包含时间类型的参数节点(Param)
 */
static bool
tdengine_contain_time_param(List *exprs)
{
	ListCell *lc;

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		Oid type;

		if (!IsA(expr, Param))
			continue;

		type = exprType((Node *)expr);

		if (TDENGINE_IS_TIME_TYPE(type))
		{
			return true;
		}
	}

	return false;
}
/*
 * 检查表达式列表中是否包含时间类型的常量节点(Const)
 */
static bool
tdengine_contain_time_const(List *exprs)
{
	ListCell *lc;

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		Oid type;

		if (!IsA(expr, Const))
			continue;

		type = exprType((Node *)expr);

		if (TDENGINE_IS_TIME_TYPE(type))
		{
			return true;
		}
	}

	return false;
}

/*
 * tdengine_is_grouping_target: 检查给定的目标条目(TargetEntry)是否是GROUP BY子句的分组目标
 */
bool tdengine_is_grouping_target(TargetEntry *tle, Query *query)
{
	ListCell *lc;

	if (!query->groupClause)
		return false;

	foreach (lc, query->groupClause)
	{
		SortGroupClause *grp = (SortGroupClause *)lfirst(lc);
		/* 检查当前分组项是否与目标条目匹配 */
		if (grp->tleSortGroupRef == tle->ressortgroupref)
		{
			return true;
		}
	}

	/* 没有找到匹配的分组项 */
	return false;
}

/*
 * tdengine_append_field_key - 向缓冲区添加第一个找到的字段键(field key)
 */
void tdengine_append_field_key(TupleDesc tupdesc, StringInfo buf, Index rtindex, PlannerInfo *root, bool first)
{
	int i;

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
		RangeTblEntry *rte = planner_rt_fetch(rtindex, root);
		char *name = tdengine_get_column_name(rte->relid, i);

		if (attr->attisdropped)
			continue;

		if (!TDENGINE_IS_TIME_COLUMN(name) && !tdengine_is_tag_key(name, rte->relid))
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			tdengine_deparse_column_ref(buf, rtindex, i, -1, root, false, false);
			return;
		}
	}
}

/*
 * 获取外表对应的远程表名
 */
char *tdengine_get_table_name(Relation rel)
{
	ForeignTable *table;
	char *relname = NULL;
	ListCell *lc = NULL;

	/* 获取外表元数据 */
	table = GetForeignTable(RelationGetRelid(rel));

	foreach (lc, table->options)
	{
		DefElem *def = (DefElem *)lfirst(lc);

		if (strcmp(def->defname, "table") == 0)
			relname = defGetString(def);
	}

	if (relname == NULL)
		relname = RelationGetRelationName(rel);

	return relname;
}

/*
 * 获取外表中指定列的列名
 */
char *
tdengine_get_column_name(Oid relid, int attnum)
{
	List *options = NULL;
	ListCell *lc_opt;
	char *colname = NULL;

	options = GetForeignColumnOptions(relid, attnum);

	foreach (lc_opt, options)
	{
		DefElem *def = (DefElem *)lfirst(lc_opt);

		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	if (colname == NULL)
		colname = get_attname(relid, attnum,false);
	return colname;
}

/*
 * 检查指定列是否为标签键(tag key)
 */
bool tdengine_is_tag_key(const char *colname, Oid reloid)
{
	tdengine_opt *options;
	ListCell *lc;

	options = tdengine_get_options(reloid, GetUserId());

	if (!options->tags_list)
		return false;

	foreach (lc, options->tags_list)
	{
		char *name = (char *)lfirst(lc);

		if (strcmp(colname, name) == 0)
			return true;
	}

	return false;
}

/*****************************************************************************
 *		函数相关子句检查
 *****************************************************************************/

/*
 * tdengine_contain_functions_walker - 递归检查子句中是否包含函数调用
 */
static bool
tdengine_contain_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (nodeTag(node) == T_FuncExpr)
	{
		return true; /* 发现函数调用 */
	}

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *)node,
								 tdengine_contain_functions_walker,
								 context, 0);
	}

	return expression_tree_walker(node,
								  tdengine_contain_functions_walker,
								  context);
}

/*
 * tdengine_is_foreign_function_tlist - 检查目标列表是否可以在远程服务器上安全执行
 */
bool tdengine_is_foreign_function_tlist(PlannerInfo *root,
										RelOptInfo *baserel,
										List *tlist)
{
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)(baserel->fdw_private);
	ListCell *lc;
	bool is_contain_function;
	bool have_slvar_fields = false;
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;

	if (!(baserel->reloptkind == RELOPT_BASEREL ||
		  baserel->reloptkind == RELOPT_OTHER_MEMBER_REL))
		return false;

	is_contain_function = false;
	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (tdengine_contain_functions_walker((Node *)tle->expr, NULL))
		{
			is_contain_function = true;
			break;
		}
	}

	if (!is_contain_function)
		return false;

	loc_cxt.have_otherfunc_tdengine_time_tlist = false;

	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		/* 设置全局上下文 */
		glob_cxt.root = root;
		glob_cxt.foreignrel = baserel;
		glob_cxt.relid = fpinfo->table->relid;
		glob_cxt.mixing_aggref_status = TDENGINE_TARGETS_MIXING_AGGREF_SAFE;
		glob_cxt.for_tlist = true;
		glob_cxt.is_inner_func = false;

		if (baserel->reloptkind == RELOPT_UPPER_REL)
			glob_cxt.relids = fpinfo->outerrel->relids;
		else
			glob_cxt.relids = baserel->relids;

		loc_cxt.collation = InvalidOid;
		loc_cxt.state = FDW_COLLATE_NONE;
		loc_cxt.can_skip_cast = false;
		loc_cxt.can_pushdown_stable = false;
		loc_cxt.can_pushdown_volatile = false;
		loc_cxt.tdengine_fill_enable = false;
		loc_cxt.has_time_key = false;
		loc_cxt.has_sub_or_add_operator = false;

		/* 递归检查表达式树 */
		if (!tdengine_foreign_expr_walker((Node *)tle->expr, &glob_cxt, &loc_cxt))
			return false;

		if (list_length(tlist) > 1 && loc_cxt.can_pushdown_stable)
		{
			elog(WARNING, "Selecting multiple functions with regular expression or star. The query are not pushed down.");
			return false;
		}

		if (loc_cxt.state == FDW_COLLATE_UNSAFE)
			return false;

		if (!IsA(tle->expr, FieldSelect))
		{
			if (!loc_cxt.can_pushdown_volatile)
			{
				if (loc_cxt.can_pushdown_stable)
				{
					if (contain_volatile_functions((Node *)tle->expr))
						return false;
				}
				else
				{
					if (contain_mutable_functions((Node *)tle->expr))
						return false;
				}
			}
		}

		/* 检查变量节点是否为无模式字段键 */
		if (IsA(tle->expr, Var))
		{
			Var *var = (Var *)tle->expr;
			bool is_field_key = false;

			if (tdengine_is_slvar(var->vartype, var->varattno, &fpinfo->slinfo, NULL, &is_field_key) && is_field_key)
			{
				have_slvar_fields = true;
			}
		}
	}
	if (have_slvar_fields)
	{
		if (loc_cxt.have_otherfunc_tdengine_time_tlist)
		{
			return false;
		}
		fpinfo->all_fieldtag = true;
	}

	/* 目标列表中的函数可以安全地在远程服务器上执行 */
	return true;
}

/*
 * 检查节点是否为字符串类型
 */
static bool
tdengine_is_string_type(Node *node, schemaless_info *pslinfo)
{
	Oid oidtype = 0;

	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *)node;
		oidtype = var->vartype;
	}
	else if (IsA(node, Const))
	{
		Const *c = (Const *)node;
		oidtype = c->consttype;
	}
	else if (IsA(node, OpExpr))
	{
		OpExpr *oe = (OpExpr *)node;

		if (tdengine_is_slvar_fetch(node, pslinfo))
		{
			oidtype = oe->opresulttype;
		}
		else
			return expression_tree_walker(node, tdengine_is_string_type, pslinfo);
	}
	else if (IsA(node, CoerceViaIO))
	{
		CoerceViaIO *cio = (CoerceViaIO *)node;
		Node *arg = (Node *)cio->arg;

		if (tdengine_is_slvar_fetch(arg, pslinfo))
		{
			oidtype = cio->resulttype;
		}
		else
			return expression_tree_walker(node, tdengine_is_string_type, pslinfo);
	}
	else
	{
		return expression_tree_walker(node, tdengine_is_string_type, pslinfo);
	}

	switch (oidtype)
	{
	case CHAROID:
	case VARCHAROID:
	case TEXTOID:
	case BPCHAROID:
	case NAMEOID:
		return true;
	default:
		return false;
	}
}

/*
 * 检查函数名是否存在于给定的函数列表中
 */
static bool
exist_in_function_list(char *funcname, const char **funclist)
{
	int i;

	for (i = 0; funclist[i]; i++)
	{
		if (strcmp(funcname, funclist[i]) == 0)
			return true;
	}
	return false;
}

/*
 * tdengine_is_select_all: 检查是否为全表查询(select *)
 */
bool tdengine_is_select_all(RangeTblEntry *rte, List *tlist, schemaless_info *pslinfo)
{
	int i;
	int natts = 0;
	int natts_valid = 0;
	Relation rel = table_open(rte->relid, NoLock);
	TupleDesc tupdesc = RelationGetDescr(rel);
	Oid rel_type_id;
	bool has_rel_type_id = false;
	bool has_slcol = false;
	bool has_wholerow = false;

	/* 打开表并获取元组描述符 */
	Relation rel = table_open(rte->relid, NoLock);
	TupleDesc tupdesc = RelationGetDescr(rel);

	rel_type_id = get_rel_type_id(rte->relid);

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
		ListCell *lc;

		if (attr->attisdropped)
			continue;

		foreach (lc, tlist)
		{
			Node *node = (Node *)lfirst(lc);

			if (IsA(node, TargetEntry))
				node = (Node *)((TargetEntry *)node)->expr;

			if (IsA(node, Var))
			{
				Var *var = (Var *)node;
				if (var->vartype == rel_type_id)
				{
					has_rel_type_id = true;
					break;
				}
				if (var->varattno == 0)
				{
					has_wholerow = true;
					break;
				}
				/* 检查无模式类型变量 */
				if (tdengine_is_slvar(var->vartype, var->varattno, pslinfo, NULL, NULL))
				{
					has_slcol = true;
					break;
				}
				if (var->varattno == attr->attnum)
				{
					natts++;
					break;
				}
			}
		}
	}

	/* 关闭表并返回结果 */
	table_close(rel, NoLock);
	return ((natts == natts_valid) || has_rel_type_id || has_slcol || has_wholerow);
}

/*
 * 检查无模式表中是否没有字段键(field key)
 */
static bool tdengine_is_no_field_key(Oid reloid, List *slcols)
{
	int i;
	bool no_field_key = true; // 初始假设没有字段键

	/* 遍历无模式列列表 */
	for (i = 1; i <= list_length(slcols); i++)
	{
		StringInfo *rcol = (StringInfo *)list_nth(slcols, i - 1); // 获取当前列
		char *colname = strVal(rcol);							  // 获取列名

		if (!TDENGINE_IS_TIME_COLUMN(colname))
		{
			/* 检查是否为标签键列 */
			if (!tdengine_is_tag_key(colname, reloid))
			{
				/* 发现字段键列，设置标志并终止循环 */
				no_field_key = false;
				break;
			}
		}
	}

	return no_field_key;
}
/*
 * 反解析无模式表的目标列表
 */
static void tdengine_deparse_target_list_schemaless(StringInfo buf,Relation rel,Oid reloid,Bitmapset *attrs_used,List **retrieved_attrs,bool all_fieldtag,List *slcols)
{
	TupleDesc tupdesc = RelationGetDescr(rel); // 获取表元组描述符
	bool first;								   // 是否是第一个列
	int i;									   // 循环计数器
	bool no_field_key;						   // 是否没有字段键

	// 检查是否没有字段键(只有标签键和时间列)
	no_field_key = tdengine_is_no_field_key(reloid, slcols);

	// 初始化返回的属性索引列表
	*retrieved_attrs = NIL;

	// 遍历表的所有属性
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* 跳过已删除的属性 */
		if (attr->attisdropped)
			continue;

		/* 如果选择所有字段标签或没有字段键或属性被使用，则添加到返回列表 */
		if (all_fieldtag || no_field_key ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
	}

	/* 如果选择所有字段标签或没有字段键，直接输出"*"并返回 */
	if (all_fieldtag || no_field_key)
	{
		appendStringInfoString(buf, "*");
		return;
	}

	/* 遍历无模式列列表 */
	first = true;
	for (i = 1; i <= list_length(slcols); i++)
	{
		StringInfo *rcol = (StringInfo *)list_nth(slcols, i - 1);
		char *colname = strVal(rcol);

		/* 跳过时间列 */
		if (!TDENGINE_IS_TIME_COLUMN(colname))
		{
			/* 非第一个列时添加逗号分隔符 */
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;
			/* 添加带引号的列名 */
			appendStringInfoString(buf, tdengine_quote_identifier(colname, QUOTE));
		}
	}
}

/*
 * 反解析CoerceViaIO类型转换节点
 */
static void tdengine_deparse_coerce_via_io(CoerceViaIO *cio, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 输出缓冲区
	TDengineFdwRelationInfo *fpinfo =
		(TDengineFdwRelationInfo *)(context->foreignrel->fdw_private); // 获取FDW关系信息
	OpExpr *oe = (OpExpr *)cio->arg;								   // 获取转换参数表达式

	// 确保是无模式表查询
	Assert(fpinfo->slinfo.schemaless);

	/* 检查是否为无模式变量 */
	if (tdengine_is_slvar_fetch((Node *)oe, &(fpinfo->slinfo)))
	{
		tdengine_deparse_slvar((Node *)cio,linitial_node(Var, oe->args),  lsecond_node(Const, oe->args), context);
	}
	/* 检查是否为参数节点 */
	else if (tdengine_is_param_fetch((Node *)oe, &(fpinfo->slinfo)))
	{
		tdengine_deparse_param((Param *)cio, context);
	}

	if (cio->resulttype == BOOLOID && context->has_bool_cmp)
	{
		appendStringInfoString(buf, " = true"); // 添加布尔比较
	}
}

/*
 * 反解析无模式变量(tdengine_tags/tdengine_fields)表达式
 */
static void tdengine_deparse_slvar(Node *node, Var *var, Const *cnst, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;			  // 输出缓冲区
	Relids relids = context->scanrel->relids; // 当前扫描关系ID集合

	if (bms_is_member(var->varno, relids) && var->varlevelsup == 0)
	{
		appendStringInfo(buf, "%s", tdengine_quote_identifier(TextDatumGetCString(cnst->constvalue), QUOTE));
	}
	else
	{
		if (context->params_list)
		{
			int pindex = 0; // 参数索引
			ListCell *lc;	// 列表迭代器

			/* 在参数列表中查找当前节点的索引 */
			foreach (lc, *context->params_list)
			{
				pindex++;
				if (equal(node, (Node *)lfirst(lc)))
					break;
			}
			if (lc == NULL)
			{
				pindex++;
				*context->params_list = lappend(*context->params_list, node);
			}
			tdengine_print_remote_param(pindex, var->vartype, var->vartypmod, context);
		}
		else
		{
			tdengine_print_remote_placeholder(var->vartype, var->vartypmod, context);
		}
	}
}
