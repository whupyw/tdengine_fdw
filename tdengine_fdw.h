#include "query_cxx.h"

#include "foreign/foreign.h"
#include "lib/stringinfo.h"

#include "nodes/pathnodes.h"
#include "utils/float.h"
#include "optimizer/optimizer.h"
#include "access/table.h"
#include "fmgr.h"

#include "utils/rel.h"

/* 等待超时时间设置(毫秒)，0表示无限等待 */
#define WAIT_TIMEOUT 0
/* 交互式查询超时时间设置(毫秒)，0表示不超时 */
#define INTERACTIVE_TIMEOUT 0

#define TDENGINE_TIME_COLUMN "time"
#define TDENGINE_TIME_TEXT_COLUMN "time_text"
#define TDENGINE_TAGS_COLUMN "tags"
#define TDENGINE_FIELDS_COLUMN "fields"

#define TDENGINE_TAGS_PGTYPE "jsonb"
#define TDENGINE_FIELDS_PGTYPE "jsonb"

#define TDENGINE_IS_TIME_COLUMN(X) (strcmp(X, TDENGINE_TIME_COLUMN) == 0 || \
                                    strcmp(X, TDENGINE_TIME_TEXT_COLUMN) == 0)
/* 判断类型是否为时间类型 */
#define TDENGINE_IS_TIME_TYPE(typeoid) ((typeoid == TIMESTAMPTZOID) || \
                                        (typeoid == TIMEOID) ||        \
                                        (typeoid == TIMESTAMPOID))
#define TDENGINE_IS_JSONB_TYPE(typeoid) (typeoid == JSONBOID)
/* 无错误返回码定义 */
#define CR_NO_ERROR 0

/*
 * 宏定义：用于检查目标列表中聚合函数和非聚合函数的混合情况
 */
#define TDENGINE_TARGETS_MARK_COLUMN (1u << 0)
#define TDENGINE_TARGETS_MARK_AGGREF (1u << 1)
#define TDENGINE_TARGETS_MIXING_AGGREF_UNSAFE (TDENGINE_TARGETS_MARK_COLUMN | TDENGINE_TARGETS_MARK_AGGREF)
#define TDENGINE_TARGETS_MIXING_AGGREF_SAFE (0u)

#define CODE_VERSION 20200

typedef struct schemaless_info
{
    bool schemaless;    /* 启用无模式 */
    Oid slcol_type_oid; /* 无模式列的 jsonb 类型的对象标识符（OID） */
    Oid jsonb_op_oid;   /* jsonb 类型 "->>" 箭头操作符的对象标识符（OID） */

    Oid relid; /* 关系的对象标识符（OID） */
} schemaless_info;

/*
 * 用于存储 TDengine 服务器信息
 * TODO: 支持超级表
 */
typedef struct tdengine_opt
{
    char *driver;       
    char *protocol;     
    char *svr_database; 
    char *svr_table;    
    char *svr_address;  
    int svr_port;       
    char *svr_username; 
    char *svr_password; 
    List *tags_list;    
    int schemaless;     
} tdengine_opt;

typedef struct TDengineFdwRelationInfo
{
    /*
     * 为 true 表示该关系可以下推。对于简单的外部扫描，此值始终为 true。
     */
    bool pushdown_safe;

    List *remote_conds;
    List *local_conds;

    List *final_remote_exprs;

    Bitmapset *attrs_used;

    /* 为 true 表示 query_pathkeys 可以安全下推 */
    bool qp_is_pushdown_safe;

    QualCost local_conds_cost;
    Selectivity local_conds_sel;

    /* 连接条件的选择性 */
    Selectivity joinclause_sel;

    /*
     * 关系的索引
     */
    int relation_index;

    /* 目标列表中的函数下推支持 */
    bool is_tlist_func_pushdown;

    /* 为 true 表示目标列表中除了时间列之外的所有列 */
    bool all_fieldtag;
    /* 无模式信息 */
    schemaless_info slinfo;
    /* JsonB 列列表 */
    List *slcols;

    /*
     * 关系的名称
     */
    char *relation_name;

    /* 连接信息 */
    RelOptInfo *outerrel;
    RelOptInfo *innerrel;
    JoinType jointype;
    /* joinclauses 仅包含外部连接的 JOIN/ON 条件 */
    List *joinclauses; /* RestrictInfo 列表 */

    /* 上层关系信息 */
    UpperRelationKind stage;

    /* 分组信息 */
    List *grouped_tlist;

    /* 子查询信息 */
    bool make_outerrel_subquery; /* 我们是否将外部关系解析为子查询？ */
    bool make_innerrel_subquery; /* 我们是否将内部关系解析为子查询？ */
    Relids lower_subquery_rels;  /* 所有出现在下层子查询中的关系 ID */

    /* 扫描或连接的估计大小和成本。 */
    double rows;
    int width;
    Cost startup_cost;
    Cost total_cost;

    double retrieved_rows;
    Cost rel_startup_cost;
    Cost rel_total_cost;

    bool use_remote_estimate;
    Cost fdw_startup_cost;
    Cost fdw_tuple_cost;
    List *shippable_extensions; /* 白名单扩展的 OID */

    /* 缓存的目录信息。 */
    ForeignTable *table;
    ForeignServer *server;
    UserMapping *user; 

    int fetch_size; 
} TDengineFdwRelationInfo;
/*
 * 用于 ForeignScanState 中 fdw_state 的特定于 FDW 的信息
 */
typedef struct TDengineFdwExecState
{
    char *query;           /* 查询字符串 */
    Relation rel;          /* 外部表的关系缓存条目 */
    Oid relid;             /* 关系的对象标识符（OID） */
    UserMapping *user;     /* 外部服务器的用户映射 */
    List *retrieved_attrs; 

    char **params;
    bool cursor_exists;                    
    int numParams;                         
    FmgrInfo *param_flinfo;                
    List *param_exprs;                     
    const char **param_values;             
    Oid *param_types;                      
    TDengineType *param_tdengine_types;    
    TDengineValue *param_tdengine_values;  
    TDengineColumnInfo *param_column_info; 
    int p_nums;                            
    FmgrInfo *p_flinfo;                    

    tdengine_opt *tdengineFdwOptions; /* TDengine FDW 选项 */

    int batch_size;    /* FDW 选项 "batch_size" 的值 */
    List *attr_list;   
    List *column_list; 

    int64 row_nums;     /* 行数 */
    Datum **rows;       /* 扫描的所有行 */
    int64 rowidx;       /* 当前行的索引 */
    bool **rows_isnull; 
    bool for_update;    
    bool is_agg;        
    List *tlist;        

    /* 工作内存上下文 */
    MemoryContext temp_cxt; 
    AttrNumber *junk_idx;

    struct TDengineFdwExecState *aux_fmstate; 

    /* 目标列表中的函数下推支持 */
    bool is_tlist_func_pushdown;

    /* 无模式信息 */
    schemaless_info slinfo;

    void *temp_result;
} TDengineFdwExecState;


extern bool tdengine_is_foreign_expr(PlannerInfo *root,RelOptInfo *baserel,Expr *expr,bool for_tlist);

extern bool tdengine_is_foreign_function_tlist(PlannerInfo *root,RelOptInfo *baserel,List *tlist);

/* option.c headers */

extern tdengine_opt *tdengine_get_options(Oid foreigntableid, Oid userid);
extern void tdengine_deparse_insert(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *targetAttrs);
extern void tdengine_deparse_update(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *targetAttrs, List *attname);
extern void tdengine_deparse_delete(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *attname);
extern bool tdengine_deparse_direct_delete_sql(StringInfo buf, PlannerInfo *root,Index rtindex, Relation rel,RelOptInfo *foreignrel,List *remote_conds,List **params_list,List **retrieved_attrs);
extern void tdengine_deparse_drop_measurement_stmt(StringInfo buf, Relation rel);

/* deparse.c headers */

extern void tdengine_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel,List *tlist, List *remote_conds, List *pathkeys,bool is_subquery, List **retrieved_attrs,List **params_list, bool has_limit);
extern void tdengine_deparse_analyze(StringInfo buf, char *dbname, char *relname);
extern void tdengine_deparse_string_literal(StringInfo buf, const char *val);
extern List *tdengine_build_tlist_to_deparse(RelOptInfo *foreignrel);
extern int tdengine_set_transmission_modes(void);
extern void tdengine_reset_transmission_modes(int nestlevel);
extern bool tdengine_is_select_all(RangeTblEntry *rte, List *tlist, schemaless_info *pslinfo);
extern List *tdengine_pull_func_clause(Node *node);
extern List *tdengine_build_tlist_to_deparse(RelOptInfo *foreignrel);

extern char *tdengine_get_data_type_name(Oid data_type_id);
extern char *tdengine_get_column_name(Oid relid, int attnum);
extern char *tdengine_get_table_name(Relation rel);

extern bool tdengine_is_tag_key(const char *colname, Oid reloid);

/* schemaless.c headers */

extern List *tdengine_pull_slvars(Expr *expr, Index varno, List *columns,bool extract_raw, List *remote_exprs, schemaless_info *pslinfo);
extern void tdengine_get_schemaless_info(schemaless_info *pslinfo, bool schemaless, Oid reloid);
extern char *tdengine_get_slvar(Expr *node, schemaless_info *slinfo);
extern bool tdengine_is_slvar(Oid oid, int attnum, schemaless_info *pslinfo, bool *is_tags, bool *is_fields);
extern bool tdengine_is_slvar_fetch(Node *node, schemaless_info *pslinfo);
extern bool tdengine_is_param_fetch(Node *node, schemaless_info *pslinfo);

/* tdengine_query.c headers */
extern Datum tdengine_convert_to_pg(Oid pgtyp, int pgtypmod, char *value);
extern Datum tdengine_convert_record_to_datum(Oid pgtyp, int pgtypmod, char **row, int attnum, int ntags, int nfield,char **column, char *opername, Oid relid, int ncol, bool is_schemaless);

extern void tdengine_bind_sql_var(Oid type, int attnum, Datum value, TDengineColumnInfo *param_column_info,TDengineType * param_tdengine_types, TDengineValue * param_tdengine_values);

