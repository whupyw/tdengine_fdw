#ifndef QUERY_CXX_H
#define QUERY_CXX_H

#include "postgres.h"
#include "tdengine_fdw.h"

/* TDengine 中的值的信息 */
typedef union TDengineValue
{
    long long int i; 
    double d;        
    int b;           
    char *s;         
} TDengineValue;

/* measurement的模式信息 */
typedef struct TableInfo
{
    char *measurement; 
    char **tag;        
    char **field;      
    char **field_type; 
    int tag_len;       
    int field_len;     
} TableInfo;


/* 表的列类型信息 */
typedef enum TDengineColumnType
{
    TDENGINE_UNKNOWN_KEY, 
    TDENGINE_TIME_KEY,    
    TDENGINE_TAG_KEY,     
    TDENGINE_FIELD_KEY,   
} TDengineColumnType;


/* TDengineQuery 函数的返回类型 */
struct TDengineQuery_return
{
    TDengineResult *r0; 
    char *r1;           
};
/* 一行数据 */
typedef struct TDengineRow
{
    char **tuple; 
} TDengineRow;

/* TDengine 查询结果集*/
typedef struct TDengineResult
{
    TDengineRow *rows; 
    int ncol;          
    int nrow;          
    char **columns;    
    char **tagkeys;    
    int ntag;          
} TDengineResult;

/* 数据类型的信息 */
typedef enum TDengineType
{
    TDENGINE_INT64,   
    TDENGINE_DOUBLE,  
    TDENGINE_BOOLEAN, 
    TDENGINE_STRING,  
    TDENGINE_TIME,    
    TDENGINE_NULL,    
} TDengineType;

/* 表的列信息 */
typedef struct TDengineColumnInfo
{
    char *column_name;              
    TDengineColumnType column_type; 
} TDengineColumnInfo;

/* 函数的返回类型 */
struct TDengineSchemaInfo_return
{
    struct TableInfo *r0; 
    long long r1;         
    char *r2;             
};
#endif /* QUERY_CXX_H */