// 测试：执行sql-插入数据（参数绑定）

// TAOS standard API example. The same syntax as MySQL, but only a subset
// to compile: gcc -o stmt_insert_demo stmt_insert_demo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "taosws.h"

/**
 * @brief 执行SQL语句并处理结果
 *
 * @param taos TDengine连接句柄
 * @param sql 要执行的SQL语句
 *
 * 功能说明：
 * 1. 执行传入的SQL语句
 * 2. 检查执行结果状态
 * 3. 错误时输出错误信息并退出程序
 * 4. 成功时释放结果集资源
 */
void executeSQL(WS_TAOS *taos, const char *sql)
{
    // 执行SQL查询
    WS_RES *res = ws_query(taos, sql);

    // 获取错误码
    int code = ws_errno(res);

    // 错误处理
    if (code != 0)
    {
        // 输出错误信息
        fprintf(stderr, "%s\n", ws_errstr(res));
        // 释放结果集
        ws_free_result(res);
        // 关闭连接
        ws_close(taos);
        // 退出程序
        exit(EXIT_FAILURE);
    }

    // 释放结果集资源
    ws_free_result(res);
}

/**
 * @brief 检查错误码并处理错误
 *
 * @param stmt 预处理语句句柄
 * @param code 错误码
 * @param msg 自定义错误消息
 *
 * 功能说明：
 * 1. 检查错误码是否为非0(表示有错误)
 * 2. 如果出错，打印错误信息(包括自定义消息、错误码和详细错误)
 * 3. 关闭预处理语句句柄
 * 4. 退出程序
 */
void checkErrorCode(WS_STMT *stmt, int code, const char *msg)
{
    if (code != 0)
    {
        // 打印错误信息
        fprintf(stderr, "%s. code: %d, error: %s\n", msg, code, ws_stmt_errstr(stmt));
        // 关闭预处理语句
        ws_stmt_close(stmt);
        // 退出程序
        exit(EXIT_FAILURE);
    }
}

typedef struct
{
    int64_t ts;
    float current;
    int voltage;
    float phase;
} Row;

int num_of_sub_table = 10;
int num_of_row = 10;
int total_affected = 0;

/**
 * @brief 使用预处理语句批量插入数据
 *
 * @param taos TDengine连接句柄
 *
 * 功能说明：
 * 1. 初始化预处理语句
 * 2. 准备INSERT语句模板
 * 3. 循环插入多个子表的数据
 * 4. 为每个子表设置表名和TAG值
 * 5. 为每个子表批量插入多行数据
 * 6. 执行批量插入并统计影响行数
 */
void insertData(WS_TAOS *taos)
{
    // 1. 调用 ws_stmt_init() 创建参数绑定对象
    WS_STMT *stmt = ws_stmt_init(taos);
    if (stmt == NULL)
    {
        fprintf(stderr, "Failed to init ws_stmt, error: %s\n", ws_stmt_errstr(NULL));
        exit(EXIT_FAILURE);
    }

    // 2. 调用 ws_stmt_prepare() 解析 INSERT 语句
    const char *sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)";
    int code = ws_stmt_prepare(stmt, sql, 0);
    checkErrorCode(stmt, code, "Failed to execute ws_stmt_prepare");

    // 循环处理每个子表
    for (int i = 1; i <= num_of_sub_table; i++)
    {
        // 生成表名和位置信息
        char table_name[20];
        sprintf(table_name, "d_bind_%d", i);
        char location[20];
        sprintf(location, "location_%d", i);

        // 设置表名和TAG值
        WS_MULTI_BIND tags[2];
        // groupId TAG
        tags[0].buffer_type = TSDB_DATA_TYPE_INT;
        tags[0].buffer_length = sizeof(int);
        tags[0].length = (int32_t *)&tags[0].buffer_length;
        tags[0].buffer = &i;
        tags[0].is_null = NULL;
        tags[0].num = 1;
        // location TAG
        tags[1].buffer_type = TSDB_DATA_TYPE_BINARY;
        tags[1].buffer_length = strlen(location);
        tags[1].length = (int32_t *)&tags[1].buffer_length;
        tags[1].buffer = location;
        tags[1].is_null = NULL;
        tags[1].num = 1;

        // 3. 如果 INSERT 语句中预留了表名但没有预留 TAGS，
        // 那么调用 ws_stmt_set_tbname() 来设置表名；
        // 4. 如果 INSERT 语句中既预留了表名又预留了 TAGS（如 INSERT 语句采取的是自动建表的方式），
        // 那么调用 ws_stmt_set_tbname_tags() 来设置表名和 TAGS 的值；
        code = ws_stmt_set_tbname_tags(stmt, table_name, tags, 2);
        checkErrorCode(stmt, code, "Failed to set table name and tags\n");

        // 准备插入数据的参数绑定
        WS_MULTI_BIND params[4];
        // 时间戳字段
        params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
        params[0].buffer_length = sizeof(int64_t);
        params[0].length = (int32_t *)&params[0].buffer_length;
        params[0].is_null = NULL;
        params[0].num = 1;
        // 电流字段
        params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
        params[1].buffer_length = sizeof(float);
        params[1].length = (int32_t *)&params[1].buffer_length;
        params[1].is_null = NULL;
        params[1].num = 1;
        // 电压字段
        params[2].buffer_type = TSDB_DATA_TYPE_INT;
        params[2].buffer_length = sizeof(int);
        params[2].length = (int32_t *)&params[2].buffer_length;
        params[2].is_null = NULL;
        params[2].num = 1;
        // 相位字段
        params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
        params[3].buffer_length = sizeof(float);
        params[3].length = (int32_t *)&params[3].buffer_length;
        params[3].is_null = NULL;
        params[3].num = 1;

        // 7. 可以重复第 3 ～ 6 步，为批处理加入更多的数据行；
        for (int j = 0; j < num_of_row; j++)
        {
            // 生成随机数据
            struct timeval tv;
            gettimeofday(&tv, NULL);
            long long milliseconds = tv.tv_sec * 1000LL + tv.tv_usec / 1000;
            int64_t ts = milliseconds + j;
            float current = (float)rand() / RAND_MAX * 30;
            int voltage = rand() % 300;
            float phase = (float)rand() / RAND_MAX;

            // 绑定参数值
            params[0].buffer = &ts;
            params[1].buffer = &current;
            params[2].buffer = &voltage;
            params[3].buffer = &phase;

            // 5. 调用 ws_stmt_bind_param_batch() 以多行的方式设置 VALUES 的值
            code = ws_stmt_bind_param_batch(stmt, params, 4);
            checkErrorCode(stmt, code, "Failed to bind param");
        }
        // 6. 调用 ws_stmt_add_batch() 把当前绑定的参数加入批处理；
        code = ws_stmt_add_batch(stmt);
        checkErrorCode(stmt, code, "Failed to add batch");

        // 8. 调用 ws_stmt_execute() 执行已经准备好的批处理指令；
        int affected_rows = 0;
        code = ws_stmt_execute(stmt, &affected_rows);
        checkErrorCode(stmt, code, "Failed to exec stmt");

        // 统计影响行数
        int affected = ws_stmt_affected_rows_once(stmt);
        total_affected += affected;
    }
    // 9. 执行完毕，调用 ws_stmt_close() 释放所有资源。
    fprintf(stdout, "Successfully inserted %d rows to power.meters.\n", total_affected);
    ws_stmt_close(stmt);
}

int main()
{
    int code = 0;
    char *dsn = "ws://localhost:6041";
    WS_TAOS *taos = ws_connect(dsn);
    if (taos == NULL)
    {
        fprintf(stderr, "Failed to connect to %s, ErrCode: 0x%x, ErrMessage: %s.\n", dsn, ws_errno(NULL), ws_errstr(NULL));
        exit(EXIT_FAILURE);
    }
    // create database and table
    executeSQL(taos, "CREATE DATABASE IF NOT EXISTS power");
    executeSQL(taos, "USE power");
    executeSQL(taos,
               "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
               "(groupId INT, location BINARY(24))");
    insertData(taos);
    ws_close(taos);
}