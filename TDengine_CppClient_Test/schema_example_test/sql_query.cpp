// 测试：执行SQL-查询数据

// TAOS standard API example. The same syntax as MySQL, but only a subset
// to compile: gcc -o query_data_demo query_data_demo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosws.h"

static int DemoQueryData() {
  // ANCHOR: query_data
  int   code = 0;  // 用于存储错误码
  char *dsn = "ws://localhost:6041";  // TDengine WebSocket连接字符串

  // 建立连接
  WS_TAOS *taos = ws_connect(dsn);  // 创建TDengine WebSocket连接
  if (taos == NULL) {  // 检查连接是否成功
    // 输出连接失败的错误信息
    fprintf(stderr, "Failed to connect to %s, ErrCode: 0x%x, ErrMessage: %s.\n", 
            dsn, ws_errno(NULL), ws_errstr(NULL));
    return -1;  // 返回错误码
  }

  // 查询数据，请确保数据库和表已存在
  const char *sql = "SELECT ts, current, location FROM power.meters limit 100";  // SQL查询语句
  WS_RES *result = ws_query(taos, sql);  // 执行查询
  code = ws_errno(result);  // 获取查询错误码
  if (code != 0) {  // 检查查询是否成功
    // 输出查询失败的错误信息
    fprintf(stderr, "Failed to query data from power.meters, sql: %s, ErrCode: 0x%x, ErrMessage: %s\n.", 
            sql, code, ws_errstr(result));
    ws_close(taos);  // 关闭连接
    return -1;  // 返回错误码
  }

  WS_ROW row = NULL;  // 行数据指针
  int rows = 0;  // 行计数器
  int num_fields = ws_field_count(result);  // 获取字段数量
  const WS_FIELD *fields = ws_fetch_fields(result);  // 获取字段信息

  // 输出查询成功信息
  fprintf(stdout, "query successfully, got %d fields, the sql is: %s.\n", num_fields, sql);

  // 逐行获取记录
  while ((row = ws_fetch_row(result))) {  // 获取下一行数据
    // 在此处添加您的数据处理逻辑
    // 可以通过row[i]访问每个字段的值
    
    rows++;  // 行数递增
  }
  fprintf(stdout, "total rows: %d\n", rows);  // 输出总行数
  ws_free_result(result);  // 释放查询结果

  // 关闭连接并清理资源
  ws_close(taos);  // 关闭TDengine连接
  return 0;  // 返回成功
  // ANCHOR_END: query_data
}


int main(int argc, char *argv[]) { return DemoQueryData(); }