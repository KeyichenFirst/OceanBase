#include "common/log/log.h"
#include "common/types.h"
#include "sql/stmt/drop_table_stmt.h"
#include "event/sql_debug.h"
#include "storage/table/table.h"
#include "storage/db/db.h"

RC DropTableStmt::drop(Db *db, const DropTableSqlNode &drop_table, Stmt *&stmt)
{
    // 获取表名
    const char *table_name = drop_table.relation_name.c_str();
    
    // 检查传入的 db 和表名是否有效
    if (nullptr == db || nullptr == table_name) {
        LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
        return RC::INVALID_ARGUMENT;
    }

    // 检查表是否存在
    Table *table = db->find_table(table_name);
    if (nullptr == table) {
        LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
        return RC::SCHEMA_TABLE_NOT_EXIST;  // 修正：添加分号
    }

    // 检查该表是否有依赖关系
    // RC rc = db->check_dependencies(table_name);
    // if (rc != RC::SUCCESS) {
    //     LOG_WARN("Table %s has dependencies and cannot be dropped.", table_name);
    //     return rc;  // 返回依赖错误码，例如 RC::DEPENDENT_OBJECT_EXISTS
    // }

    // 如果没有依赖，创建 DropTableStmt 对象，存储表名
    stmt = new DropTableStmt(drop_table.relation_name);

    // 输出调试信息
    sql_debug("drop table statement: table name %s", drop_table.relation_name.c_str());

    return RC::SUCCESS;
}
