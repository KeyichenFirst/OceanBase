/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/6/13.
//

#include "sql/executor/insert_executor.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/insert_stmt.h"
#include "storage/db/db.h"

RC InsertExecutor::execute(SQLStageEvent *sql_event)
{
    Stmt    *stmt    = sql_event->stmt();
   //   Session *session = sql_event->session_event()->session();

   InsertStmt *insert_stmt = static_cast<InsertStmt *>(stmt);

   // 获取目标表
   Table *table = insert_stmt->table();
    if (nullptr == table) {
        LOG_ERROR("Cannot find target table for insert.");
        return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    
    // 获得插入的
    const Value *values_array = insert_stmt->values();
    int value_amount = insert_stmt->value_amount();
    // 获取单条记录的大小
    int re_size = table->table_meta().record_size();

    // 构造一个 Record 对象
    char *record_data = new char[re_size];
    memset(record_data, 0, re_size);

    // 将插入的数据填充到 record_data
    int offset = 0;
    for (int i = 0; i < value_amount; i++) {
    const Value &value = values_array[i];
    int value_size = 0;

    // 使用 attr_type() 获取数据类型
    switch (value.attr_type()) {
        case AttrType::INTS:
            value_size = sizeof(int);
            break;
        case AttrType::FLOATS:
            value_size = sizeof(float);
            break;
        // 可以根据其他类型进行扩展
        default:
            LOG_ERROR("Unsupported value type.");
            delete[] record_data;
            return RC::INVALID_ARGUMENT;
    }

        // 将数据复制到record_data
        memcpy(record_data + offset, value.data(), value_size);
        offset += value_size;
    }

     // 创建 Record 对象
    Record record;
    record.set_data(record_data);
    
    RC rc = table->insert_record(record);

    if (rc != RC::SUCCESS) {
        LOG_ERROR("Failed to insert record into table. rc=%s", strrc(rc));
    }

    // 释放内存
    delete[] record_data;

  return rc;
}