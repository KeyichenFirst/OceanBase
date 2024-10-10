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

#include "sql/executor/select_tables_executor.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/select_stmt.h"
#include "storage/db/db.h"

RC SelectTablesExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();

  SelectStmt *select_tables_stmt = static_cast<SelectStmt *>(stmt);

  const std::vector<Table *> &tables = select_tables_stmt->tables();
   // 检查是否有多个表
  if (tables.size() < 2) {
    LOG_ERROR("SelectTablesExecutor requires at least two tables for a multi-table query");
    return RC::INVALID_ARGUMENT;
  }

  RC rc = session->get_current_db()->select_tables(tables);

  return rc;
}