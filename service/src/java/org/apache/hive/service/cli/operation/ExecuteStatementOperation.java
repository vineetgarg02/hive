/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.cli.operation;

import java.sql.SQLException;
import java.util.Map;

import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.session.HiveSession;

public abstract class ExecuteStatementOperation extends Operation {
  protected String statement = null;

  public ExecuteStatementOperation(HiveSession parentSession, String statement,
      Map<String, String> confOverlay, boolean runInBackground) {
    super(parentSession, confOverlay, OperationType.EXECUTE_STATEMENT);
    this.statement = statement;
  }

  public String getStatement() {
    return statement;
  }

  public static ExecuteStatementOperation newExecuteStatementOperation(HiveSession parentSession,
      String statement, Map<String, String> confOverlay, boolean runAsync,
      long queryTimeout, Map<Integer, String> stmtParams) throws HiveSQLException {

    String cleanStatement = HiveStringUtils.removeComments(statement);
    String[] tokens = cleanStatement.trim().split("\\s+");
    CommandProcessor processor = null;
    try {
      processor = CommandProcessorFactory.getForHiveCommand(tokens, parentSession.getHiveConf());
    } catch (SQLException e) {
      throw new HiveSQLException(e.getMessage(), e.getSQLState(), e);
    }
    if (processor == null) {
      // runAsync, queryTimeout makes sense only for a SQLOperation
      // Pass the original statement to SQLOperation as sql parser can remove comments by itself
      return new SQLOperation(parentSession, statement, confOverlay, runAsync,
          queryTimeout, stmtParams);
    }
    return new HiveCommandOperation(parentSession, cleanStatement, processor, confOverlay);
  }
}
