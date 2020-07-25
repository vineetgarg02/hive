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

package org.apache.hadoop.hive.ql.ddl.table.drop;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Analyzer for Prepare queries. This analyzer generates plan for the parameterized query
 * and save it in cache
 */
@DDLType(types = HiveParser.TOK_PREPARE)
public class PrepareStatementAnalyzer extends CalcitePlanner {

  public PrepareStatementAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  private String getQueryName(ASTNode root) {
    ASTNode queryNameAST = (ASTNode)(root.getChild(1));
    return queryNameAST.getText();
  }

  /**
   * This method saves the current {@link PrepareStatementAnalyzer} object as well as
   * the config used to compile the plan.
   * @param root
   * @throws SemanticException
   */
  private void savePlan(String queryName) throws SemanticException{
    SessionState ss = SessionState.get();
    assert(ss != null);

    if (ss.getPreparePlans().containsKey(queryName)) {
      throw new SemanticException("Prepare query: " + queryName + " already exists.");
    }
    ss.getPreparePlans().put(queryName, this);

    ss.getQueryConfig().put(queryName, this.conf);
  }


  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    ASTNode query = (ASTNode)(root.getChild(0));
    String queryName = getQueryName(root);

    // first compile the parameterized query
    super.analyzeInternal(query);

    // need to mark this as prepared query so that compiler later can skip running it
    // and skip initializing tasks etc.
    this.isPrepareQuery = true;

    //save the plan, skip saving for explain
    if (this.ctx.getExplainConfig() == null) {
      savePlan(queryName);
    }
  }
}
