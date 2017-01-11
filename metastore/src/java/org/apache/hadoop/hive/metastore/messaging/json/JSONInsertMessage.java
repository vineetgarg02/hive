/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.messaging.json;

import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * JSON implementation of InsertMessage
 */
public class JSONInsertMessage extends InsertMessage {

  @JsonProperty
  String server, servicePrincipal, db, table;

  @JsonProperty
  Long timestamp;

  @JsonProperty
  List<String> files;

  @JsonProperty
  Map<String, String> partKeyVals;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONInsertMessage() {
  }

  public JSONInsertMessage(String server, String servicePrincipal, String db, String table,
      Map<String, String> partKeyVals, List<String> files, Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.db = db;
    this.table = table;
    this.timestamp = timestamp;
    this.partKeyVals = partKeyVals;
    this.files = files;
    checkValid();
  }

  public JSONInsertMessage(String server, String servicePrincipal, String db, String table,
      Map<String, String> partKeyVals, List<String> files, List<String> checksums, Long timestamp) {
    this(server, servicePrincipal, db, table, partKeyVals, files, timestamp);
    for (int i = 0; i < files.size(); i++) {
      if ((!checksums.isEmpty()) && (checksums.get(i) != null) && !checksums.get(i).isEmpty()) {
        files.set(i, encodeFileUri(files.get(i), checksums.get(i)));
      }
    }
  }

  // TODO: this needs to be enhanced once change management based filesystem is implemented
  // Currently using fileuri#checksum as the format
  private String encodeFileUri(String fileUriStr, String fileChecksum) {
    return fileUriStr + "#" + fileChecksum;
  }

  @Override
  public String getTable() {
    return table;
  }

  @Override
  public String getServer() {
    return server;
  }

  @Override
  public Map<String, String> getPartitionKeyValues() {
    return partKeyVals;
  }

  @Override
  public List<String> getFiles() {
    return files;
  }

  @Override
  public String getServicePrincipal() {
    return servicePrincipal;
  }

  @Override
  public String getDB() {
    return db;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    try {
      return JSONMessageDeserializer.mapper.writeValueAsString(this);
    } catch (Exception exception) {
      throw new IllegalArgumentException("Could not serialize: ", exception);
    }
  }

}