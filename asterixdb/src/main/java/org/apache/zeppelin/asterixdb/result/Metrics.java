/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.asterixdb.result;

/**
 * Metrics field for the result metadata
 */
public class Metrics {

  private String elapsedTime;
  private String executionTime;
  private String resultCount;
  private String resultSize;

  public String getElapsedTime() {
    return elapsedTime;
  }

  public void setElapsedTime(String elapsedTime) {
    this.elapsedTime = elapsedTime;
  }

  public String getExecutionTime() {
    return executionTime;
  }

  public void setExecutionTime(String executionTime) {
    this.executionTime = executionTime;
  }

  public String getResultCount() {
    return resultCount;
  }

  public void setResultCount(String resultCount) {
    this.resultCount = resultCount;
  }

  public String getResultSize() {
    return resultSize;
  }

  public void setResultSize(String resultSize) {
    this.resultSize = resultSize;
  }
}
