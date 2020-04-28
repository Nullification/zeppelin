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

import com.google.gson.JsonElement;
import java.util.List;

/**
 * Result object that will be returned from AsterixDB HTTP API.
 */
public class ResultObject {

  private String requestID;
  private String clientContextID;
  private JsonElement results;
  private String status;
  private Metrics metrics;
  private List<Message> errors;
  private List<Message> warnings;
  private Plans plans;

  public String getRequestID() {
    return requestID;
  }

  public void setRequestID(String requestID) {
    this.requestID = requestID;
  }

  public String getClientContextID() {
    return clientContextID;
  }

  public void setClientContextID(String clientContextID) {
    this.clientContextID = clientContextID;
  }

  public JsonElement getResults() {
    return results;
  }

  public void setResults(JsonElement results) {
    this.results = results;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public void setMetrics(Metrics metrics) {
    this.metrics = metrics;
  }

  public List<Message> getErrors() {
    return errors;
  }

  public void setErrors(List<Message> errors) {
    this.errors = errors;
  }

  public List<Message> getWarnings() {
    return warnings;
  }

  public void setWarnings(List<Message> warnings) {
    this.warnings = warnings;
  }

  public Plans getPlans() {
    return plans;
  }

  public void setPlans(Plans plans) {
    this.plans = plans;
  }
}
