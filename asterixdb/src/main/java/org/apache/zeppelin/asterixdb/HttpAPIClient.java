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

package org.apache.zeppelin.asterixdb;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.zeppelin.asterixdb.result.ResultObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AsterixDB HTTP API client
 */
public class HttpAPIClient {

  private static final Logger logger = LoggerFactory.getLogger(HttpAPIClient.class);
  private static final String QUERY_SERVICE_PATH = "/query/service";
  private static final String QUERY_CANCEL_PATH = "/admin/requests/running/";
  private final String uri;
  private final String planFormat;
  private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private final int warningCount;

  public HttpAPIClient(final String host, final String port, String planFormat, int warningCount) {
    uri = "http://" + host + ":" + port;
    this.planFormat = planFormat == null ? "STRING" : planFormat;
    this.warningCount = warningCount;
  }

  public ResultObject executeQuery(String query, String clientContextId, boolean plan) {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    HttpPost apiHttpPost = new HttpPost(uri + QUERY_SERVICE_PATH);
    String result = "";
    JsonObject jsonRequest = new JsonObject();
    jsonRequest.add("statement", new JsonPrimitive(query));
    jsonRequest.add("client_context_id", new JsonPrimitive(clientContextId));
    jsonRequest.add("plan-format", new JsonPrimitive(planFormat));
    jsonRequest.add("optimized-logical-plan", new JsonPrimitive(plan));
    jsonRequest.add("max-warnings", new JsonPrimitive(warningCount));
    try {
      apiHttpPost.setEntity(new StringEntity(jsonRequest.toString(), ContentType.APPLICATION_JSON));
      InputStream inputStream = httpclient.execute(apiHttpPost).getEntity().getContent();
      StringWriter writer = new StringWriter();
      IOUtils.copy(inputStream, writer, "UTF-8");
      result = writer.toString();
    } catch (IOException e) {
      logger.error("Couldn't connect to AsterixDB HTTP API", e);
      return null;
    }
    return gson.fromJson(result, ResultObject.class);
  }

  public boolean cancelQueryExecution(String clientContextId) {
    CloseableHttpClient httpclient = HttpClients.createDefault();
    HttpDelete endPoint = new HttpDelete(getCancelURI(clientContextId));
    try {
      InputStream inputStream = httpclient.execute(endPoint).getEntity().getContent();
      StringWriter writer = new StringWriter();
      IOUtils.copy(inputStream, writer, "UTF-8");
    } catch (IOException e) {
      logger.error("Could not cancel job for paragraph id " + clientContextId, e);
      return false;
    }
    return true;
  }

  private String getCancelURI(String clientContextId) {
    return uri + QUERY_CANCEL_PATH + "?client_context_id=" + clientContextId;
  }

}
