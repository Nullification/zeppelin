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

import org.apache.zeppelin.asterixdb.result.ResultObject;
import org.junit.Test;

public class HttpAPIClientTest {

  private final HttpAPIClient apiClient;

  public HttpAPIClientTest() {
    apiClient = new HttpAPIClient("192.168.0.100", "19002", null, 10);
  }

  @Test
  public void testExecuteQuery() {
    final String clientContextId = "test1";
    final ResultObject resultObject = apiClient.executeQuery("SELECT 1+1;", clientContextId,
            true);
    assert ("success".equals(resultObject.getStatus()));
  }

  @Test
  public void testWarningQuery() {
    final String clientContextId = "test1";
    final ResultObject resultObject = apiClient.executeQuery("SELECT 1 < \"str\";", clientContextId,
            true);
    assert ("success".equals(resultObject.getStatus()));
  }

  @Test
  public void testCancelQuery() {
    final String clientContextId = "test2";
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        //Long running query
        String query =
            "SELECT t1, t2\n"
                + "FROM range(1, 1000000) as t1, range (1, 1000000) as t2\n"
                + "WHERE t1 = t2;";
        apiClient.executeQuery(query, clientContextId, false);
      }
    };

    //Run query.
    new Thread(runnable).start();

    //Wait for query submission
    try {
      Thread.sleep(2000L);
    } catch (InterruptedException e) {
      //Ignore
    }

    //Assert that the query get cancelled
    assert (apiClient.cancelQueryExecution(clientContextId));
  }
}
