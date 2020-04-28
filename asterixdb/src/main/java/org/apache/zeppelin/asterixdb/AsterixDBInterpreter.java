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


import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.zeppelin.asterixdb.result.Message;
import org.apache.zeppelin.asterixdb.result.ResultObject;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache AsterixDB SQL++ Interpreter for Zeppelin.
 */
public class AsterixDBInterpreter extends Interpreter {

  private static final String CONNECTION_QUERY = "SELECT 'Hello World';";
  private static final String JSON_VIEWER =
      "<script src=\"http://rawgit.com/abodelot/jquery.json-viewer/master/json-viewer"
      + "/jquery.json-viewer.js\"></script>\n"
      + "<link href=\"http://rawgit.com/abodelot/jquery.json-viewer/master/json-viewer"
      + "/jquery.json-viewer.css\" "
      + "type=\"text/css\" rel=\"stylesheet\" />\n";
  public static final String HOST = "asterixdb.host";
  public static final String PORT = "asterixdb.port";
  private static final String RESULT_SIZE = "asterixdb.result.size";
  private static final String PLAN_FORMAT = "asterixdb.plan.format";
  private static final String WARNING_COUNT = "asterixdb.warning.count";

  // Default configs
  private static final String DEFAULT_RESULT_SIZE = "1000";
  private static final String DEFAULT_PLAN_FORMAT = "STRING";
  private static final String DEFAULT_WARNING_COUNT = "10";
  private static final Logger LOGGER = LoggerFactory.getLogger(AsterixDBInterpreter.class);
  private final HttpAPIClient api;
  private final int resultSize;

  public AsterixDBInterpreter(Properties property) {
    super(property);

    final String host = getProperty(HOST);
    final String port = getProperty(PORT);
    final String resultSizeString = getProperty(RESULT_SIZE, DEFAULT_RESULT_SIZE);
    final String planFormat = getProperty(PLAN_FORMAT, DEFAULT_PLAN_FORMAT);
    final String warningCountString = getProperty(WARNING_COUNT, DEFAULT_WARNING_COUNT);
    final int warningCount = Integer.parseInt(warningCountString);
    api = new HttpAPIClient(host, port, planFormat, warningCount);
    resultSize = Integer.parseInt(resultSizeString);
    LOGGER.info("AsterixDB Interpreter initiated");
  }

  @Override
  public void open() {
    final ResultObject resultObject = api.executeQuery(CONNECTION_QUERY,
            "zeppelin_connection", false);
    if (resultObject == null || !"success".equals(resultObject.getStatus())) {
      LOGGER.error("Couldn't connect to AsterixDB HTTP API");
    }
    LOGGER.info("Connected to AsterixDB");

  }

  @Override
  public void close() {
    logger.info("Connection closed.");
  }


  @Override
  public InterpreterResult interpret(String submittedQuery, final InterpreterContext context) {
    final boolean flatten = isFlatten(submittedQuery);
    final boolean plan = isPlan(submittedQuery);

    final String query;
    if (flatten || plan) {
      query = submittedQuery.substring(5);
    } else {
      query = submittedQuery;
    }

    final ResultObject resultObject = api.executeQuery(query, context.getParagraphId(), plan);

    if (resultObject == null) {
      return new InterpreterResult(Code.ERROR, Type.TEXT, "Could not connect to AsterixDB");
    } else if (!"success".equals(resultObject.getStatus())) {
      return new InterpreterResult(Code.ERROR, InterpreterResult.Type.TEXT,
          getErrors(resultObject.getErrors()));
    } else if ("success".equals(resultObject.getStatus()) && resultObject.getResults() == null
            && !plan) {
      return new InterpreterResult(Code.SUCCESS, Type.TEXT, "success");
    }

    final InterpreterResult.Type resultType;
    StringBuilder resultString = new StringBuilder();

    if (plan && !flatten) {
      resultString.append("PLAN:\n");
      final JsonArray planJsonArray = new JsonArray();
      planJsonArray.add(new JsonPrimitive(resultObject.getPlans().getOptimizedLogicalPlan()));
      resultString.append(jsonFormat(planJsonArray,
              context.getParagraphId() + "-PLAN"));
      resultString.append('\n');
    }

    if (resultObject.getResults() != null) {
      if (flatten) {
        resultString.append(flatFormat(resultObject.getResults()));
        resultType = InterpreterResult.Type.TABLE;
      } else {
        resultString.append(jsonFormat(resultObject.getResults(), context.getParagraphId()));
        resultType = Type.HTML;
      }
    } else {
      resultType = Type.HTML;
      resultString.append("\n" + "success");
    }

    if (resultType == Type.HTML) {
      final List<Message> warnings = resultObject.getWarnings();
      if (warnings != null && !warnings.isEmpty()) {
        resultString.append("Warnings: <br>");
        for (Message m : warnings) {
          resultString.append(m.getMsg());
          resultString.append("<br>");
        }
      }
      resultString.append("<br>");
      resultString.append("Execution Time: " +
              resultObject.getMetrics().getExecutionTime());
    }



    return new InterpreterResult(InterpreterResult.Code.SUCCESS, resultType,
            resultString.toString());
  }

  private boolean isFlatten(String submittedQuery) {
    return submittedQuery.length() > 5
            && "+flat".equals(submittedQuery.substring(0, 5));
  }

  private boolean isPlan(String submittedQuery) {
    return submittedQuery.length() > 5
            && "+plan".equals(submittedQuery.substring(0, 5));
  }

  @Override
  public void cancel(final InterpreterContext context) {
    api.cancelQueryExecution(context.getParagraphId());
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(final InterpreterContext context) {
    return 0;
  }

  private String jsonFormat(final JsonElement resultArray, final String id) {
    final String result = ensureResultSize(resultArray).toString();
    return JSON_VIEWER + " <script>$('#json-renderer" + id + "').jsonViewer(" + result
        + ");</script></head><body><pre id=\"json-renderer" + id + "\"></pre></body>";
  }

  private String flatFormat(final JsonElement resultArray) {
    final List<Map<String, Object>> flattenHits = new LinkedList<>();
    final Set<String> keys = new TreeSet<>();
    final JsonArray tuples = ensureResultSize(resultArray);

    if (tuples.size() == 0) {
      return "";
    }

    for (int i = 0; i < tuples.size(); i++) {
      final String json = tuples.get(i).toString();
      final Map<String, Object> flattenMap = JsonFlattener.flattenAsMap(json);
      flattenHits.add(flattenMap);
      for (final String key : flattenMap.keySet()) {
        keys.add(key);
      }
    }

    final StringBuffer buffer = new StringBuffer();
    for (final String key : keys) {
      buffer.append(key).append('\t');
    }
    buffer.replace(buffer.lastIndexOf("\t"), buffer.lastIndexOf("\t") + 1, "\n");

    for (final Map<String, Object> hit : flattenHits) {
      for (final String key : keys) {
        final Object val = hit.get(key);
        if (val != null) {
          buffer.append(val);
        }
        buffer.append('\t');
      }
      buffer.replace(buffer.lastIndexOf("\t"), buffer.lastIndexOf("\t") + 1, "\n");
    }
    return buffer.toString();
  }

  private String getErrors(final List<Message> errors) {
    final StringBuilder stringBuilder = new StringBuilder();
    for (final Message e : errors) {
      stringBuilder.append("Error " + e.getMsg() + "\n");
    }
    return stringBuilder.toString();
  }

  //TODO(wyk): Fix array copy. Find an easy way to cap the result size without parsing.
  private JsonArray ensureResultSize(final JsonElement result) {
    final JsonArray resultArray = result.getAsJsonArray();
    JsonArray resizedResultArray = resultArray;
    if (resultSize < resultArray.size()) {
      resizedResultArray = new JsonArray();
      for (int i = 0; i < resultSize; i++) {
        resizedResultArray.add(resultArray.get(i));
      }
    }
    return resizedResultArray;
  }
}
