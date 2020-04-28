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

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsterixDBInterpreterTests {

  private static final String HOST = "localhost";
  private static final String PORT = "19002;";
  private static AsterixDBInterpreter interpreter;

  @BeforeClass
  public static void setup() {
    Properties properties = new Properties();
    properties.setProperty(AsterixDBInterpreter.HOST, HOST);
    properties.setProperty(AsterixDBInterpreter.PORT, PORT);
    interpreter = new AsterixDBInterpreter(properties);
    interpreter.open();
  }

  private InterpreterContext getContext(String noteId, String paragraphId) {
    AngularObjectRegistry objectRegistry = new AngularObjectRegistry("asterixdbInterpreter", null);
    return new InterpreterContext(noteId, paragraphId, null, null, null, null, null, null, null,
        objectRegistry, null, null, null);
  }

  @Test
  public void testQuery() {
    final InterpreterContext context = getContext("0", "testQuery");
    final String query = "SELECT 1+1 as sum;";
    final InterpreterResult result = interpreter.interpret(query, context);
    assertEquals(Code.SUCCESS, result.code());
    assertEquals("[{\"sum\":2}]", result.message().get(0).getData());
  }

  @Test
  public void testDDL() {
    final InterpreterContext context = getContext("0", "testDDL");
    final String query =
        "DROP DATAVERSE test if exists;\n" +
            "CREATE DATAVERSE test;";
    final InterpreterResult result = interpreter.interpret(query, context);
    assertEquals(Code.SUCCESS, result.code());
  }

  @Test
  public void testError() {
    final InterpreterContext context = getContext("0", "testDDL");
    final String query = "test 123";
    final InterpreterResult result = interpreter.interpret(query, context);
    assertEquals(Code.ERROR, result.code());
  }

}
