//// This file is part of OpenTSDB.
//// Copyright (C) 2017  The OpenTSDB Authors.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////   http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//package net.opentsdb.query.execution;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.fail;
//
//import org.junit.Before;
//import org.junit.Test;
//
//import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
//import com.fasterxml.jackson.databind.JavaType;
//import com.google.common.reflect.TypeToken;
//
//import net.opentsdb.utils.JSON;
//
//public class TestQueryExecutorConfigResolver {
//  
//  protected JavaType type;
//  
//  @Before
//  public void before() throws Exception {
//    type = JSON.getMapper().getTypeFactory()
//        .constructSimpleType(QueryExecutorConfig.class, new JavaType[0]);
//  }
//  
//  @Test
//  public void init() throws Exception {
//    final QueryExecutorConfigResolver resolver = 
//        new QueryExecutorConfigResolver();
//    resolver.init(type);
//    assertEquals("net.opentsdb.query.execution", 
//        QueryExecutorConfigResolver.PACKAGE);
//    assertEquals(Id.CUSTOM, resolver.getMechanism());
//    assertEquals("QueryExecutorConfig", resolver.idFromBaseType());
//    
//    try {
//      resolver.init(null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
//
//  @Test
//  public void idFromValue() throws Exception {
//    final QueryExecutorConfigResolver resolver = 
//        new QueryExecutorConfigResolver();
//    resolver.init(type);
//    
//    // simple type, no declaring class.
//    assertEquals("String", resolver.idFromValue("Hello!"));
//    
//    // compound class
//    final TestQueryExecutorConfig.UTConfig config = 
//        (TestQueryExecutorConfig.UTConfig) 
//        new TestQueryExecutorConfig.UTConfig.Builder()
//      .setExecutorType(
//          "net.opentsdb.query.execution.TestQueryExecutorConfig$UTConfig")
//      .setExecutorId("UTTest")
//      .build();
//    assertEquals("TestQueryExecutorConfig", resolver.idFromValue(config));
//    
//    try {
//      resolver.idFromValue(null);
//      fail("Expected NullPointerException");
//    } catch (NullPointerException e) { }
//  }
//  
//  @Test
//  public void idFromValueAndType() throws Exception {
//    final QueryExecutorConfigResolver resolver = 
//        new QueryExecutorConfigResolver();
//    resolver.init(type);
//    assertEquals("String", resolver.idFromValueAndType(null, 
//        "Hello!".getClass()));
//    
//    // compound class
//    final TestQueryExecutorConfig.UTConfig config = 
//        (TestQueryExecutorConfig.UTConfig) 
//        new TestQueryExecutorConfig.UTConfig.Builder()
//        .setExecutorType(
//            "net.opentsdb.query.execution.TestQueryExecutorConfig$UTConfig")
//        .setExecutorId("UTTest")
//      .build();
//    assertEquals("TestQueryExecutorConfig", resolver.idFromValueAndType(null, 
//        config.getClass()));
//    
//    // switcher-roo
//    assertEquals("TestQueryExecutorConfig", resolver.idFromValueAndType(config, 
//        "Hello!".getClass()));
//    
//    try {
//      resolver.idFromValueAndType(null, null);
//      fail("Expected NullPointerException");
//    } catch (NullPointerException e) { }
//  }
//  
//  @Test
//  public void typeOf() throws Exception {
//    TypeToken<?> token = QueryExecutorConfigResolver.typeOf(
//        "net.opentsdb.query.execution.TestQueryExecutorConfig$UTConfig");
//    assertEquals(TestQueryExecutorConfig.UTConfig.class, token.getRawType());
//    
//    try {
//      QueryExecutorConfigResolver.typeOf("");
//      fail("Expected IllegalStateException");
//    } catch (IllegalStateException e) { }
//    
//    try {
//      QueryExecutorConfigResolver.typeOf("net.opentsdb.nosuchclass");
//      fail("Expected IllegalStateException");
//    } catch (IllegalStateException e) { }
//    
//    try {
//      QueryExecutorConfigResolver.typeOf(null);
//      fail("Expected NullPointerException");
//    } catch (NullPointerException e) { }
//  }
//  
//  @Test
//  public void typeFromId() throws Exception {
//    final QueryExecutorConfigResolver resolver = 
//        new QueryExecutorConfigResolver();
//    resolver.init(type);
//    JavaType type = resolver.typeFromId(null,
//        "net.opentsdb.query.execution.TestQueryExecutorConfig$UTConfig");
//    assertEquals(TestQueryExecutorConfig.UTConfig.class, type.getRawClass());
//    
//    try {
//      resolver.typeFromId(null, "");
//      fail("Expected IllegalStateException");
//    } catch (IllegalStateException e) { }
//    
//    try {
//      resolver.typeFromId(null, "net.opentsdb.nosuchclass");
//      fail("Expected IllegalStateException");
//    } catch (IllegalStateException e) { }
//    
//    try {
//      resolver.typeFromId(null, null);
//      fail("Expected NullPointerException");
//    } catch (NullPointerException e) { }
//  }
//  
//}
