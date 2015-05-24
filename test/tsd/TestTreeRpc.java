// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Method;
import java.util.TreeMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.storage.MockBase;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.TestTree;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class, HBaseClient.class, GetRequest.class, Tree.class,
  PutRequest.class, KeyValue.class, Scanner.class, DeleteRequest.class })
public final class TestTreeRpc {
  private static byte[] NAME_FAMILY = "name".getBytes(MockBase.ASCII());
  private TSDB tsdb;
  private HBaseClient client = mock(HBaseClient.class);
  private MockBase storage;
  private TreeRpc rpc = new TreeRpc();
  
  final static private Method branchToStorageJson;
  static {
    try {
      branchToStorageJson = Branch.class.getDeclaredMethod("toStorageJson");
      branchToStorageJson.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  final static private Method TreetoStorageJson;
  static {
    try {
      TreetoStorageJson = Tree.class.getDeclaredMethod("toStorageJson");
      TreetoStorageJson.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  final static private Method LeaftoStorageJson;
  static {
    try {
      LeaftoStorageJson = Leaf.class.getDeclaredMethod("toStorageJson");
      LeaftoStorageJson.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  final static private Method TSMetagetStorageJSON;
  static {
    try {
      TSMetagetStorageJSON = TSMeta.class.getDeclaredMethod("getStorageJSON");
      TSMetagetStorageJSON.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  final static private Method UIDMetagetStorageJSON;
  static {
    try {
      UIDMetagetStorageJSON = UIDMeta.class.getDeclaredMethod("getStorageJSON");
      UIDMetagetStorageJSON.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    tsdb = new TSDB(client, config);
    storage = new MockBase(tsdb, client, true, true, true, true);
  }
  
  @Test
  public void constructor() throws Exception {
    new TreeRpc();
  }
  
  @Test (expected = BadRequestException.class)
  public void noRoute() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb,  "/api/tree/noroute");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTreeBadMethod() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.TRACE, "/api/tree");
    final HttpQuery query = new HttpQuery(tsdb, req, NettyMocks.fakeChannel());
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleTreeGetAll() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"Test Tree\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"2nd Tree\""));
  }
  
  @Test
  public void handleTreeGetSingle() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?treeid=2");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"2nd Tree\""));
    assertFalse(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"Test Tree\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTreeGetNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/tree?treeid=3");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTreeGetBadID655536() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/tree?treeid=655536");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleTreeQSCreate() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?name=NewTree&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(1, storage.numColumns(new byte[] { 0, 3 }));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTreeQSCreateNoName() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?method_override=post&description=HelloWorld");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTreeQSCreateOutOfIDs() throws Exception {
    setupStorage();
    storage.addColumn(new byte[] { (byte) 0xFF, (byte) 0xFF }, 
        "tree".getBytes(MockBase.ASCII()), "{}".getBytes(MockBase.ASCII()));
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?method_override=post");
    rpc.execute(tsdb, query);
  }
    
  @Test
  public void handleTreePOSTCreate() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
      "/api/tree", "{\"name\":\"New Tree\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(1, storage.numColumns(new byte[] { 0, 3 }));
  }

  @Test
  public void handleTreeQSModify() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?treeid=1&method_override=post&description=HelloWorld");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"description\":\"HelloWorld\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"Test Tree\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTreeQSModifyNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?treeid=3&method_override=post&description=HelloWorld");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleTreeQSModifyNotModified() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?treeid=1&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test
  public void handleTreePOSTModify() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
      "/api/tree", "{\"treeId\":1,\"description\":\"Hello World\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"description\":\"Hello World\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"Test Tree\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTreeQSPutNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?treeid=3&method_override=put&description=HelloWorld");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleTreeQSPutNotModified() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?treeid=1&method_override=put");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test
  public void handleTreeQSPut() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?treeid=1&method_override=put&description=HelloWorld");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"description\":\"HelloWorld\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"\""));
  }
  
  @Test
  public void handleTreePOSTPut() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.putQuery(tsdb, 
      "/api/tree", "{\"treeId\":1,\"description\":\"Hello World\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"description\":\"Hello World\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"\""));
  }

  @Test
  public void handleTreeQSDeleteDefault() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?treeid=1&method_override=delete");
    // make sure the root is there BEFORE we delete
    assertEquals(4, storage.numColumns(new byte[] { 0, 1 }));
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    // make sure the definition is still there but the root is gone
    assertEquals(3, storage.numColumns(new byte[] { 0, 1 }));
    assertEquals(-1, storage.numColumns(
        Branch.stringToId("00010001BECD000181A8")));
    assertEquals(-1, storage.numColumns(
        Branch.stringToId("00010001BECD000181A8BF992A99")));
  }
  
  @Test
  public void handleTreeQSDeleteDefinition() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?treeid=1&method_override=delete&definition=true");
    // make sure the root is there BEFORE we delete
    assertEquals(4, storage.numColumns(new byte[] { 0, 1 }));
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    // make sure the definition has been deleted too
    assertEquals(-1, storage.numColumns(new byte[] { 0, 1 }));
    assertEquals(-1, storage.numColumns(
        Branch.stringToId("00010001BECD000181A8")));
    assertEquals(-1, storage.numColumns(
        Branch.stringToId("00010001BECD000181A8BF992A99")));
  }
  
  @Test
  public void handleTreePOSTDeleteDefault() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.deleteQuery(tsdb, 
      "/api/tree", "{\"treeId\":1}");
    // make sure the root is there BEFORE we delete
    assertEquals(4, storage.numColumns(new byte[] { 0, 1 }));
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    // make sure the definition is still there but the root is gone
    assertEquals(3, storage.numColumns(new byte[] { 0, 1 }));
    assertEquals(-1, storage.numColumns(
        Branch.stringToId("00010001BECD000181A8")));
    assertEquals(-1, storage.numColumns(
        Branch.stringToId("00010001BECD000181A8BF992A99")));
  }
  
  @Test
  public void handleTreePOSTDeleteDefinition() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.deleteQuery(tsdb, 
      "/api/tree", "{\"treeId\":1,\"definition\":true}");
    // make sure the root is there BEFORE we delete
    assertEquals(4, storage.numColumns(new byte[] { 0, 1 }));
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    // make sure the definition has been deleted too
    assertEquals(-1, storage.numColumns(new byte[] { 0, 1 }));
    assertEquals(-1, storage.numColumns(
        Branch.stringToId("00010001BECD000181A8")));
    assertEquals(-1, storage.numColumns(
        Branch.stringToId("00010001BECD000181A8BF992A99")));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTreeQSDeleteNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?treeid=3&method_override=delete");
    rpc.execute(tsdb, query);
  }

  @Test
  public void handleBranchRoot() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/tree/branch?treeid=1");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"displayName\":\"ROOT\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"branches\":null"));
  }
  
  @Test
  public void handleBranchChild() throws Exception {
    setupStorage();
    setupBranch();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/branch?branch=00010001BECD000181A8");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"metric\":\"sys.cpu.0\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"branches\":["));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleBranchNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/branch?branch=00010001BECD000181A8BBBBB");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleBranchNoTree() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/branch");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleBranchBadMethod() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.TRACE, "/api/tree/branch");
    final HttpQuery query = new HttpQuery(tsdb, req, NettyMocks.fakeChannel());
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleRuleGetQS() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=1&order=0");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"type\":\"METRIC\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"level\":1"));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRuleGetQSNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=2&order=2");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRuleGetQSTreeNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=4&level=1&order=0");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRuleGetQSMissingTree() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?level=1&order=0");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRuleGetQSMissingLevel() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&order=0");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRuleGetQSMissingOrder() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=1");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleRuleQSNew() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=2&order=1&description=Testing" +
      "&method_override=post&type=metric");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"description\":\"Testing\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"level\":2"));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRuleQSNewFailValidation() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=2&order=1&description=Testing" +
      "&method_override=post&type=tagk");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRuleQSNewMissingType() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=2&order=1&description=Testing&method_override=post");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleRuleQSNotModified() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=1&order=0&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test
  public void handleRuleQSModify() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=1&order=0&description=Testing&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"description\":\"Testing\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"level\":1"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"notes\":\"Metric rule\""));
  }
  
  @Test
  public void handleRulePOSTNew() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
      "/api/tree/rule", "{\"treeId\":1,\"level\":2,\"order\":2,\"description\":" +
      "\"Testing\",\"type\":\"metric\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"description\":\"Testing\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"level\":2"));
  }

  @Test
  public void handleRulePOSTModify() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
      "/api/tree/rule", "{\"treeId\":1,\"level\":1,\"order\":0,\"description\":" +
      "\"Testing\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"description\":\"Testing\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"level\":1"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"notes\":\"Metric rule\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRulesPOSTNoRules() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
      "/api/tree/rules", "");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleRuleQSPut() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=1&order=0&description=Testing" + 
      "&method_override=put&type=metric");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"description\":\"Testing\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"level\":1"));
    assertFalse(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"notes\":\"Metric rule\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRuleQSPutMissingType() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=1&order=0&description=Testing&method_override=put");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleRulePUT() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.putQuery(tsdb, 
      "/api/tree/rule", "{\"treeId\":1,\"level\":1,\"order\":0,\"description\":" +
      "\"Testing\",\"type\":\"metric\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"description\":\"Testing\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"level\":1"));
    assertFalse(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"notes\":\"Metric rule\""));
  }
  
  @Test
  public void handleRuleQSDelete() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=1&order=0&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(3, storage.numColumns(new byte[] { 0, 1 }));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRuleQSDeleteNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rule?treeid=1&level=2&order=0&method_override=delete");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleRuleDELETE() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.deleteQuery(tsdb, 
      "/api/tree/rule", "{\"treeId\":1,\"level\":1,\"order\":0}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(3, storage.numColumns(new byte[] { 0, 1 }));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRuleBadMethod() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.TRACE, "/api/tree/rule");
    final HttpQuery query = new HttpQuery(tsdb, req, NettyMocks.fakeChannel());
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRulesGetQS() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rules?treeid=1");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleRulesPOST() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
      "/api/tree/rules", "[{\"treeId\":1,\"level\":0,\"order\":0,\"type\":" +
      "\"METRIC\"},{\"treeId\":1,\"level\":0,\"order\":1,\"type\":\"tagk\"," +
      "\"field\":\"fqdn\"},{\"treeId\":1,\"level\":1,\"order\":0,\"type\":" +
      "\"tagk\",\"field\":\"host\"}]");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(5, storage.numColumns(new byte[] { 0, 1 }));
    final String rule = new String(storage.getColumn(new byte[] { 0, 1 }, 
        "tree_rule:0:0".getBytes(MockBase.ASCII())), MockBase.ASCII());
    assertTrue(rule.contains("\"type\":\"METRIC\""));
    assertTrue(rule.contains("description\":\"Host Name\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRulesPOSTEmpty() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
      "/api/tree/rules", "[]]");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleRulesPUT() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.putQuery(tsdb, 
      "/api/tree/rules", "[{\"treeId\":1,\"level\":0,\"order\":0,\"type\":" +
      "\"METRIC\"},{\"treeId\":1,\"level\":0,\"order\":1,\"type\":\"tagk\"," +
      "\"field\":\"fqdn\"},{\"treeId\":1,\"level\":1,\"order\":0,\"type\":" +
      "\"tagk\",\"field\":\"host\"}]");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(5, storage.numColumns(new byte[] { 0, 1 }));
    final String rule = new String(storage.getColumn(new byte[] { 0, 1 }, 
        "tree_rule:0:0".getBytes(MockBase.ASCII())), MockBase.ASCII());
    assertTrue(rule.contains("\"type\":\"METRIC\""));
    assertFalse(rule.contains("\"description\":\"Host Name\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRulesPOSTTreeMissmatch() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
      "/api/tree/rules", "[{\"treeId\":2,\"level\":0,\"order\":0,\"type\":" +
      "\"METRIC\"},{\"treeId\":1,\"level\":0,\"order\":1,\"type\":\"tagk\"," +
      "\"field\":\"fqdn\"},{\"treeId\":1,\"level\":1,\"order\":0,\"type\":" +
      "\"tagk\",\"field\":\"host\"}]");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleRulesDeleteQS() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree/rules?treeid=1&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(2, storage.numColumns(new byte[] { 0, 1 }));
  }
  
  @Test
  public void handleRulesDelete() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.deleteQuery(tsdb, 
      "/api/tree/rules?treeid=1", "");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(2, storage.numColumns(new byte[] { 0, 1 }));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleRulesDeleteTreeNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.deleteQuery(tsdb, 
      "/api/tree/rules?treeid=5", "");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleTestQS() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/test?treeid=1&tsuids=000001000001000001000002000002");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("Adding leaf"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("000001000001000001000002000002"));
  }
  
  @Test
  public void handleTestQSMulti() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/test?treeid=1&tsuids=000001000001000001000002000002," +
        "000001000001000001000002000003");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("Adding leaf"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("000001000001000001000002000002"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("000001000001000001000002000003"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("Unable to locate TSUID meta data"));
  }
  
  @Test
  public void handleTestPOST() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/tree/test", "{\"treeId\":1,\"tsuids\":[" + 
        "\"000001000001000001000002000002\"]}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("Adding leaf"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("000001000001000001000002000002"));
  }
  
  @Test
  public void handleTestPUT() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.putQuery(tsdb, 
        "/api/tree/test", "{\"treeId\":1,\"tsuids\":[" + 
        "\"000001000001000001000002000002\"]}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("Adding leaf"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("000001000001000001000002000002"));
  }
  
  @Test
  public void handleTestPOSTMulti() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/tree/test", "{\"treeId\":1,\"tsuids\":[" + 
        "\"000001000001000001000002000002\"," +
        "\"000001000001000001000002000003\"]}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("Adding leaf"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("000001000001000001000002000002"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("000001000001000001000002000003"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("Unable to locate TSUID meta data"));
  }
  
  @Test
  public void handleTestTSUIDNotFound() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/test?treeid=1&tsuids=000001000001000001000002000003");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("Unable to locate TSUID meta data"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("000001000001000001000002000003"));
    
  }
  
  @Test
  public void handleTestNSU() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    storage.flushRow(new byte[] { 0, 0, 2 });
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/test?treeid=1&tsuids=000001000001000001000002000002");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("TSUID was missing a UID name"));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("000001000001000001000002000002"));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTestTreeNotFound() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/test?treeid=3&tsuids=000001000001000001000002000002");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTestMissingTreeId() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/test?tsuids=000001000001000001000002000002");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTestQSMissingTSUIDs() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/test?treeid=1");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTestPOSTMissingTSUIDs() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/tree/test", "{\"treeId\":1}");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTestBadMethod() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.TRACE, "/api/tree/test");
    final HttpQuery query = new HttpQuery(tsdb, req, NettyMocks.fakeChannel());
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleCollissionsQS() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/collisions?treeid=1");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"010101\":\"AAAAAA\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"020202\":\"BBBBBB\""));
  }
  
  @Test
  public void handleCollissionsQSSingleTSUID() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/collisions?treeid=1&tsuids=010101");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"010101\":\"AAAAAA\"}", 
        query.response().getContent().toString(MockBase.ASCII()));
  }
  
  @Test
  public void handleCollissionsQSTSUIDs() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/collisions?treeid=1&tsuids=010101,020202");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"010101\":\"AAAAAA\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"020202\":\"BBBBBB\""));
  }
  
  @Test
  public void handleCollissionsQSTSUIDNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/collisions?treeid=1&tsuids=030101");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{}", 
        query.response().getContent().toString(MockBase.ASCII()));
  }
  
  @Test
  public void handleCollissionsPOST() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/tree/collisions", "{\"treeId\":1}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"010101\":\"AAAAAA\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"020202\":\"BBBBBB\""));
  }
  
  @Test
  public void handleCollissionsPOSTSingleTSUID() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/tree/collisions", "{\"treeId\":1,\"tsuids\":[\"020202\"]}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"020202\":\"BBBBBB\"}", 
        query.response().getContent().toString(MockBase.ASCII()));
  }
  
  @Test
  public void handleCollissionsPOSTTSUIDs() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/tree/collisions", "{\"treeId\":1,\"tsuids\":" +
        "[\"010101\",\"020202\"]}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"010101\":\"AAAAAA\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"020202\":\"BBBBBB\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleCollissionsTreeNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/collisions?treeid=5");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleCollissionsMissingTreeId() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/collisions");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleCollissionsBadMethod() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.TRACE, "/api/tree/collisions");
    final HttpQuery query = new HttpQuery(tsdb, req, NettyMocks.fakeChannel());
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleNotMatchedQS() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/notmatched?treeid=1");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"010101\":\"Failed rule 0:0\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"020202\":\"Failed rule 1:1\""));
  }
  
  @Test
  public void handleNotMatchedQSSingleTSUID() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/notmatched?treeid=1&tsuids=010101");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"010101\":\"Failed rule 0:0\"}", 
        query.response().getContent().toString(MockBase.ASCII()));
  }
  
  @Test
  public void handleNotMatchedQSTSUIDs() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/notmatched?treeid=1&tsuids=010101,020202");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"010101\":\"Failed rule 0:0\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"020202\":\"Failed rule 1:1\""));
  }
  
  @Test
  public void handleNotMatchedQSTSUIDNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/notmatched?treeid=1&tsuids=030101");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{}", 
        query.response().getContent().toString(MockBase.ASCII()));
  }
  
  @Test
  public void handleNotMatchedPOST() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/tree/notmatched", "{\"treeId\":1}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"010101\":\"Failed rule 0:0\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"020202\":\"Failed rule 1:1\""));
  }
  
  @Test
  public void handleNotMatchedPOSTSingleTSUID() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/tree/notmatched", "{\"treeId\":1,\"tsuids\":[\"020202\"]}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"020202\":\"Failed rule 1:1\"}", 
        query.response().getContent().toString(MockBase.ASCII()));
  }
  
  @Test
  public void handleNotMatchedPOSTTSUIDs() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.postQuery(tsdb, 
        "/api/tree/notmatched", "{\"treeId\":1,\"tsuids\":" +
        "[\"010101\",\"020202\"]}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"010101\":\"Failed rule 0:0\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"020202\":\"Failed rule 1:1\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleNotMatchedNotFound() throws Exception {
    setupStorage();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/notmatched?treeid=5");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleNotMatchedMissingTreeId() throws Exception {
    setupStorage();
    setupBranch();
    setupTSMeta();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/tree/notmatched");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleNotMatchedBadMethod() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.TRACE, "/api/tree/notmatched");
    final HttpQuery query = new HttpQuery(tsdb, req, NettyMocks.fakeChannel());
    rpc.execute(tsdb, query);
  }
  
  /**
   * Setups objects in MockBase including two trees, rule sets, root branch,
   * child branch, leaves and some collisions and no matches. These are used for
   * most of the tests so they're all here.
   */
  private void setupStorage() throws Exception {         
    Tree tree = TestTree.buildTestTree();
 
    // store root
    TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    Branch root = new Branch(tree.getTreeId());
    root.setDisplayName("ROOT");
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    storage.addColumn(root.compileBranchId(), Tree.TREE_FAMILY(),
        "branch".getBytes(MockBase.ASCII()), 
        (byte[])branchToStorageJson.invoke(root));
    
    // store the first tree
    byte[] key = new byte[] { 0, 1 };
    storage.addColumn(key, Tree.TREE_FAMILY(), "tree".getBytes(MockBase.ASCII()), 
        (byte[])TreetoStorageJson.invoke(TestTree.buildTestTree()));
    
    TreeRule rule = new TreeRule(1);
    rule.setField("host");
    rule.setDescription("Hostname rule");
    rule.setType(TreeRuleType.TAGK);
    rule.setDescription("Host Name");
    storage.addColumn(key, Tree.TREE_FAMILY(), 
        "tree_rule:0:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));

    rule = new TreeRule(1);
    rule.setField("");
    rule.setLevel(1);
    rule.setNotes("Metric rule");
    rule.setType(TreeRuleType.METRIC);
    storage.addColumn(key, Tree.TREE_FAMILY(),
        "tree_rule:1:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));
    
    root = new Branch(1);
    root.setDisplayName("ROOT");
    root_path = new TreeMap<Integer, String>();
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    storage.addColumn(key, Tree.TREE_FAMILY(), 
        "branch".getBytes(MockBase.ASCII()), 
        (byte[])branchToStorageJson.invoke(root));
    
    // tree 2
    key = new byte[] { 0, 2 };

    Tree tree2 = new Tree();
    tree2.setTreeId(2);
    tree2.setName("2nd Tree");
    tree2.setDescription("Other Tree");
    storage.addColumn(key, Tree.TREE_FAMILY(), "tree".getBytes(MockBase.ASCII()), 
        (byte[])TreetoStorageJson.invoke(tree2));
    
    rule = new TreeRule(2);
    rule.setField("host");
    rule.setType(TreeRuleType.TAGK);
    storage.addColumn(key, Tree.TREE_FAMILY(), 
        "tree_rule:0:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));
    
    rule = new TreeRule(2);
    rule.setField("");
    rule.setLevel(1);
    rule.setType(TreeRuleType.METRIC);
    storage.addColumn(key, Tree.TREE_FAMILY(), 
        "tree_rule:1:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));
    
    root = new Branch(2);
    root.setDisplayName("ROOT");
    root_path = new TreeMap<Integer, String>();
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    storage.addColumn(key, Tree.TREE_FAMILY(), 
        "branch".getBytes(MockBase.ASCII()), 
        (byte[])branchToStorageJson.invoke(root));
    
    // sprinkle in some collisions and no matches for fun
    // collisions
    key = new byte[] { 0, 1, 1 };
    String tsuid = "010101";
    byte[] qualifier = new byte[Tree.COLLISION_PREFIX().length + 
                                      (tsuid.length() / 2)];
    System.arraycopy(Tree.COLLISION_PREFIX(), 0, qualifier, 0, 
        Tree.COLLISION_PREFIX().length);
    byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.COLLISION_PREFIX().length, 
        tsuid_bytes.length);
    storage.addColumn(key, Tree.TREE_FAMILY(), qualifier, 
        "AAAAAA".getBytes(MockBase.ASCII()));
    
    tsuid = "020202";
    qualifier = new byte[Tree.COLLISION_PREFIX().length + 
                                      (tsuid.length() / 2)];
    System.arraycopy(Tree.COLLISION_PREFIX(), 0, qualifier, 0, 
        Tree.COLLISION_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.COLLISION_PREFIX().length, 
        tsuid_bytes.length);
    storage.addColumn(key, Tree.TREE_FAMILY(), qualifier, 
        "BBBBBB".getBytes(MockBase.ASCII()));
    
    // not matched
    key = new byte[] { 0, 1, 2 };
    tsuid = "010101";
    qualifier = new byte[Tree.NOT_MATCHED_PREFIX().length + 
                             (tsuid.length() / 2)];
    System.arraycopy(Tree.NOT_MATCHED_PREFIX(), 0, qualifier, 0, 
        Tree.NOT_MATCHED_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.NOT_MATCHED_PREFIX().length, 
    tsuid_bytes.length);
    storage.addColumn(key, Tree.TREE_FAMILY(), qualifier, 
        "Failed rule 0:0".getBytes(MockBase.ASCII()));
    
    tsuid = "020202";
    qualifier = new byte[Tree.NOT_MATCHED_PREFIX().length + 
                             (tsuid.length() / 2)];
    System.arraycopy(Tree.NOT_MATCHED_PREFIX(), 0, qualifier, 0, 
        Tree.NOT_MATCHED_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.NOT_MATCHED_PREFIX().length, 
    tsuid_bytes.length);
    storage.addColumn(key, Tree.TREE_FAMILY(), qualifier, 
        "Failed rule 1:1".getBytes(MockBase.ASCII()));
    
    // drop some branches in for tree 1
    Branch branch = new Branch(1);
    TreeMap<Integer, String> path = new TreeMap<Integer, String>();
    path.put(0, "ROOT");
    path.put(1, "sys");
    path.put(2, "cpu");
    branch.prependParentPath(path);
    branch.setDisplayName("cpu");
    storage.addColumn(branch.compileBranchId(), Tree.TREE_FAMILY(),
        "branch".getBytes(MockBase.ASCII()), 
        (byte[])branchToStorageJson.invoke(branch));
    
    Leaf leaf = new Leaf("user", "000001000001000001");
    qualifier = leaf.columnQualifier();
    storage.addColumn(branch.compileBranchId(), Tree.TREE_FAMILY(),
        qualifier, (byte[])LeaftoStorageJson.invoke(leaf));
    
    leaf = new Leaf("nice", "000002000002000002");
    qualifier = leaf.columnQualifier();
    storage.addColumn(branch.compileBranchId(), Tree.TREE_FAMILY(),
        qualifier, (byte[])LeaftoStorageJson.invoke(leaf));
    
    // child branch
    branch = new Branch(1);
    path.put(3, "mboard");
    branch.prependParentPath(path);
    branch.setDisplayName("mboard");
    storage.addColumn(branch.compileBranchId(), Tree.TREE_FAMILY(),
        "branch".getBytes(MockBase.ASCII()), 
        (byte[])branchToStorageJson.invoke(branch));
    
    leaf = new Leaf("Asus", "000003000003000003");
    qualifier = leaf.columnQualifier();
    storage.addColumn(branch.compileBranchId(), Tree.TREE_FAMILY(),
        qualifier, (byte[])LeaftoStorageJson.invoke(leaf));
  }
  
  /**
   * Sets up some UID name maps in storage for use when loading leaves from a 
   * branch. Without these, the unit tests will fail since the leaves couldn't 
   * find their name maps.
   */
  private void setupBranch() {
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "metrics".getBytes(MockBase.ASCII()),
        "sys.cpu.0".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "tagk".getBytes(MockBase.ASCII()),
        "host".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "tagv".getBytes(MockBase.ASCII()),
        "web01".getBytes(MockBase.ASCII()));
  }
  
  /**
   * Sets up a TSMeta object and associated UIDMeta objects in storage for 
   * testing the "test" call. These are necessary as the TSMeta is loaded when
   * parsed through the tree.
   */
  private void setupTSMeta() throws Exception {
    final TSMeta meta = new TSMeta("000001000001000001000002000002");
    storage.addColumn(UniqueId.stringToUid("000001000001000001000002000002"), 
        NAME_FAMILY, "ts_meta".getBytes(MockBase.ASCII()), 
        (byte[])TSMetagetStorageJSON.invoke(meta));
    
    final UIDMeta metric = new UIDMeta(UniqueIdType.METRIC, new byte[] { 0, 0, 1 }, 
        "sys.cpu.0");
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "metric_meta".getBytes(MockBase.ASCII()), 
        (byte[])UIDMetagetStorageJSON.invoke(metric));
    final UIDMeta tagk1 = new UIDMeta(UniqueIdType.TAGK, new byte[] { 0, 0, 1 }, 
        "host");
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "tagk_meta".getBytes(MockBase.ASCII()), 
        (byte[])UIDMetagetStorageJSON.invoke(tagk1));
    final UIDMeta tagv1 = new UIDMeta(UniqueIdType.TAGV, new byte[] { 0, 0, 1 }, 
        "web-01.lga.mysite.com");
    storage.addColumn(new byte[] { 0, 0, 1 }, NAME_FAMILY,
        "tagv_meta".getBytes(MockBase.ASCII()), 
        (byte[])UIDMetagetStorageJSON.invoke(tagv1));
    final UIDMeta tagk2 = new UIDMeta(UniqueIdType.TAGK, new byte[] { 0, 0, 2 }, 
        "type");
    storage.addColumn(new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagk_meta".getBytes(MockBase.ASCII()), 
        (byte[])UIDMetagetStorageJSON.invoke(tagk2));
    final UIDMeta tagv2 = new UIDMeta(UniqueIdType.TAGV, new byte[] { 0, 0, 2 }, 
        "user");
    storage.addColumn(new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagv_meta".getBytes(MockBase.ASCII()), 
        (byte[])UIDMetagetStorageJSON.invoke(tagv2));
    
    storage.addColumn(new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagk".getBytes(MockBase.ASCII()),
        "type".getBytes(MockBase.ASCII()));
    storage.addColumn(new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagv".getBytes(MockBase.ASCII()),
        "user".getBytes(MockBase.ASCII()));
  }
}
