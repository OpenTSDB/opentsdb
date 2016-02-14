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
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.nio.charset.Charset;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.utils.Config;

import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.RowLock;
import org.hbase.async.Scanner;
import org.jboss.netty.channel.Channel;
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

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, HBaseClient.class, RowLock.class, 
  AnnotationRpc.class, KeyValue.class, GetRequest.class, Scanner.class})
public final class TestAnnotationRpc {
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private MockBase storage;
  private AnnotationRpc rpc = new AnnotationRpc();
  
  final private byte[] global_row_key = 
      new byte[] { 0, 0, 0, (byte) 0x4F, (byte) 0x29, (byte) 0xD2, 0 };
  final private byte[] tsuid_row_key = 
      new byte[] { 0, 0, 1, (byte) 0x52, (byte) 0xC2, (byte) 0x09, 0, 0, 0, 
        1, 0, 0, 1 };
  
  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    tsdb = new TSDB(client, config);
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    
    // add a global
    storage.addColumn(global_row_key, 
        new byte[] { 1, 0, 0 }, 
        ("{\"startTime\":1328140800,\"endTime\":1328140801,\"description\":" + 
            "\"Description\",\"notes\":\"Notes\",\"custom\":{\"owner\":" + 
            "\"ops\"}}").getBytes(MockBase.ASCII()));
    
    storage.addColumn(global_row_key, 
        new byte[] { 1, 0, 1 }, 
        ("{\"startTime\":1328140801,\"endTime\":1328140803,\"description\":" + 
            "\"Global 2\",\"notes\":\"Nothing\"}").getBytes(MockBase.ASCII()));
    
    // add a local
    storage.addColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x02 }, 
        ("{\"tsuid\":\"000001000001000001\",\"startTime\":1388450562," +
            "\"endTime\":1419984000,\"description\":\"Hello!\",\"notes\":" + 
            "\"My Notes\",\"custom\":{\"owner\":\"ops\"}}")
            .getBytes(MockBase.ASCII()));
    
    storage.addColumn(tsuid_row_key, 
        new byte[] { 1, 0x0A, 0x03 }, 
        ("{\"tsuid\":\"000001000001000001\",\"startTime\":1388450563," +
            "\"endTime\":1419984000,\"description\":\"Note2\",\"notes\":" + 
            "\"Nothing\"}")
            .getBytes(MockBase.ASCII()));
    
    // add some data points too
    storage.addColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x10 }, new byte[] { 1 });
    
    storage.addColumn(tsuid_row_key, 
        new byte[] { 0x50, 0x18 }, new byte[] { 2 });
  }
  
  @Test
  public void constructor() throws Exception {
    new AnnotationRpc();
  }
  
  @Test (expected = BadRequestException.class)
  public void badMethod() throws Exception {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.TRACE, "/api/annotation");
    final HttpQuery query = new HttpQuery(tsdb, req, channelMock);
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void get() throws Exception {
    storage.dumpToSystemOut();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001&start_time=1388450562");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void getGlobal() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?start_time=1328140800");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void getGlobals() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/annotations?start_time=1328140800");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test (expected = BadRequestException.class)
  public void getNotFound() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001&start_time=1388450568");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void getGlobalNotFound() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?start_time=1388450563");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void getGlobalsNotFound() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/annotation?start_time=1388450563");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void getMissingStart() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001");
    rpc.execute(tsdb, query);
  }
 
  @Test
  public void postNew() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001&start_time=1388450564" + 
    "&description=Boo&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"\""));
    assertEquals(5, storage.numColumns(tsuid_row_key));
  }
  
  @Test
  public void postNewGlobal() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?start_time=1328140802" + 
    "&description=Boo&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"\""));
    assertEquals(3, storage.numColumns(global_row_key));
  }
  
  @Test (expected = BadRequestException.class)
  public void postNewMissingStart() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001" + 
    "&description=Boo&method_override=post");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void modify() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001&start_time=1388450562" + 
    "&description=Boo&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"My Notes\""));
  }
  
  @Test
  public void modifyGlobal() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?start_time=1328140800" + 
    "&description=Boo&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"Notes\""));
  }
  
  @Test
  public void modifyPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
    "/api/annotation", "{\"tsuid\":\"000001000001000001\",\"startTime\":" +
    "1388450562,\"description\":\"Boo\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"My Notes\""));
  }
  
  @Test
  public void modifyGlobalPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
    "/api/annotation", "{\"startTime\":1328140800" + 
    ",\"description\":\"Boo\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"Notes\""));
  }

  @Test
  public void modifyPut() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001&start_time=1388450562" + 
    "&description=Boo&method_override=put");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"\""));
    assertTrue(data.contains("\"startTime\":1388450562"));
  }
  
  @Test
  public void modifyPutGlobal() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?start_time=1328140800" + 
    "&description=Boo&method_override=put");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"\""));
    assertTrue(data.contains("\"startTime\":1328140800"));
  }

  @Test
  public void modifyNoChange() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/annotation?tsuid=000001000001000001&start_time=1388450562" + 
      "&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test
  public void delete() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/annotation?tsuid=000001000001000001&start_time=1388450562" + 
      "&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(3, storage.numColumns(tsuid_row_key));
  }
  
  @Test
  public void deleteGlobal() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/annotation?start_time=1328140800" + 
      "&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(1, storage.numColumns(global_row_key));
  }

  @Test (expected = BadRequestException.class)
  public void bulkBadMethodGet() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/annotation/bulk");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void bulkMissingContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/annotation/bulk", "");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void bulkMissingInvalidContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/annotation/bulk", 
        "Not a json object");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void bulkMissingInvalidSingleObject() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/annotation/bulk", 
        "{\"tsuid\":\"000001000001000001\",\"startTime\":" +
            "1388450562,\"description\":\"Boo\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void bulkModifyPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
    "/api/annotation/bulk", "[{\"tsuid\":\"000001000001000001\",\"startTime\":" +
    "1388450562,\"description\":\"Boo\"},{\"tsuid\":\"000001000001000002\"," + 
    "\"startTime\":1388450562,\"description\":\"Gum\"}]");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"My Notes\""));
    assertTrue(data.contains("\"description\":\"Gum\""));
  }
  
  @Test
  public void bulkModifyGlobalPOST() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
    "/api/annotation/bulk", "[{\"startTime\":1328140800" + 
    ",\"description\":\"Boo\"},{\"startTime\":1388450562,\"description\":" +
    "\"Gum\"}]");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"Notes\""));
    assertTrue(data.contains("\"description\":\"Gum\""));
  }

  @Test (expected = BadRequestException.class)
  public void bulkModifyPOSTMissingStart() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
    "/api/annotation/bulk", "[{\"tsuid\":\"000001000001000001\",\"startTime\":" +
    "1388450562,\"description\":\"Boo\"},{\"tsuid\":\"000001000001000002\"," + 
    "\"description\":\"Gum\"}]");
    rpc.execute(tsdb, query);
  }

  @Test
  public void bulkModifyPut() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/annotation/bulk",
    "[{\"tsuid\":\"000001000001000001\",\"startTime\":" +
    "1328140800,\"description\":\"Boo\"},{\"tsuid\":\"000001000001000002\"," + 
    "\"startTime\":1328140800,\"description\":\"Gum\"}]");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"\""));
    assertTrue(data.contains("\"description\":\"Gum\""));
  }
  
  @Test
  public void bulkModifyPutGlobal() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/annotation/bulk",
    "[{\"startTime\":1328140800,\"description\":\"Boo\"},{" + 
    "\"startTime\":1328140800,\"description\":\"Gum\"}]");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"\""));
    assertTrue(data.contains("\"startTime\":1328140800"));
    assertTrue(data.contains("\"description\":\"Gum\""));
  }

  @Test
  public void bulkDelete() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation/bulk?tsuids=000001000001000001,000001000001000002" +
    "&start_time=1388450560000&end_time=1388450562000&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"totalDeleted\":1"));
    assertEquals(3, storage.numColumns(tsuid_row_key));
  }
  
  @Test
  public void bulkDeleteNotFound() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation/bulk?tsuids=000001000001000001,000001000001000002" +
    "&start_time=1388450550000&end_time=1388450560000&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"totalDeleted\":0"));
    assertEquals(4, storage.numColumns(tsuid_row_key));
  }
  
  @Test
  public void bulkDeleteAllTime() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation/bulk?tsuids=000001000001000001,000001000001000002" +
    "&start_time=1000000000000&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"totalDeleted\":2"));
    assertEquals(2, storage.numColumns(tsuid_row_key));
  }

  @Test
  public void bulkDeleteGlobal() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation/bulk?start_time=1328140799000&end_time=1328140800000" +
    "&global=true&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"totalDeleted\":1"));
    assertEquals(1, storage.numColumns(global_row_key));
  }
  
  @Test
  public void bulkDeleteGlobalNotFound() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation/bulk?start_time=1328140600000&end_time=1328140700000" +
    "&global=true&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"totalDeleted\":0"));
    assertEquals(2, storage.numColumns(global_row_key));
  }
  
  @Test
  public void bulkDeleteGlobalAllTime() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation/bulk?start_time=1000000000000" +
    "&global=true&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"totalDeleted\":2"));
    assertEquals(-1, storage.numColumns(global_row_key));
  }
  
  @Test (expected = BadRequestException.class)
  public void bulkDeleteMissingStart() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation/bulk?tsuids=000001000001000001,000001000001000002" +
    "&end_time=1388450562000&method_override=delete");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void bulkDeleteMissingTsuidsAndGlobal() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation/bulk?&start_time=1388450562000&method_override=delete");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void bulkDeleteEmptyTsuids() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation/bulk?&start_time=1388450562000&tsuids=&method_override=delete");
    rpc.execute(tsdb, query);
  }

  @Test
  public void bulkDeleteDELETE() throws Exception {
    HttpQuery query = NettyMocks.deleteQuery(tsdb, 
    "/api/annotation/bulk", "{\"tsuids\":[\"000001000001000001\"," +
    "\"000001000001000002\"],\"startTime\":\"1388450560000\",\"endTime\":" +
    "\"1388450562000\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"totalDeleted\":1"));
    assertEquals(3, storage.numColumns(tsuid_row_key));
  }
  
  @Test
  public void bulkDeleteGlobalDELETE() throws Exception {
    HttpQuery query = NettyMocks.deleteQuery(tsdb, 
    "/api/annotation/bulk", "{\"startTime\":\"1328140799000\",\"endTime\":" +
        "\"1328140800000\",\"global\":true}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"totalDeleted\":1"));
    assertEquals(1, storage.numColumns(global_row_key));
  }
  
  @Test (expected = BadRequestException.class)
  public void bulkDeleteMissingStartDELETE() throws Exception {
    HttpQuery query = NettyMocks.deleteQuery(tsdb, 
    "/api/annotation/bulk?", "{\"tsuids\":[\"000001000001000001\"," +
        "\"000001000001000002\"],\"endTime\":" +
        "\"1388450562000\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void bulkDeleteEmptyTsuidsDELETE() throws Exception {
    HttpQuery query = NettyMocks.deleteQuery(tsdb, 
    "/api/annotation/bulk", "{\"startTime\":\"1328140799000\",\"endTime\":" +
        "\"1328140800000\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void bulkDeleteNoBodyDELETE() throws Exception {
    HttpQuery query = NettyMocks.deleteQuery(tsdb, 
    "/api/annotation/bulk", null);
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void bulkDeleteBadJSONDELETE() throws Exception {
    HttpQuery query = NettyMocks.deleteQuery(tsdb, 
    "/api/annotation/bulk", "{thisisnotjson}");
    rpc.execute(tsdb, query);
  }
}
