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
import static org.mockito.Matchers.anyString;
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
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
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
  
  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    PowerMockito.whenNew(HBaseClient.class)
      .withArguments(anyString(), anyString()).thenReturn(client);
    tsdb = new TSDB(config);
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    
    // add a global
    storage.addColumn(
        new byte[] { 0, 0, 0, (byte) 0x4F, (byte) 0x29, (byte) 0xD2, 0 }, 
        new byte[] { 1, 0, 0 }, 
        ("{\"startTime\":1328140800,\"endTime\":1328140801,\"description\":" + 
            "\"Description\",\"notes\":\"Notes\",\"custom\":{\"owner\":" + 
            "\"ops\"}}").getBytes(MockBase.ASCII()));
    
    // add a local
    storage.addColumn(
        new byte[] { 0, 0, 1, (byte) 0x52, (byte) 0xC2, (byte) 0x09, 0, 0, 0, 
            1, 0, 0, 1 }, 
        new byte[] { 1, 0x0A, 0x02 }, 
        ("{\"tsuid\":\"000001000001000001\",\"startTime\":1388450562," +
            "\"endTime\":1419984000,\"description\":\"Hello!\",\"notes\":" + 
            "\"My Notes\",\"custom\":{\"owner\":\"ops\"}}")
            .getBytes(MockBase.ASCII()));
  }
  
  @Test
  public void constructor() throws Exception {
    new AnnotationRpc();
  }
  
  @Test (expected = BadRequestException.class)
  public void badMethod() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/annotation");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void get() throws Exception {
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
  
  @Test (expected = BadRequestException.class)
  public void getNotFound() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001&start_time=1388450563");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void getGlobalNotFound() throws Exception {
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
    "/api/annotation?tsuid=000001000001000001&start_time=1388450563" + 
    "&description=Boo&method=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"\""));
    assertEquals(2, storage.numColumns(new byte[] { 0, 0, 1, (byte) 0x52, 
        (byte) 0xC2, (byte) 0x09, 0, 0, 0, 1, 0, 0, 1 }));
  }
  
  @Test
  public void postNewGlobal() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?start_time=1328140801" + 
    "&description=Boo&method=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"\""));
    assertEquals(2, storage.numColumns(
        new byte[] { 0, 0, 0, (byte) 0x4F, (byte) 0x29, (byte) 0xD2, 0 }));
  }
  
  @Test (expected = BadRequestException.class)
  public void postNewMissingStart() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001" + 
    "&description=Boo&method=post");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void modify() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001&start_time=1388450562" + 
    "&description=Boo&method=post");
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
    "&description=Boo&method=post");
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
    "&description=Boo&method=put");
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
    "&description=Boo&method=put");
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
      "&method=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test
  public void delete() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/annotation?tsuid=000001000001000001&start_time=1388450562" + 
      "&method=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(-1, storage.numColumns(new byte[] { 0, 0, 1, (byte) 0x52, 
        (byte) 0xC2, (byte) 0x09, 0, 0, 0, 1, 0, 0, 1 }));
  }
  
  @Test
  public void deleteGlobal() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/annotation?start_time=1328140800" + 
      "&method=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    assertEquals(-1, storage.numColumns(
        new byte[] { 0, 0, 0, (byte) 0x4F, (byte) 0x29, (byte) 0xD2, 0 }));
  }
}
