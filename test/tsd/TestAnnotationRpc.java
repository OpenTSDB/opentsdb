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

import static com.google.common.base.Preconditions.checkNotNull;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.Map;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.hbase.async.KeyValue;
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
@PrepareForTest({KeyValue.class, Scanner.class})
public final class TestAnnotationRpc {
  private TSDB tsdb = null;
  private MemoryStore tsdb_store;
  private AnnotationRpc rpc = new AnnotationRpc();
  
  final private byte[] global_row_key = 
      new byte[] { 0, 0, 0, (byte) 0x4F, (byte) 0x29, (byte) 0xD2, 0 };
  final private byte[] tsuid_row_key = 
      new byte[] { 0, 0, 1, (byte) 0x52, (byte) 0xC2, (byte) 0x09, 0, 0, 0, 
        1, 0, 0, 1 };

  private static final String TSUID_GLOBAL_ANNOTATION = null;
  private static final String TSUID_ANNOTATION = "000001000001000001";
  private static final long GLOBAL_ONE_START_TIME = 1328140800;
  private static final long GLOBAL_ONE_END_TIME = 1328140801;

  private static final long GLOBAL_TWO_START_TIME = 1328140801;
  private static final long GLOBAL_TWO_END_TIME = 1328140803;

  private static final long LOCAL_ONE_START_TIME = 1388450562;
  private static final long LOCAL_ONE_END_TIME = 1419984000;

  private static final long LOCAL_TWO_START_TIME = 1388450563;
  private static final long LOCAL_TWO_END_TIME = 1419984000;

  private static final Map<String,String> custom = ImmutableMap.of("owner", "ops");

  private final Annotation global_one = new Annotation();
  private final Annotation global_two = new Annotation();
  private final Annotation local_one = new Annotation();
  private final Annotation local_two = new Annotation();

  private final Table<String, Long, Annotation> annotations;

  public TestAnnotationRpc() {
    annotations = HashBasedTable.create();
    global_one.setTSUID(TSUID_GLOBAL_ANNOTATION);
    global_one.setStartTime(GLOBAL_ONE_START_TIME);
    global_one.setEndTime(GLOBAL_ONE_END_TIME);
    global_one.setDescription("Description");
    global_one.setNotes("Notes");
    global_one.setCustom(custom);

    global_two.setTSUID(TSUID_GLOBAL_ANNOTATION);
    global_two.setStartTime(GLOBAL_TWO_START_TIME);
    global_two.setEndTime(GLOBAL_TWO_END_TIME);
    global_two.setDescription("Global 2");
    global_two.setNotes("Nothing");

    local_one.setTSUID(TSUID_ANNOTATION);
    local_one.setStartTime(LOCAL_ONE_START_TIME);
    local_one.setEndTime(LOCAL_ONE_END_TIME);
    local_one.setDescription("Hello!");
    local_one.setNotes("My Notes");
    local_one.setCustom(custom);

    local_two.setTSUID(TSUID_ANNOTATION);
    local_two.setStartTime(LOCAL_TWO_START_TIME);
    local_two.setEndTime(LOCAL_TWO_END_TIME);
    local_two.setDescription("Note2");
    local_two.setNotes("Nothing");

    annotations.put("", GLOBAL_ONE_START_TIME, global_one);
    annotations.put("", GLOBAL_TWO_START_TIME, global_two);

    annotations.put(TSUID_ANNOTATION, LOCAL_ONE_START_TIME, local_one);
    annotations.put(TSUID_ANNOTATION, LOCAL_TWO_START_TIME, local_two);
  }

  @Before
  public void before() throws Exception {
    final Config config = new Config(false);
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);

    // add a global
    tsdb_store.updateAnnotation(null, global_one);

    // add another global
    tsdb_store.updateAnnotation(null, global_two);

    // add a local
    tsdb_store.updateAnnotation(null, local_one);

    // add another local
    tsdb_store.updateAnnotation(null, local_two);

    // add some data points too
    tsdb_store.addColumn(tsuid_row_key,
      new byte[]{0x50, 0x10}, new byte[]{1});
    
    tsdb_store.addColumn(tsuid_row_key,
      new byte[]{0x50, 0x18}, new byte[]{2});
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
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001&start_time=1388450562");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION,GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION,GLOBAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
  }
  
  @Test
  public void getGlobal() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?start_time=1328140800");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
  }
  
  @Test (expected = BadRequestException.class)
  public void getNotFound() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/annotation?tsuid=000001000001000001&start_time=1388450568");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
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
    "/api/annotation?tsuid=000001000001000001&start_time=1388450564" + 
    "&description=Boo&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"\""));

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));

    Annotation a = tsdb.getAnnotation("000001000001000001",1388450564)
            .joinUninterruptibly();
    assertEquals("Boo", a.getDescription());
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

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));

    Annotation a = tsdb.getAnnotation(TSUID_GLOBAL_ANNOTATION, 1328140802)
            .joinUninterruptibly();
    assertEquals("Boo",a.getDescription());
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

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));

    Annotation local = tsdb.
            getAnnotation(TSUID_ANNOTATION, LOCAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(local);

    assertTrue(local.getDescription().equals("Boo"));
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

    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));

    Annotation global = tsdb.
            getAnnotation(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(global);

    assertEquals("Boo", global.getDescription());
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

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    Annotation local = tsdb.
            getAnnotation(TSUID_ANNOTATION, LOCAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(local);

    assertEquals("Boo",local.getDescription());
    assertEquals("My Notes", local.getNotes());
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
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));

    Annotation global = tsdb.
            getAnnotation(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(global);

    assertEquals("Boo",global.getDescription());
    assertEquals("Notes", global.getNotes());
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

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));

    Annotation local = tsdb.
            getAnnotation(TSUID_ANNOTATION, LOCAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(local);
    assertEquals("Boo", local.getDescription());
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

    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));

    Annotation global = tsdb.
            getAnnotation(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(global);
    assertEquals("Boo",global.getDescription());
  }

  @Test
  public void modifyNoChange() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/annotation?tsuid=000001000001000001&start_time=1388450562" + 
      "&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());

    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
  }
  
  @Test
  public void delete() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/annotation?tsuid=000001000001000001&start_time=1388450562" + 
      "&method_override=delete");
    rpc.execute(tsdb, query);
    //check right resonse
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    //check that tsuid is gone
    assertNull(tsdb.getAnnotation(TSUID_ANNOTATION, LOCAL_ONE_START_TIME)
            .joinUninterruptibly());
    //verify others
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));

  }
  
  @Test
  public void deleteGlobal() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/annotation?start_time=1328140800" + 
      "&method_override=delete");
    rpc.execute(tsdb, query);
    //check right resonse
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
    //check that tsuid is gone
    assertNull(tsdb.getAnnotation(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME)
            .joinUninterruptibly());
    //verify others
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
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
    //verify unchanged
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));

    Annotation local = tsdb.
            getAnnotation(TSUID_ANNOTATION, LOCAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(local);
    assertEquals("Boo", local.getDescription());

    Annotation new_local = tsdb.
            getAnnotation("000001000001000002", LOCAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(new_local);
    assertEquals("Gum", new_local.getDescription());
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
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));

    Annotation global = tsdb.
            getAnnotation(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(global);
    assertEquals("Boo", global.getDescription());

    Annotation new_local = tsdb.
            getAnnotation(TSUID_GLOBAL_ANNOTATION, LOCAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(new_local);
    assertEquals("Gum", new_local.getDescription());
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

    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));

    Annotation local = tsdb.
            getAnnotation(TSUID_ANNOTATION, GLOBAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(local);
    assertEquals("Boo", local.getDescription());

    Annotation new_local = tsdb.
            getAnnotation("000001000001000002", GLOBAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(new_local);
    assertEquals("Gum", new_local.getDescription());
  }
  
  @Test
  public void bulkModifyPutGlobal() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/annotation/bulk",
    "[{\"startTime\":1328140800,\"description\":\"Boo\"},{" + 
    "\"startTime\":1328140801,\"description\":\"Gum\"}]");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String data = query.response().getContent()
      .toString(Charset.forName("UTF-8"));
    assertTrue(data.contains("\"description\":\"Boo\""));
    assertTrue(data.contains("\"notes\":\"\""));
    assertTrue(data.contains("\"startTime\":1328140800"));
    assertTrue(data.contains("\"description\":\"Gum\""));

    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));

    Annotation global = tsdb.
            getAnnotation(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME)
            .joinUninterruptibly();
    checkNotNull(global);
    assertEquals("Boo", global.getDescription());

    Annotation global2 = tsdb.
            getAnnotation(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME)
            .joinUninterruptibly();
    checkNotNull(global2);
    assertEquals("Gum", global2.getDescription());
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

    assertNull(tsdb.getAnnotation(TSUID_ANNOTATION, LOCAL_ONE_START_TIME)
      .joinUninterruptibly());

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));

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

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
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

    assertNull(tsdb.getAnnotation(TSUID_ANNOTATION, LOCAL_ONE_START_TIME)
            .joinUninterruptibly());

    assertNull(tsdb.getAnnotation(TSUID_ANNOTATION, LOCAL_TWO_START_TIME)
            .joinUninterruptibly());

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
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

    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
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

    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
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

    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
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

    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
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

    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_ONE_START_TIME));
    assertTrue(isUnchanged(TSUID_ANNOTATION, LOCAL_TWO_START_TIME));
    assertTrue(isUnchanged(TSUID_GLOBAL_ANNOTATION, GLOBAL_TWO_START_TIME));
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

  private boolean isUnchanged(final String TSUID, final long timestamp)
          throws Exception {
    String tsuid = TSUID;
    if (null == TSUID) tsuid = "";

    Annotation a = tsdb.
            getAnnotation(TSUID, timestamp)
            .joinUninterruptibly();
    checkNotNull(a);
    Annotation original = annotations.get(tsuid, timestamp);
    checkNotNull(original);

    try {
      return original.getDescription().equals(a.getDescription()) &&
              original.getNotes().equals(a.getNotes());
    } catch (NullPointerException e) {
      return false;
    }

  }

}
