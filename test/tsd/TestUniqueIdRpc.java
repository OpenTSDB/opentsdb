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

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.Const;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;

import com.fasterxml.jackson.databind.JsonNode;
import org.hbase.async.*;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSMeta.class, UIDMeta.class,
  RowLock.class, UniqueIdRpc.class, KeyValue.class,
  Scanner.class})
public final class TestUniqueIdRpc {
  private static byte[] NAME_FAMILY = "name".getBytes(Const.CHARSET_ASCII);
  private TSDB tsdb;
  private MemoryStore tsdb_store;
  private UniqueIdRpc rpc = new UniqueIdRpc();
  
  @Before
  public void before() throws Exception {
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("tsd.http.show_stack_trace", "true");
    Config config = new Config(false, properties);

    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);
  }

  @Test
  public void constructor() throws Exception {
    new TestUniqueIdRpc();
  }
  
  @Test (expected = BadRequestException.class)
  public void badMethod() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/uid/assign");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void notImplemented() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/uid");
    this.rpc.execute(tsdb, query);
  }
  
  // Test /api/uid/assign ----------------------
  
  @Test
  public void assignQsMetricSingle() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?metric=sys.cpu.0");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());
    String s = jsonNode.findValue("metric").findValue("sys.cpu.0").asText();
    assertEquals("000001", s);

    assertTrue(jsonNode.findPath("metric_errors").isMissingNode());
    assertTrue(hasNoTagK(jsonNode));
    assertTrue(hasNoTagV(jsonNode));
  }

  @Test
  public void assignQsMetricDouble() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?metric=sys.cpu.0,sys.cpu.1");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());
    String s = jsonNode.findValue("metric").findValue("sys.cpu.0").asText();
    assertEquals("000001", s);
    s = jsonNode.findValue("metric").findValue("sys.cpu.1").asText();
    assertEquals("000002", s);

    assertTrue(jsonNode.findPath("metric_errors").isMissingNode());
    assertTrue(hasNoTagK(jsonNode));
    assertTrue(hasNoTagV(jsonNode));
  }
  
  @Test
  public void assignQsMetricSingleBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/assign?metric=sys.cpu.0");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findValue("metric_errors").findValue("sys.cpu.0")
            .asText();

    assertEquals("Name already exists with UID: 000001", s);
    assertFalse(jsonNode.findValue("metric").elements().hasNext());

    assertTrue(hasNoTagK(jsonNode));
    assertTrue(hasNoTagV(jsonNode));
  }
  
  @Test
  public void assignQsMetric2Good1Bad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?metric=sys.cpu.0,sys.cpu.1,sys.cpu.2");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());

    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());
    String s = jsonNode.findValue("metric_errors").findValue("sys.cpu.0")
            .asText();
    assertEquals("Name already exists with UID: 000001", s);

    s = jsonNode.findValue("metric").findValue("sys.cpu.1").asText();
    assertEquals("000002", s);

    s = jsonNode.findValue("metric").findValue("sys.cpu.2").asText();
    assertEquals("000003", s);

    assertTrue(hasNoTagK(jsonNode));
    assertTrue(hasNoTagV(jsonNode));
  }
  
  @Test
  public void assignQsTagkSingle() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/assign?tagk=host");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findValue("tagk").findValue("host").asText();
    assertEquals("000001", s);

    assertTrue(jsonNode.findPath("tagk_errors").isMissingNode());
    assertTrue(hasNoMetric(jsonNode));
    assertTrue(hasNoTagV(jsonNode));
  }
  
  @Test
  public void assignQsTagkDouble() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/assign?tagk=host,fqdn");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());

    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findValue("tagk").findValue("host").asText();
    assertEquals("000001", s);

    s = jsonNode.findValue("tagk").findValue("fqdn").asText();
    assertEquals("000002", s);

    assertTrue(jsonNode.findPath("tagk_errors").isMissingNode());
    assertTrue(hasNoMetric(jsonNode));
    assertTrue(hasNoTagV(jsonNode));
  }
  
  @Test
  public void assignQsTagkSingleBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagk=datacenter");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    assertFalse(jsonNode.findValue("tagk").elements().hasNext());

    String s = jsonNode.findValue("tagk_errors").findValue("datacenter").asText();

    assertEquals("Name already exists with UID: 000001", s);

    assertTrue(hasNoMetric(jsonNode));
    assertTrue(hasNoTagV(jsonNode));
  }
  
  @Test
    public void assignQsTagk2Good1Bad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagk=host,datacenter,fqdn");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findValue("tagk").findValue("host").asText();
    assertEquals("000002", s);

    s = jsonNode.findValue("tagk").findValue("fqdn").asText();
    assertEquals("000003", s);

    s = jsonNode.findValue("tagk_errors").findValue("datacenter").asText();
    assertEquals("Name already exists with UID: 000001", s);

    assertTrue(hasNoMetric(jsonNode));
    assertTrue(hasNoTagV(jsonNode));
  }
    
  @Test
  public void assignQsTagvSingle() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=localhost");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findValue("tagv").findValue("localhost").asText();
    assertEquals("000001", s);

    assertTrue(jsonNode.findPath("tagv_error").isMissingNode());

    assertTrue(hasNoMetric(jsonNode));
    assertTrue(hasNoTagK(jsonNode));
  }
  
  @Test
  public void assignQsTagvDouble() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=localhost,foo");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findValue("tagv").findValue("localhost").asText();
    assertEquals("000001", s);

    s = jsonNode.findValue("tagv").findValue("foo").asText();
    assertEquals("000002", s);

    assertTrue(jsonNode.findPath("tagv_error").isMissingNode());

    assertTrue(hasNoMetric(jsonNode));
    assertTrue(hasNoTagK(jsonNode));
  }
  
  @Test
  public void assignQsTagvSingleBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=myserver");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("tagv_errors").findPath("myserver").asText();
    assertEquals("Name already exists with UID: 000001", s);

    s = jsonNode.findPath("tagv").asText();
    assertEquals("", s);

    assertTrue(hasNoMetric(jsonNode));
    assertTrue(hasNoTagK(jsonNode));
  }
  
  @Test
  public void assignQsTagv2Good1Bad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/assign?tagv=localhost,myserver,foo");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("tagv_errors").findPath("myserver").asText();
    assertEquals("Name already exists with UID: 000001", s);

    s = jsonNode.findPath("tagv").findPath("foo").asText();
    assertEquals("000003", s);

    s = jsonNode.findPath("tagv").findPath("localhost").asText();
    assertEquals("000002", s);

    assertTrue(hasNoMetric(jsonNode));
    assertTrue(hasNoTagK(jsonNode));
  }
  
  @Test
  public void assignQsFull() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=localhost,foo" + 
        "&metric=sys.cpu.1,sys.cpu.2" +
        "&tagk=host,fqdn");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("tagv").findPath("localhost").asText();
    assertEquals("000002", s);
    s = jsonNode.findPath("tagv").findPath("foo").asText();
    assertEquals("000003", s);

    s = jsonNode.findPath("tagk").findPath("host").asText();
    assertEquals("000002", s);
    s = jsonNode.findPath("tagk").findPath("fqdn").asText();
    assertEquals("000003", s);

    s = jsonNode.findPath("metric").findPath("sys.cpu.1").asText();
    assertEquals("000002", s);
    s = jsonNode.findPath("metric").findPath("sys.cpu.2").asText();
    assertEquals("000003", s);

    assertTrue(jsonNode.findPath("tagv_errors").isMissingNode());
    assertTrue(jsonNode.findPath("tagk_errors").isMissingNode());
    assertTrue(jsonNode.findPath("metric_errors").isMissingNode());
  }
  
  @Test
  public void assignQsFullBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=localhost,myserver,foo" + 
        "&metric=sys.cpu.0,sys.cpu.1,sys.cpu.2" +
        "&tagk=host,datacenter,fqdn");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("tagv").findPath("localhost").asText();
    assertEquals("000002", s);
    s = jsonNode.findPath("tagv").findPath("foo").asText();
    assertEquals("000003", s);

    s = jsonNode.findPath("tagk").findPath("host").asText();
    assertEquals("000002", s);
    s = jsonNode.findPath("tagk").findPath("fqdn").asText();
    assertEquals("000003", s);

    s = jsonNode.findPath("metric").findPath("sys.cpu.1").asText();
    assertEquals("000002", s);
    s = jsonNode.findPath("metric").findPath("sys.cpu.2").asText();
    assertEquals("000003", s);

    s = jsonNode.findPath("tagv_errors").findPath("myserver").asText();
    assertEquals("Name already exists with UID: 000001", s);
    s = jsonNode.findPath("tagk_errors").findPath("datacenter").asText();
    assertEquals("Name already exists with UID: 000001", s);
    s = jsonNode.findPath("metric_errors").findPath("sys.cpu.0").asText();
    assertEquals("Name already exists with UID: 000001", s);
  }
  
  @Test (expected = BadRequestException.class)
  public void assignQsNoParamValue() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=");
    this.rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void assignQsEmpty() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign");
    this.rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void assignQsTypo() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign/metrics=hello");
    this.rpc.execute(tsdb, query);
  }

  @Test
  public void assignPostMetricSingle() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"metric\":[\"sys.cpu.0\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("metric").findPath("sys.cpu.0").asText();
    assertEquals(s, "000001");

    assertTrue(jsonNode.findPath("metric_errors").isMissingNode());
    assertTrue(hasNoTagK(jsonNode));
    assertTrue(hasNoTagV(jsonNode));
  }

  @Test
  public void assignPostMetricDouble() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"metric\":[\"sys.cpu.0\",\"sys.cpu.1\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("metric").findPath("sys.cpu.0").asText();
    assertEquals("000001", s);

    s = jsonNode.findPath("metric").findPath("sys.cpu.1").asText();
    assertEquals("000002", s);
  }

  @Test
  public void assignPostMetricSingleBad() throws Exception {
    setupAssign();

    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"metric\":[\"sys.cpu.0\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("metric_errors").findPath("sys.cpu.0").asText();
    assertEquals("Name already exists with UID: 000001", s);

    s = jsonNode.findPath("metric").asText();
    assertEquals("", s);
  }

  @Test
  public void assignPostMetric2Good1Bad() throws Exception {
    setupAssign();

    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"metric\":[\"sys.cpu.0\",\"sys.cpu.1\",\"sys.cpu.2\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("metric_errors").findPath("sys.cpu.0").asText();
    assertEquals("Name already exists with UID: 000001", s);

    s = jsonNode.findPath("metric").findPath("sys.cpu.1").asText();
    assertEquals("000002", s);

    s = jsonNode.findPath("metric").findPath("sys.cpu.2").asText();
    assertEquals("000003", s);
  }

  @Test
  public void assignPostTagkSingle() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"tagk\":[\"host\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("tagk").findPath("host").asText();
    assertEquals("000001", s);

    assertTrue(hasNoTagV(jsonNode));
    assertTrue(hasNoMetric(jsonNode));
  }
  @Test
  public void assignPostTagkDouble() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagk\":[\"host\",\"fqdn\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("tagk").findPath("host").asText();
    assertEquals("000002", s);
    s = jsonNode.findPath("tagk").findPath("fqdn").asText();
    assertEquals("000003", s);

    assertTrue(hasNoTagV(jsonNode));
    assertTrue(hasNoMetric(jsonNode));
  }
  @Test
  public void assignPostTagkSingleBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagk\":[\"datacenter\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    assertFalse(jsonNode.findValue("tagk").elements().hasNext());

    String s = jsonNode.findPath("tagk_errors").findPath("datacenter").asText();
    assertEquals("Name already exists with UID: 000001", s);

    assertTrue(hasNoTagV(jsonNode));
    assertTrue(hasNoMetric(jsonNode));
  }
  @Test
  public void assignPostTagk2Good1Bad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagk\":[\"host\",\"datacenter\",\"fqdn\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("tagk_errors").findPath("datacenter").asText();
    assertEquals("Name already exists with UID: 000001", s);

    s = jsonNode.findPath("tagk").findPath("host").asText();
    assertEquals("000002", s);

    s = jsonNode.findPath("tagk").findPath("fqdn").asText();
    assertEquals("000003", s);

    assertTrue(hasNoTagV(jsonNode));
    assertTrue(hasNoMetric(jsonNode));
  }

  @Test
  public void assignPostTagvSingle() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"tagv\":[\"localhost\"]}");
    this.rpc.execute(tsdb, query);
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("000001", jsonNode.findPath("tagv").findPath("localhost")
            .asText());

    assertTrue(hasNoTagK(jsonNode));
    assertTrue(hasNoMetric(jsonNode));
  }
  @Test
  public void assignPostTagvDouble() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagv\":[\"localhost\",\"foo\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    assertEquals("000002", jsonNode.findPath("tagv").findPath("localhost")
            .asText());

    assertEquals("000003", jsonNode.findPath("tagv").findPath("foo")
            .asText());

    assertTrue(hasNoTagK(jsonNode));
    assertTrue(hasNoMetric(jsonNode));
  }
  @Test
  public void assignPostTagvSingleBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagv\":[\"myserver\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    assertFalse(jsonNode.findValue("tagv").elements().hasNext());

    assertEquals("Name already exists with UID: 000001",
            jsonNode.findPath("tagv_errors").findPath("myserver").asText());

    assertTrue(hasNoTagK(jsonNode));
    assertTrue(hasNoMetric(jsonNode));
  }

  @Test
  public void assignPostTagv2Good1Bad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagv\":[\"localhost\",\"myserver\",\"foo\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    JsonNode jsonNode = RpcJSONUtils.parseResponse(query.response());

    String s = jsonNode.findPath("tagv_errors").findPath("myserver").asText();
    assertEquals(s, "Name already exists with UID: 000001");

    s = jsonNode.findPath("tagv").findPath("localhost").asText();
    assertEquals(s, "000002");

    s = jsonNode.findPath("tagv").findPath("foo").asText();
    assertEquals(s, "000003");

    assertTrue(hasNoTagK(jsonNode));
    assertTrue(hasNoMetric(jsonNode));
  }

  @Test
  public void assignPostFull() throws Exception {
   setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"tagv\":[\"localhost\",\"foo\"],"
        + "\"metric\":[\"sys.cpu.1\",\"sys.cpu.2\"],"
        + "\"tagk\":[\"host\",\"fqdn\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());

    JsonNode node = RpcJSONUtils.parseResponse(query.response()).findPath("tagv");
    assertEquals("000002", node.findPath("localhost").asText());
    assertEquals("000003", node.findPath("foo").asText());

    node = RpcJSONUtils.parseResponse(query.response()).findPath("metric");
    assertEquals("000002", node.findPath("sys.cpu.1").asText());
    assertEquals("000003", node.findPath("sys.cpu.2").asText());

    node = RpcJSONUtils.parseResponse(query.response()).findPath("tagk");
    assertEquals("000002", node.findPath("host").asText());
    assertEquals("000003", node.findPath("fqdn").asText());

  }
  
  @Test
  public void assignPostFullBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"tagv\":[\"localhost\",\"myserver\",\"foo\"],"
        + "\"metric\":[\"sys.cpu.0\",\"sys.cpu.1\",\"sys.cpu.2\"],"
        + "\"tagk\":[\"host\",\"datacenter\",\"fqdn\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    // contents may shift in flight, hence no parsing
    JsonNode node = RpcJSONUtils.parseResponse(query.response()).findPath("tagv");
    assertEquals("000002", node.findPath("localhost").asText());
    assertEquals("000003", node.findPath("foo").asText());

    node = RpcJSONUtils.parseResponse(query.response()).findPath("metric");
    assertEquals("000002", node.findPath("sys.cpu.1").asText());
    assertEquals("000003", node.findPath("sys.cpu.2").asText());

    node = RpcJSONUtils.parseResponse(query.response()).findPath("tagk");
    assertEquals("000002", node.findPath("host").asText());
    assertEquals("000003", node.findPath("fqdn").asText());

    node = RpcJSONUtils.parseResponse(query.response()).findPath("tagv_errors");
    assertEquals("Name already exists with UID: 000001",
            node.findPath("myserver").asText());

    node = RpcJSONUtils.parseResponse(query.response()).findPath("metric_errors");
    assertEquals("Name already exists with UID: 000001",
            node.findPath("sys.cpu.0").asText());

    node = RpcJSONUtils.parseResponse(query.response()).findPath("tagk_errors");
    assertEquals("Name already exists with UID: 000001",
            node.findPath("datacenter").asText());

  }

  @Test (expected = BadRequestException.class)
  public void assignPostBadJSON() throws Exception {
    setupAssign();
    // missing a quotation mark
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"tagv\":[\"localhost\",myserver\",\"foo\"],"
        + "\"metric\":[\"sys.cpu.0\",\"sys.cpu.1\",\"sys.cpu.2\"],"
        + "\"tagk\":[\"host\",\"datacenter\",\"fqdn\"]}");
    this.rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void assignPostNotJSON() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", "Hello");
    this.rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void assignPostNoContent() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", "");
    this.rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void assignPostEmptyJSON() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", "{}");
    this.rpc.execute(tsdb, query);
  }

  @Test
  public void stringToUniqueIdTypeMetric() throws Exception {
    setupAssign();
    assertEquals(UniqueIdType.METRIC, UniqueId.stringToUniqueIdType("Metric"));
  }
  
  @Test
  public void stringToUniqueIdTypeTagk() throws Exception {
    setupAssign();
    assertEquals(UniqueIdType.TAGK, UniqueId.stringToUniqueIdType("TagK"));
  }
  
  @Test
  public void stringToUniqueIdTypeTagv() throws Exception {
    setupAssign();
    assertEquals(UniqueIdType.TAGV, UniqueId.stringToUniqueIdType("TagV"));
  }
  
  @Test (expected = NullPointerException.class)
  public void stringToUniqueIdTypeNull() throws Exception {
    UniqueId.stringToUniqueIdType(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToUniqueIdTypeEmpty() throws Exception {
    UniqueId.stringToUniqueIdType("");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToUniqueIdTypeInvalid() throws Exception {setupAssign();
    UniqueId.stringToUniqueIdType("Not a type");
  }

  // Teset /api/uid/uidmeta --------------------
  
  @Test
  public void uidGet() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?type=metric&uid=000001");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test (expected = BadRequestException.class)
  public void uidGetNoUID() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?type=metric");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void uidGetNoType() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?uid=000001");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void uidGetNSU() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?type=metric&uid=000002");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void uidPost() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void uidPostNotModified() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"type\":\"metric\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test (expected = BadRequestException.class)
  public void uidPostMissingUID() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/uidmeta", 
        "{\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void uidPostMissingType() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void uidPostNSU() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000002\",\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void uidPostQS() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?uid=000001&type=metric&display_name=Hello&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void uidPut() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void uidPutNotModified() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"type\":\"metric\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test (expected = BadRequestException.class)
  public void uidPutMissingUID() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/uidmeta", 
        "{\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void uidPutMissingType() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void uidPutNSU() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000002\",\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void uidPutQS() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?uid=000001&type=metric&display_name=Hello&method_override=put");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void uidDelete() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.deleteQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }

  @Test (expected = BadRequestException.class)
  public void uidDeleteMissingUID() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.deleteQuery(tsdb, "/api/uid/uidmeta", 
        "{\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void uidDeleteMissingType() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.deleteQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }

  @Test
  public void uidDeleteQS() throws Exception {
    setupUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?uid=000001&type=metric&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  // Test /api/uid/tsmeta ----------------------
  
  @Test
  public void tsuidGet() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/tsmeta?tsuid=000001000001000001");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void tsuidGetByM() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/tsmeta?m=sys.cpu.0{host=web01}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void tsuidPostByM() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/tsmeta?m=sys.cpu.0{host=web02}&create=true", 
        "{\"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidPostByMNoCreate() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/tsmeta?m=sys.cpu.0{host=web02}", 
        "{\"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void tsuidGetByMMultiTagWrongOrder() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/tsmeta?m=sys.cpu.2{datacenter=dc01,host=web01}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void tsuidGetByMMultiTag() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/tsmeta?m=sys.cpu.2{host=web01,datacenter=dc01}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidGetByMEmpty() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/tsmeta?m=");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidGetByMBadSyntax() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/tsmeta?m=sys.cpu.0{datacenter=dc");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidGetNotFound() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/tsmeta?tsuid=000001000001000002");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidGetMissingTSUID() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/tsmeta");
    rpc.execute(tsdb, query);
  }

  @Test
  public void tsuidPost() throws Exception {  
    setupTSUID();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/tsmeta", 
        "{\"tsuid\":\"000001000001000001\", \"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(Charset.forName("UTF-8"))
        .contains("\"displayName\":\"Hello World\""));    
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidPostNoTSUID() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/tsmeta", 
        "{\"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void tsuidPostNotModified() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/tsmeta", 
        "{\"tsuid\":\"000001000001000001\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test
  public void tsuidPostQS() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/uid/tsmeta?tsuid=000001000001000001&display_name=42&method_override=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(Charset.forName("UTF-8"))
        .contains("\"displayName\":\"42\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidPostQSNoTSUID() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/uid/tsmeta?display_name=42&method_override=post");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void tsuidPut() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/tsmeta", 
        "{\"tsuid\":\"000001000001000001\", \"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(Charset.forName("UTF-8"))
        .contains("\"displayName\":\"Hello World\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidPutNoTSUID() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/tsmeta", 
        "{\"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void tsuidPutNotModified() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/tsmeta", 
        "{\"tsuid\":\"000001000001000001\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test
  public void tsuidPutQS() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/uid/tsmeta?tsuid=000001000001000001&display_name=42&method_override=put");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(Charset.forName("UTF-8"))
        .contains("\"displayName\":\"42\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidPutQSNoTSUID() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/uid/tsmeta?display_name=42&method_override=put");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void tsuidDelete() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.deleteQuery(tsdb, "/api/uid/tsmeta", 
        "{\"tsuid\":\"000001000001000001\", \"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void tsuidDeleteQS() throws Exception {
    setupTSUID();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/uid/tsmeta?tsuid=000001000001000001&method_override=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  /**
   * Sets up common mocks for UID assignment tests
   * @throws Exception if something goes pear shaped
   */
  private void setupAssign() throws Exception {
    tsdb_store.allocateUID("sys.cpu.0", UniqueIdType.METRIC);
    tsdb_store.allocateUID("myserver", UniqueIdType.TAGV);
    tsdb_store.allocateUID("datacenter", UniqueIdType.TAGK);
  }
  
  /**
   * Sets up common mocks for UID tests
   * @throws Exception if something goes pear shaped
   */
  private void setupUID() throws Exception {
    tsdb_store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, UniqueIdType.METRIC);
    tsdb_store.allocateUID("sys.cpu.2", new byte[]{0, 0, 3}, UniqueIdType.METRIC);

    UIDMeta meta = new UIDMeta(
            UniqueIdType.METRIC,
            new byte[]{0, 0, 1},
            "sys.cpu.0");
    meta.setDisplayName("System CPU");
    meta.setDescription("Description");
    meta.setNotes("MyNotes");
    meta.setCreated(1328140801);

    tsdb_store.add(meta);
  }

  /**
   * Sets up common mocks for TSUID tests
   * @throws Exception if something goes pear shaped
   */
  private void setupTSUID() throws Exception {
    tsdb_store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, UniqueIdType.METRIC);
    tsdb_store.allocateUID("sys.cpu.2", new byte[] {0, 0, 2}, UniqueIdType.METRIC);

    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, UniqueIdType.TAGK);
    tsdb_store.allocateUID("datacenter", new byte[] {0, 0, 2}, UniqueIdType.TAGK);

    tsdb_store.allocateUID("web01", new byte[]{0, 0, 1}, UniqueIdType.TAGV);
    tsdb_store.allocateUID("web02", new byte[]{0, 0, 3}, UniqueIdType.TAGV);

    tsdb_store.addColumn(new byte[] { 0, 0, 1 },
        NAME_FAMILY,
        "metric_meta".getBytes(Const.CHARSET_ASCII),
        ("{\"uid\":\"000001\",\"type\":\"METRIC\",\"name\":\"sys.cpu.0\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"System CPU\"}").getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[] { 0, 0, 2 },
        "metric_meta".getBytes(Const.CHARSET_ASCII),
        ("{\"uid\":\"000002\",\"type\":\"METRIC\",\"name\":\"sys.cpu.2\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"System CPU\"}").getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[] { 0, 0, 1 },
        NAME_FAMILY,
        "tagk_meta".getBytes(Const.CHARSET_ASCII),
        ("{\"uid\":\"000001\",\"type\":\"TAGK\",\"name\":\"host\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Host server name\"}").getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[] { 0, 0, 2 },
        "tagk_meta".getBytes(Const.CHARSET_ASCII),
        ("{\"uid\":\"000002\",\"type\":\"TAGK\",\"name\":\"datacenter\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Host server name\"}").getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[] { 0, 0, 1 },
        NAME_FAMILY,
        "tagv_meta".getBytes(Const.CHARSET_ASCII),
        ("{\"uid\":\"000001\",\"type\":\"TAGV\",\"name\":\"web01\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Web server 1\"}").getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[] { 0, 0, 3 },
        "tagv_meta".getBytes(Const.CHARSET_ASCII),
        ("{\"uid\":\"000003\",\"type\":\"TAGV\",\"name\":\"web02\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Web server 1\"}").getBytes(Const.CHARSET_ASCII));
    tsdb_store.allocateUID("dc01", new byte[]{0, 0, 2}, UniqueIdType.TAGV);
    tsdb_store.addColumn(new byte[] { 0, 0, 2 },
        "tagv_meta".getBytes(Const.CHARSET_ASCII),
        ("{\"uid\":\"000002\",\"type\":\"TAGV\",\"name\":\"dc01\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Web server 1\"}").getBytes(Const.CHARSET_ASCII));

    tsdb_store.addColumn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 },
        NAME_FAMILY,
        "ts_meta".getBytes(Const.CHARSET_ASCII),
        ("{\"tsuid\":\"000001000001000001\",\"displayName\":\"Display\"," +
            "\"description\":\"Description\",\"notes\":\"Notes\",\"created" +
            "\":1366671600,\"custom\":null,\"units\":\"\",\"dataType\":" +
            "\"Data\",\"retention\":42,\"max\":1.0,\"min\":\"NaN\"}")
            .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 },
        NAME_FAMILY,
        "ts_ctr".getBytes(Const.CHARSET_ASCII),
        Bytes.fromLong(1L));
    tsdb_store.addColumn(new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 1, 0, 0, 2, 0, 0, 2 },
        NAME_FAMILY,
        "ts_meta".getBytes(Const.CHARSET_ASCII),
        ("{\"tsuid\":\"000002000001000001000002000002\",\"displayName\":\"Display\"," +
            "\"description\":\"Description\",\"notes\":\"Notes\",\"created" +
            "\":1366671600,\"custom\":null,\"units\":\"\",\"dataType\":" +
            "\"Data\",\"retention\":42,\"max\":1.0,\"min\":\"NaN\"}")
            .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 1, 0, 0, 2, 0, 0, 2 },
        NAME_FAMILY,
        "ts_ctr".getBytes(Const.CHARSET_ASCII),
        Bytes.fromLong(1L));
    tsdb_store.addColumn(new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 3, 0, 0, 2, 0, 0, 2 },
        NAME_FAMILY,
        "ts_meta".getBytes(Const.CHARSET_ASCII),
        ("{\"tsuid\":\"000002000001000003000002000002\",\"displayName\":\"Display\"," +
            "\"description\":\"Description\",\"notes\":\"Notes\",\"created" +
            "\":1366671600,\"custom\":null,\"units\":\"\",\"dataType\":" +
            "\"Data\",\"retention\":42,\"max\":1.0,\"min\":\"NaN\"}")
            .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 3, 0, 0, 2, 0, 0, 2 },
        NAME_FAMILY,
        "ts_ctr".getBytes(Const.CHARSET_ASCII),
        Bytes.fromLong(1L));
  }
  private boolean hasNoTagK(JsonNode jsonNode) {
    return (jsonNode.findPath("tagk").isMissingNode() &&
            jsonNode.findPath("tagk_errors").isMissingNode());
  }
  private boolean hasNoTagV(JsonNode jsonNode) {
    return (jsonNode.findPath("tagv").isMissingNode() &&
            jsonNode.findPath("tagv_errors").isMissingNode());
  }
  private boolean hasNoMetric(JsonNode jsonNode) {
    return (jsonNode.findPath("metric").isMissingNode() &&
            jsonNode.findPath("metric_errors").isMissingNode());
  }
}
