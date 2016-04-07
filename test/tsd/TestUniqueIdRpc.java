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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.RowLock;
import org.hbase.async.Scanner;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, TSMeta.class, UIDMeta.class, 
  HBaseClient.class, RowLock.class, UniqueIdRpc.class, KeyValue.class, 
  GetRequest.class, Scanner.class, UniqueId.class})
public final class TestUniqueIdRpc {
  private final static byte[] NAME_FAMILY = "name".getBytes(MockBase.ASCII());
  private final static byte[] UID_TABLE = "tsdb-uid".getBytes(MockBase.ASCII());
  private final static byte[] META_TABLE = "tsdb-meta".getBytes();
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private MockBase storage;
  private UniqueIdRpc rpc = new UniqueIdRpc();
  
  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();
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
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?metric=sys.cpu.0");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"metric\":{\"sys.cpu.0\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsMetricDouble() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?metric=sys.cpu.0,sys.cpu.2");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(
        "{\"metric\":{\"sys.cpu.0\":\"000001\",\"sys.cpu.2\":\"000003\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsMetricSingleBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?metric=sys.cpu.1");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"metric_errors\":{\"sys.cpu.1\":\"Name already exists with " 
        + "UID: 000002\"},\"metric\":{}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsMetric2Good1Bad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?metric=sys.cpu.0,sys.cpu.1,sys.cpu.2");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"metric_errors\":{\"sys.cpu.1\":\"Name already exists with "
        + "UID: 000002\"},\"metric\":{\"sys.cpu.0\":\"000001\",\"sys.cpu.2\":"
        + "\"000003\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsTagkSingle() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagk=host");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagk\":{\"host\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsTagkDouble() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagk=host,fqdn");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(
        "{\"tagk\":{\"fqdn\":\"000003\",\"host\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsTagkSingleBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagk=datacenter");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"tagk_errors\":{\"datacenter\":\"Name already exists with " 
        + "UID: 000002\"},\"tagk\":{}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsTagk2Good1Bad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagk=host,datacenter,fqdn");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"tagk_errors\":{\"datacenter\":\"Name already exists with "
        + "UID: 000002\"},\"tagk\":{\"fqdn\":\"000003\",\"host\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
    
  @Test
  public void assignQsTagvSingle() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=localhost");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagv\":{\"localhost\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsTagvDouble() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=localhost,foo");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(
        "{\"tagv\":{\"foo\":\"000003\",\"localhost\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsTagvSingleBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=myserver");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"tagv\":{},\"tagv_errors\":{\"myserver\":\"Name already "
        + "exists with UID: 000002\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsTagv2Good1Bad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=localhost,myserver,foo");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"tagv\":{\"foo\":\"000003\",\"localhost\":\"000001\"},"
        + "\"tagv_errors\":{\"myserver\":\"Name already exists with "
        + "UID: 000002\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsFull() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=localhost,foo" + 
        "&metric=sys.cpu.0,sys.cpu.2" +
        "&tagk=host,fqdn");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    // contents may shift in flight, hence no parsing
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
    // contents may shift in flight, hence no parsing
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
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"metric\":[\"sys.cpu.0\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"metric\":{\"sys.cpu.0\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostMetricDouble() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"metric\":[\"sys.cpu.0\",\"sys.cpu.2\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(
        "{\"metric\":{\"sys.cpu.0\":\"000001\",\"sys.cpu.2\":\"000003\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostMetricSingleBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"metric\":[\"sys.cpu.2\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"metric_errors\":{\"sys.cpu.1\":\"Name already exists with " 
        + "UID: 000002\"},\"metric\":{}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostMetric2Good1Bad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"metric\":[\"sys.cpu.0\",\"sys.cpu.1\",\"sys.cpu.2\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"metric_errors\":{\"sys.cpu.1\":\"Name already exists with "
        + "UID: 000002\"},\"metric\":{\"sys.cpu.0\":\"000001\",\"sys.cpu.2\":"
        + "\"000003\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void assignPostTagkSingle() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"tagk\":[\"host\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagk\":{\"host\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagkDouble() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagk\":[\"host\",\"fqdn\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(
        "{\"tagk\":{\"fqdn\":\"000003\",\"host\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagkSingleBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagk\":[\"datacenter\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagk_errors\":{\"datacenter\":\"Name already exists with " 
        + "UID: 000002\"},\"tagk\":{}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagk2Good1Bad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagk\":[\"host\",\"datacenter\",\"fqdn\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagk_errors\":{\"datacenter\":\"Name already exists with "
        + "UID: 000002\"},\"tagk\":{\"fqdn\":\"000003\",\"host\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void assignPostTagvSingle() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"tagv\":[\"localhost\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagv\":{\"localhost\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagvDouble() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagv\":[\"localhost\",\"foo\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(
        "{\"tagv\":{\"foo\":\"000003\",\"localhost\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagvSingleBad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagv\":[\"myserver\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagv\":{},\"tagv_errors\":{\"myserver\":\"Name already "
        + "exists with UID: 000002\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagv2Good1Bad() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagv\":[\"localhost\",\"myserver\",\"foo\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagv\":{\"foo\":\"000003\",\"localhost\":\"000001\"},"
        + "\"tagv_errors\":{\"myserver\":\"Name already exists with "
        + "UID: 000002\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void assignPostFull() throws Exception {
    setupAssign();
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"tagv\":[\"localhost\",\"foo\"],"
        + "\"metric\":[\"sys.cpu.0\",\"sys.cpu.2\"],"
        + "\"tagk\":[\"host\",\"fqdn\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    // contents may shift in flight, hence no parsing
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
    setupAssign();
    UniqueId.stringToUniqueIdType(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToUniqueIdTypeEmpty() throws Exception {
    setupAssign();
    UniqueId.stringToUniqueIdType("");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToUniqueIdTypeInvalid() throws Exception {setupAssign();
    UniqueId.stringToUniqueIdType("Not a type");
  }

  // Test /api/uid/rename ----------------------

  @Test (expected = BadRequestException.class)
  public void renameBadMethod() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/rename", "");
    rpc.execute(tsdb, query);
  }

  @Test
  public void renamePostMetric() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/rename",
        "{\"metric\":\"sys.cpu.1\",\"name\":\"sys.cpu.2\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"result\":\"true\"}",
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void renamePostTagk() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/rename",
        "{\"tagk\":\"datacenter\",\"name\":\"datacluster\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"result\":\"true\"}",
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void renamePostTagv() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/rename",
        "{\"tagv\":\"localhost\",\"name\":\"127.0.0.1\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"result\":\"true\"}",
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test (expected = BadRequestException.class)
  public void renamePostNoName() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/rename",
        "{\"tagk\":\"localhost\",\"not_name\":\"127.0.0.1\"}");
    rpc.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void renamePostNoType() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/rename",
        "{\"name\":\"127.0.0.1\"}");
    rpc.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void renamePostNotJSON() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/rename", "Not JSON");
    rpc.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void renamePostZeroLengthContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/rename", "");
    rpc.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void renamePostEmptyJSON() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/rename", "{}");
    rpc.execute(tsdb, query);
  }

  @Test
  public void renameQsMetric() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/rename?metric=sys.cpu.1&name=sys.cpu.2");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"result\":\"true\"}",
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void renameQsTagk() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/rename?tagk=datacenter&name=datacluster");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"result\":\"true\"}",
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void renameQsTagv() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/rename?tagv=localhost&name=127.0.0.1");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"result\":\"true\"}",
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void renameQsSkipUnsupportedParam() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/rename?tagv=localhost&name=127.0.0.1&drop=db");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"result\":\"true\"}",
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test (expected = BadRequestException.class)
  public void renameQsMissingType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/rename?name=127.0.0.1");
    rpc.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void renameQsMissingName() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/rename?metric=sys.cpu.1");
    rpc.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void renameQsNoParamValue() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/rename?metric=&name=sys.cpu.2");
    rpc.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void renameQsNoParam() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/rename?");
    rpc.execute(tsdb, query);
  }

  @Test
  public void renameRenameException() throws Exception {
    final String message = "New name already exists";
    doThrow(new IllegalArgumentException(message)).when(tsdb).renameUid("tagv",
        "localhost", "localhost");
    HttpQuery query = NettyMocks.getQuery(tsdb,
        "/api/uid/rename?tagv=localhost&name=localhost");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"error\":\"" + message + "\",\"result\":\"false\"}",
        query.response().getContent().toString(Charset.forName("UTF-8")));
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
    when(tsdb.assignUid("metric", "sys.cpu.0")).thenReturn(new byte[] { 0, 0, 1 });
    when(tsdb.assignUid("metric", "sys.cpu.1")).thenThrow(
        new IllegalArgumentException("Name already exists with UID: 000002"));
    when(tsdb.assignUid("metric", "sys.cpu.2")).thenReturn(new byte[] { 0, 0, 3 });
    
    when(tsdb.assignUid("tagk", "host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tsdb.assignUid("tagk", "datacenter")).thenThrow(
        new IllegalArgumentException("Name already exists with UID: 000002"));
    when(tsdb.assignUid("tagk", "fqdn")).thenReturn(new byte[] { 0, 0, 3 });
    
    when(tsdb.assignUid("tagv", "localhost")).thenReturn(new byte[] { 0, 0, 1 });
    when(tsdb.assignUid("tagv", "myserver")).thenThrow(
        new IllegalArgumentException("Name already exists with UID: 000002"));
    when(tsdb.assignUid("tagv", "foo")).thenReturn(new byte[] { 0, 0, 3 });
    
    // setup UIDMeta objects for testing
    UIDMeta metric = new UIDMeta(UniqueIdType.METRIC, new byte[] {0, 0, 1}, 
        "sys.cpu.0");
    metric.setDisplayName("System CPU");
    UIDMeta tagk = new UIDMeta(UniqueIdType.TAGK, new byte[] {0, 0, 1}, 
        "host");
    tagk.setDisplayName("Server Name");
    UIDMeta tagv = new UIDMeta(UniqueIdType.TAGV, new byte[] {0, 0, 1}, 
        "web01");
    tagv.setDisplayName("Web Server 1");
  }
  
  /**
   * Sets up common mocks for UID tests
   * @throws Exception if something goes pear shaped
   */
  private void setupUID() throws Exception {
    final Config config = new Config(false);
    tsdb = new TSDB(client, config);
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 1 }, 
        NAME_FAMILY,
        "metrics".getBytes(MockBase.ASCII()), 
        "sys.cpu.0".getBytes(MockBase.ASCII()));

    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 3 }, 
        NAME_FAMILY,
        "metrics".getBytes(MockBase.ASCII()), 
        "sys.cpu.2".getBytes(MockBase.ASCII()));

    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 1 }, 
        NAME_FAMILY,
        "metric_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000001\",\"type\":\"METRIC\",\"name\":\"sys.cpu.0\"," +
        "\"displayName\":\"System CPU\",\"description\":\"Description\"," +
        "\"notes\":\"MyNotes\",\"created\":1328140801,\"custom\":null}")
        .getBytes(MockBase.ASCII()));
  }

  /**
   * Sets up common mocks for TSUID tests
   * @throws Exception if something goes pear shaped
   */
  private void setupTSUID() throws Exception {
    final Config config = new Config(false);
    tsdb = new TSDB(client, config);
    
    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);
    
    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);
    
    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);
    
    storage = new MockBase(tsdb, client, true, true, true, true);
    final List<byte[]> families = new ArrayList<byte[]>(1);
    families.add(TSMeta.FAMILY);
    storage.addTable(META_TABLE, families);
    
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 1 },
        NAME_FAMILY,
        "metrics".getBytes(MockBase.ASCII()),
        "sys.cpu.0".getBytes(MockBase.ASCII()));
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 1 }, 
        NAME_FAMILY,
        "metric_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000001\",\"type\":\"METRIC\",\"name\":\"sys.cpu.0\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"System CPU\"}").getBytes(MockBase.ASCII()));   
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "metrics".getBytes(MockBase.ASCII()),
        "sys.cpu.2".getBytes(MockBase.ASCII()));
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "metric_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000002\",\"type\":\"METRIC\",\"name\":\"sys.cpu.2\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"System CPU\"}").getBytes(MockBase.ASCII()));
    
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 1 },
        NAME_FAMILY,
        "tagk".getBytes(MockBase.ASCII()),
        "host".getBytes(MockBase.ASCII()));
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 1 }, 
        NAME_FAMILY,
        "tagk_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000001\",\"type\":\"TAGK\",\"name\":\"host\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Host server name\"}").getBytes(MockBase.ASCII()));
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagk".getBytes(MockBase.ASCII()),
        "datacenter".getBytes(MockBase.ASCII()));
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagk_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000002\",\"type\":\"TAGK\",\"name\":\"datacenter\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Host server name\"}").getBytes(MockBase.ASCII()));

    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 1 }, 
        NAME_FAMILY,
        "tagv".getBytes(MockBase.ASCII()),
        "web01".getBytes(MockBase.ASCII()));
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 1 }, 
        NAME_FAMILY,
        "tagv_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000001\",\"type\":\"TAGV\",\"name\":\"web01\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Web server 1\"}").getBytes(MockBase.ASCII()));
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 3 }, NAME_FAMILY,
        "tagv".getBytes(MockBase.ASCII()),
        "web02".getBytes(MockBase.ASCII()));
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 3 }, NAME_FAMILY,
        "tagv_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000003\",\"type\":\"TAGV\",\"name\":\"web02\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Web server 1\"}").getBytes(MockBase.ASCII()));
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagv".getBytes(MockBase.ASCII()),
        "dc01".getBytes(MockBase.ASCII()));
    storage.addColumn(UID_TABLE, new byte[] { 0, 0, 2 }, NAME_FAMILY,
        "tagv_meta".getBytes(MockBase.ASCII()), 
        ("{\"uid\":\"000002\",\"type\":\"TAGV\",\"name\":\"dc01\"," +
        "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" + 
        "1328140801,\"displayName\":\"Web server 1\"}").getBytes(MockBase.ASCII()));

    storage.addColumn(META_TABLE, new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 },
        TSMeta.FAMILY,
        "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000001000001000001\",\"displayName\":\"Display\"," +
            "\"description\":\"Description\",\"notes\":\"Notes\",\"created" +
            "\":1366671600,\"custom\":null,\"units\":\"\",\"dataType\":" +
            "\"Data\",\"retention\":42,\"max\":1.0,\"min\":\"NaN\"}")
            .getBytes(MockBase.ASCII()));
    storage.addColumn(META_TABLE, new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 },
        TSMeta.FAMILY,
        "ts_ctr".getBytes(MockBase.ASCII()),
        Bytes.fromLong(1L));

    storage.addColumn(META_TABLE, 
        new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 1, 0, 0, 2, 0, 0, 2 },
        TSMeta.FAMILY,
        "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000002000001000001000002000002\",\"displayName\":\"Display\"," +
            "\"description\":\"Description\",\"notes\":\"Notes\",\"created" +
            "\":1366671600,\"custom\":null,\"units\":\"\",\"dataType\":" +
            "\"Data\",\"retention\":42,\"max\":1.0,\"min\":\"NaN\"}")
            .getBytes(MockBase.ASCII()));
    storage.addColumn(META_TABLE, 
        new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 1, 0, 0, 2, 0, 0, 2 },
        TSMeta.FAMILY,
        "ts_ctr".getBytes(MockBase.ASCII()),
        Bytes.fromLong(1L));
    storage.addColumn(META_TABLE, 
        new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 3, 0, 0, 2, 0, 0, 2 },
        TSMeta.FAMILY,
        "ts_meta".getBytes(MockBase.ASCII()),
        ("{\"tsuid\":\"000002000001000003000002000002\",\"displayName\":\"Display\"," +
            "\"description\":\"Description\",\"notes\":\"Notes\",\"created" +
            "\":1366671600,\"custom\":null,\"units\":\"\",\"dataType\":" +
            "\"Data\",\"retention\":42,\"max\":1.0,\"min\":\"NaN\"}")
            .getBytes(MockBase.ASCII()));
    storage.addColumn(META_TABLE, 
        new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 3, 0, 0, 2, 0, 0, 2 },
        TSMeta.FAMILY,
        "ts_ctr".getBytes(MockBase.ASCII()),
        Bytes.fromLong(1L));

    when(metrics.getId("sys.cpu.0")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getIdAsync("sys.cpu.0")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(metrics.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("sys.cpu.0"));
    when(metrics.getId("sys.cpu.2")).thenReturn(new byte[] { 0, 0, 2 });
    when(metrics.getIdAsync("sys.cpu.2")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(metrics.getNameAsync(new byte[] { 0, 0, 2 }))
      .thenReturn(Deferred.fromResult("sys.cpu.2"));

    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getIdAsync("host")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_names.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("host"));
    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getIdAsync("web01")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("web01"));
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 3 });
    when(tag_values.getIdAsync("web02")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 3 }));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 3 }))
      .thenReturn(Deferred.fromResult("web02"));

    when(tag_names.getId("datacenter")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_names.getIdAsync("datacenter")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(tag_names.getNameAsync(new byte[] { 0, 0, 2 }))
      .thenReturn(Deferred.fromResult("datacenter"));
    when(tag_values.getId("dc01")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getIdAsync("dc01")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 2 }))
      .thenReturn(Deferred.fromResult("dc01"));
    
  }
}
