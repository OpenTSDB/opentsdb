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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

import org.hbase.async.HBaseClient;
import org.hbase.async.RowLock;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, TSMeta.class, UIDMeta.class, 
  HBaseClient.class, RowLock.class})
public final class TestUniqueIdRpc {
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueIdRpc rpc = new UniqueIdRpc();

  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();

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
    
    TSMeta tsmeta = new TSMeta("000001000001000001");
    // hack the private fields to put the UIDMetas in the TSMeta object
    final Field uid_metric = TSMeta.class.getDeclaredField("metric");
    uid_metric.setAccessible(true);
    uid_metric.set(tsmeta, metric);
    uid_metric.setAccessible(false);
    
    final ArrayList<UIDMeta> tags = new ArrayList<UIDMeta>(2);
    tags.add(tagk);
    tags.add(tagv);
    final Field uid_tags = TSMeta.class.getDeclaredField("tags");
    uid_tags.setAccessible(true);
    uid_tags.set(tsmeta, tags);
    uid_tags.setAccessible(false);
    
    // warning: Mock the statics AFTER reflection or we can't hack the fields
    PowerMockito.mockStatic(UIDMeta.class);
    PowerMockito.mockStatic(TSMeta.class);
    
    when(TSMeta.getTSMeta(tsdb, "000001000001000001")).thenReturn(tsmeta);
    when(TSMeta.getTSMeta(tsdb, "000001000001000002")).thenReturn(null);
    
    when(UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000001"))
      .thenReturn(metric);
    when(UIDMeta.getUIDMeta(tsdb, UniqueIdType.METRIC, "000002"))
      .thenThrow(new NoSuchUniqueName("metric", "sys.cpu.1"));
    
    when(tsdb.getUidName(UniqueIdType.METRIC, new byte[] {0, 0, 1}))
        .thenReturn("sys.cpu.0");    
    when(tsdb.getUidName(UniqueIdType.METRIC, new byte[] {0, 0, 2}))
      .thenThrow(new NoSuchUniqueName("metric", "sys.cpu.1"));
    
    when(tsdb.getClient()).thenReturn(client);
    when(tsdb.uidTable()).thenReturn("tsdb-uid".getBytes());
    when(tsdb.hbaseAcquireLock((byte[])any(), (byte[])any(), anyShort()))
      .thenReturn(mock(RowLock.class));
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
    assertEquals("{\"metric\":{\"sys.cpu.0\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsMetricDouble() throws Exception {
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
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagk=host");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagk\":{\"host\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsTagkDouble() throws Exception {
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
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=localhost");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagv\":{\"localhost\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void assignQsTagvDouble() throws Exception {
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
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign?tagv=");
    this.rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void assignQsEmpty() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/assign");
    this.rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void assignQsTypo() throws Exception {
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
    assertEquals("{\"metric\":{\"sys.cpu.0\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostMetricDouble() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"metric\":[\"sys.cpu.0\",\"sys.cpu.2\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(
        "{\"metric\":{\"sys.cpu.0\":\"000001\",\"sys.cpu.2\":\"000003\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostMetricSingleBad() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"metric\":[\"sys.cpu.2\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"metric_errors\":{\"sys.cpu.1\":\"Name already exists with " 
        + "UID: 000002\"},\"metric\":{}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostMetric2Good1Bad() throws Exception {
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
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"tagk\":[\"host\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagk\":{\"host\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagkDouble() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagk\":[\"host\",\"fqdn\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(
        "{\"tagk\":{\"fqdn\":\"000003\",\"host\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagkSingleBad() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagk\":[\"datacenter\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagk_errors\":{\"datacenter\":\"Name already exists with " 
        + "UID: 000002\"},\"tagk\":{}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagk2Good1Bad() throws Exception {
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
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"tagv\":[\"localhost\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagv\":{\"localhost\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagvDouble() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagv\":[\"localhost\",\"foo\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(
        "{\"tagv\":{\"foo\":\"000003\",\"localhost\":\"000001\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagvSingleBad() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
    "{\"tagv\":[\"myserver\"]}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"tagv\":{},\"tagv_errors\":{\"myserver\":\"Name already "
        + "exists with UID: 000002\"}}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  public void assignPostTagv2Good1Bad() throws Exception {
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
    // missing a quotation mark
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", 
        "{\"tagv\":[\"localhost\",myserver\",\"foo\"],"
        + "\"metric\":[\"sys.cpu.0\",\"sys.cpu.1\",\"sys.cpu.2\"],"
        + "\"tagk\":[\"host\",\"datacenter\",\"fqdn\"]}");
    this.rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void assignPostNotJSON() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", "Hello");
    this.rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void assignPostNoContent() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", "");
    this.rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void assignPostEmptyJSON() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/assign", "{}");
    this.rpc.execute(tsdb, query);
  }

  @Test
  public void stringToUniqueIdTypeMetric() throws Exception {
    assertEquals(UniqueIdType.METRIC, UniqueId.stringToUniqueIdType("Metric"));
  }
  
  @Test
  public void stringToUniqueIdTypeTagk() throws Exception {
    assertEquals(UniqueIdType.TAGK, UniqueId.stringToUniqueIdType("TagK"));
  }
  
  @Test
  public void stringToUniqueIdTypeTagv() throws Exception {
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
  public void stringToUniqueIdTypeInvalid() throws Exception {
    UniqueId.stringToUniqueIdType("Not a type");
  }

  // Teset /api/uid/uidmeta --------------------
  
  @Test
  public void uidGet() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?type=metric&uid=000001");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test (expected = BadRequestException.class)
  public void uidGetNoUID() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?type=metric");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void uidGetNoType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?uid=000001");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void uidGetNSU() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?type=metric&uid=000002");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void uidPost() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void uidPostNotModified() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"type\":\"metric\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test (expected = BadRequestException.class)
  public void uidPostMissingUID() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/uidmeta", 
        "{\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void uidPostMissingType() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void uidPostNSU() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000002\",\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void uidPostQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?uid=000001&type=metric&display_name=Hello&method=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void uidPut() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void uidPutNotModified() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"type\":\"metric\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test (expected = BadRequestException.class)
  public void uidPutMissingUID() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/uidmeta", 
        "{\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void uidPutMissingType() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }

  @Test (expected = BadRequestException.class)
  public void uidPutNSU() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000002\",\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void uidPutQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?uid=000001&type=metric&display_name=Hello&method=put");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test
  public void uidDelete() throws Exception {
    HttpQuery query = NettyMocks.deleteQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }

  @Test (expected = BadRequestException.class)
  public void uidDeleteMissingUID() throws Exception {
    HttpQuery query = NettyMocks.deleteQuery(tsdb, "/api/uid/uidmeta", 
        "{\"type\":\"metric\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void uidDeleteMissingType() throws Exception {
    HttpQuery query = NettyMocks.deleteQuery(tsdb, "/api/uid/uidmeta", 
        "{\"uid\":\"000001\",\"displayName\":\"Hello!\"}");
    rpc.execute(tsdb, query);
  }

  @Test
  public void uidDeleteQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/uidmeta?uid=000001&type=metric&method=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  // Test /api/uid/tsmeta ----------------------
  
  @Test
  public void tsuidGet() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/tsmeta?tsuid=000001000001000001");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidGetNotFound() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/tsmeta?tsuid=000001000001000002");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidGetMissingTSUID() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/uid/tsmeta");
    rpc.execute(tsdb, query);
  }

  @Test
  public void tsuidPost() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/tsmeta", 
        "{\"tsuid\":\"000001000001000001\", \"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(Charset.forName("UTF-8"))
        .contains("\"displayName\":\"Hello World\""));    
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidPostNoTSUID() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/tsmeta", 
        "{\"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void tsuidPostNotModified() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/uid/tsmeta", 
        "{\"tsuid\":\"000001000001000001\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test
  public void tsuidPostQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/uid/tsmeta?tsuid=000001000001000001&display_name=42&method=post");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(Charset.forName("UTF-8"))
        .contains("\"displayName\":\"42\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidPostQSNoTSUID() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/uid/tsmeta?display_name=42&method=post");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void tsuidPut() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/tsmeta", 
        "{\"tsuid\":\"000001000001000001\", \"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(Charset.forName("UTF-8"))
        .contains("\"displayName\":\"Hello World\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidPutNoTSUID() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/tsmeta", 
        "{\"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void tsuidPutNotModified() throws Exception {
    HttpQuery query = NettyMocks.putQuery(tsdb, "/api/uid/tsmeta", 
        "{\"tsuid\":\"000001000001000001\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NOT_MODIFIED, query.response().getStatus());
  }
  
  @Test
  public void tsuidPutQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/uid/tsmeta?tsuid=000001000001000001&display_name=42&method=put");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(Charset.forName("UTF-8"))
        .contains("\"displayName\":\"42\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void tsuidPutQSNoTSUID() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/uid/tsmeta?display_name=42&method=put");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void tsuidDelete() throws Exception {
    HttpQuery query = NettyMocks.deleteQuery(tsdb, "/api/uid/tsmeta", 
        "{\"tsuid\":\"000001000001000001\", \"displayName\":\"Hello World\"}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void tsuidDeleteQS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
    "/api/uid/tsmeta?tsuid=000001000001000001&method=delete");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
}
