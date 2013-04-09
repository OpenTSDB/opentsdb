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

import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Config;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, HttpQuery.class})
public final class TestPutRpc {
  private TSDB tsdb = null;
  
  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();
    final HashMap<String, String> tags1 = new HashMap<String, String>();
    tags1.put("host", "web01");
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, 42, tags1))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, -42, tags1))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, 42.2f, tags1))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, -42.2f, tags1))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, 4220.0f, tags1))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, -4220.0f, tags1))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, .0042f, tags1))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.nice", 1365465600, -0.0042f, tags1))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("sys.cpu.system", 1365465600, 24, tags1))
      .thenReturn(Deferred.fromResult(new Object()));
    when(tsdb.addPoint("doesnotexist", 1365465600, 42, tags1))
      .thenThrow(new NoSuchUniqueName("metric", "doesnotexist"));
  }
  
  @Test
  public void constructor() {
    assertNotNull(new PutDataPointRpc());
  }
  
  // HTTP RPC Tests --------------------------------------
  
  @Test
  public void putSingle() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putDouble() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putSingleSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?summary", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"failed\":0,\"success\":1}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void putSingleDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"errors\":[],\"failed\":0,\"success\":1}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void putSingleSummaryAndDetails() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?summary&details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"errors\":[],\"failed\":0,\"success\":1}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void putDoubleSummary() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?summary", 
        "[{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        + ":42,\"tags\":{\"host\":\"web01\"}},{\"metric\":\"sys.cpu.system\","
        + "\"timestamp\":1365465600,\"value\":24,\"tags\":"
        + "{\"host\":\"web01\"}}]");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals("{\"failed\":0,\"success\":2}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void putNegativeInt() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putFloat() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42.2,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putNegativeFloat() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-42.2,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putSEBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":4.22e3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putSECaseBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":4.22E3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putNegativeSEBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-4.22e3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putNegativeSECaseBig() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-4.22E3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putSETiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":4.2e-3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putSECaseTiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":4.2E-3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putNegativeSETiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-4.2e-3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test
  public void putNegativeSECaseTiny() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-4.2E-3,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.NO_CONTENT, query.response().getStatus());
  }
  
  @Test (expected = BadRequestException.class)
  public void badMethod() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, "/api/put");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
  }
  
  @Test (expected = IOException.class)
  public void badJSON() throws Exception {
    // missing a quotation mark
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp:1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
  }
  
  @Test (expected = IOException.class)
  public void notJSON() throws Exception {
    // missing a quotation mark
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", "Hello World");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void noContent() throws Exception {
    // missing a quotation mark
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put", "");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
  }

  @Test
  public void noSuchUniqueName() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"doesnotexist\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"doesnotexist\","
        + "\"timestamp\":1365465600,\"value\":\"42\",\"tags\":{\"host\":"
        + "\"web01\"}},\"error\":\"Unknown metric\"}],\"failed\":1,"
        + "\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void missingMetric() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":null,\"timestamp\""
        + ":1365465600,\"value\":\"42\",\"tags\":{\"host\":\"web01\"}},"
        + "\"error\":\"Metric name was empty\"}],\"failed\":1,\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
 
  @Test
  public void nullMetric() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":null,\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":null,\"timestamp\""
        + ":1365465600,\"value\":\"42\",\"tags\":{\"host\":\"web01\"}},"
        + "\"error\":\"Metric name was empty\"}],\"failed\":1,\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void missingTimestamp() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":0,\"value\":\"42\",\"tags\":{\"host\":\"web01\"}},"
        + "\"error\":\"Invalid timestamp\"}],\"failed\":1,\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void nullTimestamp() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":null,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":0,\"value\":\"42\",\"tags\":{\"host\":\"web01\"}},"
        + "\"error\":\"Invalid timestamp\"}],\"failed\":1,\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void invalidTimestamp() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":-1,\"value\""
        +":42,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":-1,\"value\":\"42\",\"tags\":{\"host\":\"web01\"}},"
        + "\"error\":\"Invalid timestamp\"}],\"failed\":1,\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void missingValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"tags\":"
        + "{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":null,\"tags\":" 
        + "{\"host\":\"web01\"}},\"error\":\"Empty value\"}],\"failed\":1,"
        + "\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void nullValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":null,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":null,\"tags\":" 
        + "{\"host\":\"web01\"}},\"error\":\"Empty value\"}],\"failed\":1,"
        + "\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void emptyValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":\"\",\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":\"\",\"tags\":" 
        + "{\"host\":\"web01\"}},\"error\":\"Empty value\"}],\"failed\":1,"
        + "\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void badValue() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":\"notanumber\",\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":\"notanumber\",\"tags\":" 
        + "{\"host\":\"web01\"}},\"error\":\"Unable to parse value to a number"
        + "\"}],\"failed\":1,\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void ValueNaN() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":NaN,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":\"NaN\",\"tags\":" 
        + "{\"host\":\"web01\"}},\"error\":\"Unable to parse value to a number"
        + "\"}],\"failed\":1,\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test (expected = IOException.class)
  public void ValueNaNCase() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":Nan,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
  }
  
  @Test
  public void ValueINF() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":+INF,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":\"+INF\",\"tags\":" 
        + "{\"host\":\"web01\"}},\"error\":\"Unable to parse value to a number"
        + "\"}],\"failed\":1,\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void ValueNINF() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-INF,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":\"-INF\",\"tags\":" 
        + "{\"host\":\"web01\"}},\"error\":\"Unable to parse value to a number"
        + "\"}],\"failed\":1,\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test (expected = IOException.class)
  public void ValueINFUnsigned() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":INF,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
  }
  
  @Test (expected = IOException.class)
  public void ValueINFCase() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":+inf,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
  }
  
  @Test
  public void ValueInfiniy() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":+Infinity,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":\"+Infinity\",\"tags\":" 
        + "{\"host\":\"web01\"}},\"error\":\"Unable to parse value to a number"
        + "\"}],\"failed\":1,\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void ValueNInfiniy() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":-Infinity,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":\"-Infinity\",\"tags\":" 
        + "{\"host\":\"web01\"}},\"error\":\"Unable to parse value to a number"
        + "\"}],\"failed\":1,\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test (expected = IOException.class)
  public void ValueInfinityUnsigned() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":Infinity,\"tags\":{\"host\":\"web01\"}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
  }
  
  @Test
  public void missingTags() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\":42"
        + "}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":\"42\",\"tags\":" 
        + "null},\"error\":\"Missing tags\"}],\"failed\":1,"
        + "\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }

  @Test
  public void nullTags() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":null}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":\"42\",\"tags\":" 
        + "null},\"error\":\"Missing tags\"}],\"failed\":1,"
        + "\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
  
  @Test
  public void emptyTags() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/put?details", 
        "{\"metric\":\"sys.cpu.nice\",\"timestamp\":1365465600,\"value\""
        +":42,\"tags\":{}}");
    PutDataPointRpc put = new PutDataPointRpc();
    put.execute(tsdb, query);
    assertEquals(HttpResponseStatus.BAD_REQUEST, query.response().getStatus());
    assertEquals("{\"errors\":[{\"datapoint\":{\"metric\":\"sys.cpu.nice\","
        + "\"timestamp\":1365465600,\"value\":\"42\",\"tags\":" 
        + "{}},\"error\":\"Missing tags\"}],\"failed\":1,"
        + "\"success\":0}", 
        query.response().getContent().toString(Charset.forName("UTF-8")));
  }
}
