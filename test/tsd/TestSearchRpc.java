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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyChar;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.search.TimeSeriesLookup;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, HttpQuery.class, UniqueId.class, 
  RowKey.class, Tags.class, TimeSeriesLookup.class, SearchRpc.class})
public final class TestSearchRpc {
  private TSDB tsdb = null;
  private SearchRpc rpc = new SearchRpc();
  private SearchQuery search_query = null;
  private TimeSeriesLookup mock_lookup = null;
  private static final Charset UTF = Charset.forName("UTF-8");
  private static List<byte[]> test_tsuids = new ArrayList<byte[]>(3);
  static {
    test_tsuids.add(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    test_tsuids.add(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 2 });
    test_tsuids.add(new byte[] { 0, 0, 2, 0, 0, 1, 0, 0, 1 });
  }
  
  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();
  }
  
  @Test
  public void constructor() {
    assertNotNull(new SearchRpc());
  }
  
  @Test
  public void searchTSMeta() throws Exception {
    setupAnswerSearchQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/search/tsmeta?query=*");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String result = query.response().getContent().toString(UTF);
    assertTrue(result.contains("\"results\":[{\"tsuid\""));
    assertEquals(1, search_query.getResults().size());
  }
  
  @Test
  public void searchTSMeta_Summary() throws Exception {
    setupAnswerSearchQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/search/tsmeta_summary?query=*");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String result = query.response().getContent().toString(UTF);
    assertTrue(result.contains("\"results\":[{\"tags\""));
    assertEquals(1, search_query.getResults().size());
  }
  
  @Test
  public void searchTSUIDs() throws Exception {
    setupAnswerSearchQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/search/tsuids?query=*");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String result = query.response().getContent().toString(UTF);
    assertTrue(result.contains("\"results\":[\"000001000001000001\""));
    assertEquals(2, search_query.getResults().size());
  }
  
  @Test
  public void searchUIDMeta() throws Exception {
    setupAnswerSearchQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/search/uidmeta?query=*");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String result = query.response().getContent().toString(UTF);
    assertTrue(result.contains("\"results\":[{\"uid\""));
    assertEquals(2, search_query.getResults().size());
  }
  
  @Test
  public void searchAnnotation() throws Exception {
    setupAnswerSearchQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/search/annotation?query=*");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String result = query.response().getContent().toString(UTF);
    assertTrue(result.contains("\"results\":[{\"tsuid\""));
    assertEquals(1, search_query.getResults().size());
  }
  
  @Test
  public void searchEmptyResultSet() throws Exception {
    setupAnswerSearchQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/search/annotation?query=EMTPY");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String result = query.response().getContent().toString(UTF);
    assertTrue(result.contains("\"results\":[]"));
    assertEquals(0, search_query.getResults().size());
  }
  
  @Test
  public void searchQSParseLimit() throws Exception {
    setupAnswerSearchQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/search/tsmeta?query=*&limit=42");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(42, search_query.getLimit());
  }
  
  @Test
  public void searchQSParseStartIndex() throws Exception {
    setupAnswerSearchQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/search/tsmeta?query=*&start_index=4");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(4, search_query.getStartIndex());
  }
  
  @Test
  public void searchPOST() throws Exception {
    setupAnswerSearchQuery();
    final HttpQuery query = NettyMocks.postQuery(tsdb, 
      "/api/search/tsmeta", "{\"query\":\"*\",\"limit\":42,\"startIndex\":2}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String result = query.response().getContent().toString(UTF);
    assertTrue(result.contains("\"results\":[{\"tsuid\""));
    assertEquals(1, search_query.getResults().size());
    assertEquals(42, search_query.getLimit());
    assertEquals(2, search_query.getStartIndex());
  }
  
  @Test (expected = BadRequestException.class)
  public void searchBadMethod() throws Exception {
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.PUT, "/api/search");
    final HttpQuery query = new HttpQuery(tsdb, req, NettyMocks.fakeChannel());
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void searchMissingType() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/api/search?query=*");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void searchBadTypeType() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/search/badtype?query=*");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void searchMissingQuery() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, "/api/search/tsmeta");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void searchPluginNotEnabled() throws Exception {
    when(tsdb.executeSearch((SearchQuery)any()))
        .thenThrow(new IllegalStateException(
            "Searching has not been enabled on this TSD"));
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/search/tsmeta?query=*");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void searchInvalidLimit() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/search/tsmeta?query=*&limit=nan");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void searchInvalidStartIndex() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/search/tsmeta?query=*&start_index=nan");
    rpc.execute(tsdb, query);
  }
  
  @Test
  public void searchLookup() throws Exception {
    setupAnswerLookupQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/search/lookup?m={host=}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String result = query.response().getContent().toString(UTF);
    assertTrue(result.contains("\"host\":\"web01\""));
    assertTrue(result.contains("\"totalResults\":3"));
  }
  
  @Test
  public void searchLookupPOST() throws Exception {
    setupAnswerLookupQuery();    
    SearchQuery q = new SearchQuery();
    q.setTags(new ArrayList<Pair<String, String>>(2));
    q.getTags().add(new Pair<String, String>("host", "web01"));
    q.getTags().add(new Pair<String, String>("dc", "phx"));

    final HttpQuery query = NettyMocks.postQuery(tsdb, 
      "/api/search/lookup", "{\"tags\":[{\"key\":\"host\",\"value\":\"web01\"}]}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    final String result = query.response().getContent().toString(UTF);
    assertTrue(result.contains("\"host\":\"web01\""));
    assertTrue(result.contains("\"totalResults\":3"));
  }
  
  @Test (expected = BadRequestException.class)
  public void searchLookupMissingQuery() throws Exception {
    setupAnswerLookupQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/search/lookup");
    rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void searchLookupBadQuery() throws Exception {
    setupAnswerLookupQuery();
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/search/lookup?m={");
    rpc.execute(tsdb, query);
  }
  
  /**
   * Configures an Answer to respond with when the tests call 
   * tsdb.executeSearch(), responding to the type of query requested with valid
   * responses for parsing tests.
   */
  private void setupAnswerSearchQuery() {
    when(tsdb.executeSearch((SearchQuery)any())).thenAnswer(
      new Answer<Deferred<SearchQuery>>() {

        @Override
        public Deferred<SearchQuery> answer(InvocationOnMock invocation)
            throws Throwable {
          final Object[] args = invocation.getArguments();
          search_query = (SearchQuery)args[0];
          
          List<Object> results = new ArrayList<Object>(1);

          // if we want an empty response, return an empty response
          if (search_query.getQuery().toUpperCase().equals("EMTPY")) {
            search_query.setResults(results);
            search_query.setTotalResults(0);
            
            return Deferred.fromResult(search_query);
          }
          
          switch(search_query.getType()) {
            case TSMETA:
              final TSMeta meta = new TSMeta("000001000001000001");
              meta.setCreated(1356998400);
              meta.setDescription("System CPU metric");
              
              UIDMeta uid = new UIDMeta(UniqueIdType.METRIC, "000001");
              final Field uid_name = UIDMeta.class.getDeclaredField("name");
              uid_name.setAccessible(true);
              uid_name.set(uid, "sys.cpu.0");
              
              final Field metric = TSMeta.class.getDeclaredField("metric");
              metric.setAccessible(true);
              metric.set(meta, uid);
              
              final ArrayList<UIDMeta> tags = new ArrayList<UIDMeta>(2);
              uid = new UIDMeta(UniqueIdType.TAGK, "000001");
              uid_name.set(uid, "host");
              tags.add(uid);
              uid = new UIDMeta(UniqueIdType.TAGV, "000001");
              uid_name.set(uid, "web01");
              tags.add(uid);
              
              final Field tags_field = TSMeta.class.getDeclaredField("tags");
              tags_field.setAccessible(true);
              tags_field.set(meta, tags);
              results.add(meta);
              break;
            
            case LOOKUP:
            case TSMETA_SUMMARY:
              final HashMap<String, Object> ts = new HashMap<String, Object>(1);
              ts.put("metric", "sys.cpu.0");
              final HashMap<String, String> tag_map = 
                new HashMap<String, String>(2);
              tag_map.put("host", "web01");
              tag_map.put("owner", "ops");
              ts.put("tags", tag_map);
              ts.put("tsuid", "000001000001000001");
              results.add(ts);
              break;
              
            case TSUIDS:
              results.add("000001000001000001");
              results.add("000002000002000002");
              break;
              
            case UIDMETA:
              UIDMeta uid2 = new UIDMeta(UniqueIdType.METRIC, "000001");
              final Field name_field = UIDMeta.class.getDeclaredField("name");
              name_field.setAccessible(true);
              name_field.set(uid2, "sys.cpu.0");
              results.add(uid2);
              
              uid2 = new UIDMeta(UniqueIdType.TAGK, "000001");
              name_field.set(uid2, "host");
              results.add(uid2);
              break;
              
            case ANNOTATION:
              final Annotation note = new Annotation();
              note.setStartTime(1356998400);
              note.setEndTime(1356998460);
              note.setDescription("Something went pear shaped");
              note.setTSUID("000001000001000001");
              results.add(note);
              break;
              
          }
          
          search_query.setResults(results);
          search_query.setTotalResults(results.size());
          search_query.setTime(0.42F);
          
          return Deferred.fromResult(search_query);
        }

    });
  }
  
  @SuppressWarnings("unchecked")
  private void setupAnswerLookupQuery() throws Exception {
    PowerMockito.mockStatic(RowKey.class);
    when(RowKey.metricNameAsync(tsdb, test_tsuids.get(0)))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));
    when(RowKey.metricNameAsync(tsdb, test_tsuids.get(1)))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));
    when(RowKey.metricNameAsync(tsdb, test_tsuids.get(2)))
      .thenReturn(Deferred.fromResult("sys.cpu.nice"));

    PowerMockito.mockStatic(UniqueId.class);
    final List<byte[]> pair_a = new ArrayList<byte[]>(2);
    pair_a.add(new byte[] { 0, 0, 1 });
    pair_a.add(new byte[] { 0, 0, 1 });
    
    final List<byte[]> pair_b = new ArrayList<byte[]>(2);
    pair_b.add(new byte[] { 0, 0, 1 });
    pair_b.add(new byte[] { 0, 0, 2 });
    
    when(UniqueId.getTagPairsFromTSUID(test_tsuids.get(0)))
      .thenReturn(pair_a);
    when(UniqueId.getTagPairsFromTSUID(test_tsuids.get(1)))
      .thenReturn(pair_b);
    when(UniqueId.getTagPairsFromTSUID(test_tsuids.get(2)))
      .thenReturn(pair_a);
    when(UniqueId.uidToString((byte[])any())).thenCallRealMethod();
    
    PowerMockito.mockStatic(Tags.class);
    final HashMap<String, String> tags_a = new HashMap<String, String>(1);
    tags_a.put("host", "web01");
    
    final HashMap<String, String> tags_b = new HashMap<String, String>(1);
    tags_b.put("host", "web02");
    
    when(Tags.resolveIdsAsync(tsdb, pair_a))
      .thenReturn(Deferred.fromResult(tags_a));
    when(Tags.resolveIdsAsync(tsdb, pair_b))
      .thenReturn(Deferred.fromResult(tags_b));
    
    when(Tags.parseWithMetric(anyString(), anyList())).thenCallRealMethod();
    when(Tags.splitString(anyString(), anyChar())).thenCallRealMethod();
    PowerMockito.doCallRealMethod().when(Tags.class, "parse", 
        anyList(), anyString());

    mock_lookup = mock(TimeSeriesLookup.class);
    PowerMockito.whenNew(TimeSeriesLookup.class)
      .withArguments((TSDB)any(), (SearchQuery)any())
      .thenReturn(mock_lookup);
    
    when(mock_lookup.lookup()).thenReturn(test_tsuids);
  }
}
