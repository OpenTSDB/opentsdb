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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.search.TimeSeriesLookup;
import net.opentsdb.search.SearchQuery.SearchType;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Exceptions;
import net.opentsdb.utils.Pair;

/**
 * Handles very basic search calls by passing the user's query to the configured
 * search plugin and pushing the response back through the serializers.
 * Also allows for time series lookups given a metric, tag name, tag value or
 * combination thereof using the tsdb-meta table.
 * @since 2.0
 */
final class SearchRpc implements HttpRpc {

  /**
   * Handles the /api/search/&lt;type&gt; endpoint
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to work with
   */
  @Override
  public void execute(TSDB tsdb, HttpQuery query) {
  
    final HttpMethod method = query.getAPIMethod();
    if (method != HttpMethod.GET && method != HttpMethod.POST) {
      throw new BadRequestException("Unsupported method: " + method.getName());
    }
    
    // the uri will be /api/vX/search/<type> or /api/search/<type>
    final String[] uri = query.explodeAPIPath();
    final String endpoint = uri.length > 1 ? uri[1] : "";
    final SearchType type;
    final SearchQuery search_query;
    
    try {
      type = SearchQuery.parseSearchType(endpoint);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Invalid search query type supplied", e);
    }
    
    if (query.hasContent()) {
      search_query = query.serializer().parseSearchQueryV1();
    } else {
      search_query = parseQueryString(query, type);
    }
    
    search_query.setType(type);
    
    if (type == SearchType.LOOKUP) {
      processLookup(tsdb, query, search_query);
      return;
    }
    
    try {
      final SearchQuery results = 
        tsdb.executeSearch(search_query).joinUninterruptibly();
      query.sendReply(query.serializer().formatSearchResultsV1(results));
    } catch (IllegalStateException e) {
      throw new BadRequestException("Searching is not enabled", e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parses required search values from the query string
   * @param query The HTTP query to work with
   * @param type The type of search query requested
   * @return A parsed SearchQuery object
   */
  private final SearchQuery parseQueryString(final HttpQuery query, 
      final SearchType type) {
    final SearchQuery search_query = new SearchQuery();
    
    if (type == SearchType.LOOKUP) {
      final String query_string = query.getRequiredQueryStringParam("m");
      search_query.setTags(new ArrayList<Pair<String, String>>());
      
      try {
      search_query.setMetric(Tags.parseWithMetric(query_string, 
          search_query.getTags()));
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Unable to parse query", e);
      }
      if (query.hasQueryStringParam("limit")) {
        final String limit = query.getQueryStringParam("limit");
        try {
          search_query.setLimit(Integer.parseInt(limit));
        } catch (NumberFormatException e) {
          throw new BadRequestException(
                  "Unable to convert 'limit' to a valid number");
        }
      }
      return search_query;
    }
    
    // process a regular search query
    search_query.setQuery(query.getRequiredQueryStringParam("query"));
    
    if (query.hasQueryStringParam("limit")) {
      final String limit = query.getQueryStringParam("limit");
      try {
        search_query.setLimit(Integer.parseInt(limit));
      } catch (NumberFormatException e) {
        throw new BadRequestException(
            "Unable to convert 'limit' to a valid number");
      }
    }
    
    if (query.hasQueryStringParam("start_index")) {
      final String idx = query.getQueryStringParam("start_index");
      try {
        search_query.setStartIndex(Integer.parseInt(idx));
      } catch (NumberFormatException e) {
        throw new BadRequestException(
            "Unable to convert 'start_index' to a valid number");
      }
    }
    
    return search_query;
  }

  /**
   * Processes a lookup query against the tsdb-meta table, returning (and 
   * resolving) the TSUIDs of any series that matched the query.
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to work with
   * @param search_query A search query configured with at least a metric
   * or a list of tag pairs. If neither are set, the method will throw an error.
   * @throws BadRequestException if the metric and tags are null or empty or
   * a UID fails to resolve.
   * @since 2.1
   */
  private void processLookup(final TSDB tsdb, final HttpQuery query, 
      final SearchQuery search_query) {
    if (search_query.getMetric() == null && 
        (search_query.getTags() == null || search_query.getTags().size() < 1)) {
      throw new BadRequestException(
          "Missing metric and tags. Please supply at least one value.");
    }
    final long start = System.currentTimeMillis();
    
    class MetricCB implements Callback<Object, String> {
      final Map<String, Object> series;
      MetricCB(final Map<String, Object> series) {
        this.series = series;
      }
      
      @Override
      public Object call(final String name) throws Exception {
        series.put("metric", name);
        return null;
      }
    }
    
    class TagsCB implements Callback<Object, HashMap<String, String>> {
      final Map<String, Object> series;
      TagsCB(final Map<String, Object> series) {
        this.series = series;
      }
      
      @Override
      public Object call(final HashMap<String, String> names) throws Exception {
        series.put("tags", names);
        return null;
      }
    }
    
    class Serialize implements Callback<Object, ArrayList<Object>> {
      final List<Object> results;
      Serialize(final List<Object> results) {
        this.results = results;
      }
      
      @Override
      public Object call(final ArrayList<Object> ignored) throws Exception {
        search_query.setResults(results);
        search_query.setTime(System.currentTimeMillis() - start);
        query.sendReply(query.serializer().formatSearchResultsV1(search_query));
        return null;
      }
    }
    
    class LookupCB implements Callback<Deferred<Object>, List<byte[]>> {
      @Override
      public Deferred<Object> call(final List<byte[]> tsuids) throws Exception {
        final List<Object> results = new ArrayList<Object>(tsuids.size());
        search_query.setTotalResults(tsuids.size());
        
        final ArrayList<Deferred<Object>> deferreds = 
            new ArrayList<Deferred<Object>>(tsuids.size());
        
        for (final byte[] tsuid : tsuids) {
          // has to be concurrent if the uid table is split across servers
          final Map<String, Object> series = 
              new ConcurrentHashMap<String, Object>(3);
          results.add(series);
          
          series.put("tsuid", UniqueId.uidToString(tsuid));
          byte[] metric_uid = Arrays.copyOfRange(tsuid, 0, TSDB.metrics_width());
          deferreds.add(tsdb.getUidName(UniqueIdType.METRIC, metric_uid)
              .addCallback(new MetricCB(series)));
          
          final List<byte[]> tag_ids = UniqueId.getTagPairsFromTSUID(tsuid);
          deferreds.add(Tags.resolveIdsAsync(tsdb, tag_ids)
              .addCallback(new TagsCB(series)));
        }
        
        return Deferred.group(deferreds).addCallback(new Serialize(results));
      }
    }
    
    class ErrCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception e) throws Exception {
        if (e instanceof NoSuchUniqueId) {
          query.sendReply(HttpResponseStatus.NOT_FOUND,
              query.serializer().formatErrorV1(
                  new BadRequestException(HttpResponseStatus.NOT_FOUND, 
              "Unable to resolve one or more TSUIDs", (NoSuchUniqueId)e)));
        } else if (e instanceof NoSuchUniqueName) {
          query.sendReply(HttpResponseStatus.NOT_FOUND,
            query.serializer().formatErrorV1(
              new BadRequestException(HttpResponseStatus.NOT_FOUND, 
              "Unable to resolve one or more UIDs", (NoSuchUniqueName)e)));
        } else if (e instanceof DeferredGroupException) {
          final Throwable ex = Exceptions.getCause((DeferredGroupException)e);
          if (ex instanceof NoSuchUniqueId) {
            query.sendReply(HttpResponseStatus.NOT_FOUND,
                query.serializer().formatErrorV1(
                    new BadRequestException(HttpResponseStatus.NOT_FOUND, 
                "Unable to resolve one or more TSUIDs", (NoSuchUniqueId)ex)));
          } else if (ex instanceof NoSuchUniqueName) {
            query.sendReply(HttpResponseStatus.NOT_FOUND,
              query.serializer().formatErrorV1(
                new BadRequestException(HttpResponseStatus.NOT_FOUND, 
                "Unable to resolve one or more UIDs", (NoSuchUniqueName)ex)));
          } else {
            query.sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
                query.serializer().formatErrorV1(
                    new BadRequestException(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
                "Unexpected exception", ex)));
          }
        } else {
          query.sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
              query.serializer().formatErrorV1(
                  new BadRequestException(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
              "Unexpected exception", e)));
        }
        return null;
      }
    }
    
    new TimeSeriesLookup(tsdb, search_query).lookupAsync()
      .addCallback(new LookupCB())
      .addErrback(new ErrCB());
  }
}
