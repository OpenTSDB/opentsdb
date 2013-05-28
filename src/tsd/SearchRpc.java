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

import org.jboss.netty.handler.codec.http.HttpMethod;

import net.opentsdb.core.TSDB;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.search.SearchQuery.SearchType;

/**
 * Handles very basic search calls by passing the user's query to the configured
 * search plugin and pushing the response back through the serializers.
 * @since 2.0
 */
final class SearchRpc implements HttpRpc {

  /** The query we're working with */
  private HttpQuery query;
  
  /**
   * Handles the /api/search/&lt;type&gt; endpoint
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to work with
   */
  @Override
  public void execute(TSDB tsdb, HttpQuery query) {
  
    this.query = query;
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
      search_query = parseQueryString();
    }
    
    search_query.setType(type);
    
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
   * @return A parsed SearchQuery object
   */
  private final SearchQuery parseQueryString() {
    final SearchQuery search_query = new SearchQuery();
    
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
}
