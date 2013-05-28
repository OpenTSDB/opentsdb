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
package net.opentsdb.search;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * Class used for passing and executing simple queries against with the search
 * plugin. This may not be able to take advantage of all of the search engine's
 * features but is intended to satisfy most common search requests.
 * @since 2.0
 */
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchQuery {

  /** 
   * Types of searches to execute, chooses the different indexes and/or alters
   * the output format
   */
  public enum SearchType {
    TSMETA,
    TSMETA_SUMMARY,
    TSUIDS,
    UIDMETA,
    ANNOTATION
  }
  
  /** The type of search to execute */
  private SearchType type;
  
  /** The actual query to execute */
  private String query;
  
  /** Limit the number of responses so we don't overload the TSD or client */
  private int limit = 25;
  
  /** Used for paging through a result set */
  private int start_index;
  
  /** Total results from the user */
  private int total_results;
  
  /** Ammount of time it took to complete the query (including parsing the
   *  response within the TSD
   */
  private float time;
  
  /** Results from the search engine. Object depends on the query type */
  private List<Object> results;

  /**
   * Converts the human readable string to the proper enum
   * @param type The string to parse
   * @return The parsed enum
   * @throws IllegalArgumentException if the type is missing or wsa not
   * recognized
   */
  public static SearchType parseSearchType(final String type) {
    if (type == null || type.isEmpty()) {
      throw new IllegalArgumentException("Type provided was null or empty");
    }
    
    if (type.toLowerCase().equals("tsmeta")) {
      return SearchType.TSMETA;
    } else if (type.toLowerCase().equals("tsmeta_summary")) {
      return SearchType.TSMETA_SUMMARY;
    } else if (type.toLowerCase().equals("tsuids")) {
      return SearchType.TSUIDS;
    } else if (type.toLowerCase().equals("uidmeta")) {
      return SearchType.UIDMETA;
    } else if (type.toLowerCase().equals("annotation")) {
      return SearchType.ANNOTATION;
    } else {
      throw new IllegalArgumentException("Unknown type: " + type);
    }
  }
  
  // GETTERS AND SETTERS --------------------------
  
  /** @return The type of query executed */
  public SearchType getType() {
    return type;
  }

  /** @return The query itself */
  public String getQuery() {
    return query;
  }

  /** @return A limit on the number of results returned per query */
  public int getLimit() {
    return limit;
  }

  /** @return The starting index for paging through results */
  public int getStartIndex() {
    return start_index;
  }

  /** @return The total results matched on the query */
  public int getTotalResults() {
    return total_results;
  }

  /** @return The amount of time it took to complete the query */
  public float getTime() {
    return time;
  }

  /** @return The array of results. May be an empty list */
  public List<Object> getResults() {
    if (results == null) {
      return Collections.emptyList();
    }
    return results;
  }

  /** @param type The type of query to execute */
  public void setType(SearchType type) {
    this.type = type;
  }

  /** @param query The query to execute */
  public void setQuery(String query) {
    this.query = query;
  }

  /** @param limit A limit to the number of results to return */
  public void setLimit(int limit) {
    this.limit = limit;
  }

  /** @param start_index Used for paging through a result set, starts at 0 */
  public void setStartIndex(int start_index) {
    this.start_index = start_index;
  }

  /** @param total_results The total number of results matched on the query */
  public void setTotalResults(int total_results) {
    this.total_results = total_results;
  }

  /** @param time The amount of time it took to complete the query */
  public void setTime(float time) {
    this.time = time;
  }

  /** @param results The result set*/
  public void setResults(List<Object> results) {
    this.results = results;
  }
  
}
