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

import net.opentsdb.utils.Pair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import java.util.Collections;
import java.util.List;

/**
 * Class used for passing and executing simple queries against with the search plugin. This may not
 * be able to take advantage of all of the search engine's features but is intended to satisfy most
 * common search requests. With 2.1 it now allows for time series lookup queries, using the meta or
 * full data tables to determine what time series exist for a given metric, tag name, tag value or
 * combination thereof.
 *
 * @since 2.0
 */
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchQuery {

  /** The type of search to execute. */
  private SearchType type;
  /** The actual query to execute. */
  private String query;
  /** The metric to iterate over, may be null. */
  private String metric;
  /** Optional tags to match on, may be null. */
  private List<Pair<String, String>> tags;
  /** Limit the number of responses so we don't overload the TSD or client. */
  private int limit;
  /** Used for paging through a result set. */
  private int startIndex;
  /** Total results from the user. */
  private int totalResults;
  /**
   * Ammount of time it took to complete the query (including parsing the response within the TSD.
   */
  private float time;
  /** Results from the search engine. Object depends on the query type. */
  private List<Object> results;

  /**
   * Default ctor. Fields are left null.
   */
  public SearchQuery() {
    metric = "*";
    limit = 25;
  }

  /**
   * Overload to set the metric on creation.
   *
   * @param metric The metric to filter on
   */
  public SearchQuery(final String metric) {
    this.metric = metric;
    limit = 25;
  }

  /**
   * Overload to set just the tags.
   *
   * @param tags List of tagk/tagv pairs, either of which may be null
   */
  public SearchQuery(final List<Pair<String, String>> tags) {
    this.tags = tags;
    metric = "*";
    limit = 25;
  }

  /**
   * Overload to set both metric and tags.
   *
   * @param metric The metric to filter on
   * @param tags List of tagk/tagv pairs, either of which may be null
   */
  public SearchQuery(final String metric,
                     final List<Pair<String, String>> tags) {
    this.metric = metric;
    this.tags = tags;
    limit = 25;
  }

  /**
   * Converts the human readable string to the proper enum.
   *
   * @param type The string to parse
   * @return The parsed enum
   * @throws IllegalArgumentException if the type is missing or wsa not recognized
   */
  public static SearchType parseSearchType(final String type) {
    if (type == null || type.isEmpty()) {
      throw new IllegalArgumentException("Type provided was null or empty");
    }

    if ("tsmeta".equals(type.toLowerCase())) {
      return SearchType.TSMETA;
    } else if ("tsmeta_summary".equals(type.toLowerCase())) {
      return SearchType.TSMETA_SUMMARY;
    } else if ("tsuids".equals(type.toLowerCase())) {
      return SearchType.TSUIDS;
    } else if ("uidmeta".equals(type.toLowerCase())) {
      return SearchType.UIDMETA;
    } else if ("annotation".equals(type.toLowerCase())) {
      return SearchType.ANNOTATION;
    } else if ("lookup".equals(type.toLowerCase())) {
      return SearchType.LOOKUP;
    } else {
      throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("type=").append(type).append(", query=")
        .append(query).append(", metric=").append(metric)
        .append(", tags=[");
    if (tags != null) {
      for (int i = 0; i < tags.size(); i++) {
        if (i > 0) {
          buf.append(", ");
        }
        buf.append("{").append(tags.get(i).getKey()).append("=")
            .append(tags.get(i).getValue()).append("}");
      }
    }
    buf.append("], limit=").append(limit).append(", startIndex=")
        .append(startIndex);
    return buf.toString();
  }

  /** @return The type of query executed */
  public SearchType getType() {
    return type;
  }

  // GETTERS AND SETTERS --------------------------

  /** @param type The type of query to execute */
  public void setType(SearchType type) {
    this.type = type;
  }

  /** @return The query itself */
  public String getQuery() {
    return query;
  }

  /** @param query The query to execute */
  public void setQuery(String query) {
    this.query = query;
  }

  /** @return Name of a metric to use for filtering */
  public String getMetric() {
    return metric;
  }

  /** @param metric A metric to use for lookup filtering */
  public void setMetric(String metric) {
    this.metric = metric;
  }

  /** @return List of tagk/tagv pairs, either of which may be null */
  public List<Pair<String, String>> getTags() {
    return tags;
  }

  /** @param tags A list of tagk, tagv pairs, either of which may be null */
  public void setTags(List<Pair<String, String>> tags) {
    this.tags = tags;
  }

  /** @return A limit on the number of results returned per query */
  public int getLimit() {
    return limit;
  }

  /** @param limit A limit to the number of results to return */
  public void setLimit(int limit) {
    this.limit = limit;
  }

  /** @return The starting index for paging through results */
  public int getStartIndex() {
    return startIndex;
  }

  /** @param startIndex Used for paging through a result set, starts at 0 */
  public void setStartIndex(int startIndex) {
    this.startIndex = startIndex;
  }

  /** @return The total results matched on the query */
  public int getTotalResults() {
    return totalResults;
  }

  /** @param totalResults The total number of results matched on the query */
  public void setTotalResults(int totalResults) {
    this.totalResults = totalResults;
  }

  /** @return The amount of time it took to complete the query */
  public float getTime() {
    return time;
  }

  /** @param time The amount of time it took to complete the query */
  public void setTime(float time) {
    this.time = time;
  }

  /** @return The array of results. May be an empty list */
  public List<Object> getResults() {
    if (results == null) {
      return Collections.emptyList();
    }
    return results;
  }

  /** @param results The result set */
  public void setResults(List<Object> results) {
    this.results = results;
  }

  /**
   * Types of searches to execute, chooses the different indexes and/or alters the output format
   */
  public enum SearchType {
    TSMETA,
    TSMETA_SUMMARY,
    TSUIDS,
    UIDMETA,
    ANNOTATION,
    LOOKUP
  }

}
