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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

/**
 * Handles the suggest endpoint that returns X number of metrics, tagks or
 * tagvs that start with the given string. It's used for auto-complete entries
 * and does not support wildcards.
 */
final class SuggestRpc implements HttpRpc {

  /**
   * Handles an HTTP based suggest query
   * <b>Note:</b> This method must remain backwards compatible with the 1.x 
   * API call
   * @throws IOException if there is an error parsing the query or formatting 
   * the output
   * @throws BadRequestException if the user supplied bad data
   */
  public void execute(final TSDB tsdb, final HttpQuery query) 
    throws IOException {
    
    // only accept GET/POST
    if (query.method() != HttpMethod.GET && query.method() != HttpMethod.POST) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
    
    final String type;
    final String q;
    final String max;
    if (query.apiVersion() > 0 && query.method() == HttpMethod.POST) {
      final HashMap<String, String> map = query.serializer().parseSuggestV1();
      type = map.get("type");
      if (type == null || type.isEmpty()) {
        throw new BadRequestException("Missing 'type' parameter");
      }
      q = map.get("q");
      if (q == null) {
        throw new BadRequestException("Missing 'q' parameter");
      }
      max = map.get("max");
    } else { 
      type = query.getRequiredQueryStringParam("type");
      q = query.getRequiredQueryStringParam("q");
      max = query.getQueryStringParam("max");
    }
    
    final int max_results;
    if (max != null && !max.isEmpty()) {
      try {
        max_results = Integer.parseInt(max);
      } catch (NumberFormatException nfe) {
        throw new BadRequestException("Unable to parse 'max' as a number");
      }
    } else {
      max_results = 0;
    }
    
    List<String> suggestions;
    if ("metrics".equals(type)) {
      suggestions = max_results > 0 ? tsdb.suggestMetrics(q, max_results) :
         tsdb.suggestMetrics(q);
    } else if ("tagk".equals(type)) {
      suggestions = max_results > 0 ? tsdb.suggestTagNames(q, max_results) :
         tsdb.suggestTagNames(q);
    } else if ("tagv".equals(type)) {
      suggestions = max_results > 0 ? tsdb.suggestTagValues(q, max_results) :
        tsdb.suggestTagValues(q);
    } else {
      throw new BadRequestException("Invalid 'type' parameter:" + type);
    }
    
    if (query.apiVersion() > 0) {
      query.sendReply(query.serializer().formatSuggestV1(suggestions));
    } else { // deprecated API
      query.sendReply(JSON.serializeToBytes(suggestions));
    }
  }  
}
