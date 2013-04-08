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
import java.util.Map;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.fasterxml.jackson.core.type.TypeReference;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

/**
 * Implementation of the base serializer class with JSON as the format
 * <p>
 * <b>Note:</b> This class is not final and the implementations are not either
 * so that we can extend this default class with slightly different methods
 * when needed and retain everything else.
 * @since 2.0
 */
class HttpJsonSerializer extends HttpSerializer {

  /**
   * Default constructor necessary for plugin implementation
   */
  public HttpJsonSerializer() {
    super();
  }
  
  /**
   * Constructor that sets the query object
   * @param query Request/resposne object
   */
  public HttpJsonSerializer(final HttpQuery query) {
    super(query);
  }
  
  /** Initializer, nothing to do for the JSON serializer */
  @Override
  public void initialize(final TSDB tsdb) {
    // nothing to see here
  }
  
  /** Nothing to do on shutdown */
  public Deferred<Object> shutdown() {
    return new Deferred<Object>();
  }
  
  /** @return the version */
  @Override
  public String version() {
    return "2.0.0";
  }

  /** @return the shortname */
  @Override
  public String shortName() {
    return "json";
  }
  
  /**
   * Parses a suggestion query
   * @return a hash map of key/value pairs
   * @throws IOException if the parsing failed
   */
  @Override
  public HashMap<String, String> parseSuggestV1() throws IOException {
    final String json = query.getContent();
    if (json == null || json.isEmpty()) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
          "Missing message content",
          "Supply valid JSON formatted data in the body of your request");
    }
    return JSON.parseToObject(query.getContent(), 
        new TypeReference<HashMap<String, String>>(){});
  }
  
  /**
   * Formats a suggestion response
   * @param suggestions List of suggestions for the given type
   * @return A JSON formatted byte array
   * @throws IOException if the serialization failed
   */
  @Override
  public ChannelBuffer formatSuggestV1(final List<String> suggestions) 
    throws IOException {
    return this.serializeJSON(suggestions);
  }
  
  /**
   * Format the serializer status map
   * @return A JSON structure
   * @throws IOException if the serialization failed
   */
  public ChannelBuffer formatSerializersV1() throws IOException {
    return serializeJSON(HttpQuery.getSerializerStatus());
  }
  
  /**
   * Format the list of implemented aggregators
   * @param aggregators The list of aggregation functions
   * @return A JSON structure
   * @throws IOException if the serialization failed
   */
  public ChannelBuffer formatAggregatorsV1(final Set<String> aggregators) 
    throws IOException {
    return this.serializeJSON(aggregators);
  }
  
  /**
   * Format a hash map of information about the OpenTSDB version
   * @param version A hash map with version information
   * @return A JSON structure
   * @throws IOException if the serialization failed
   */
  public ChannelBuffer formatVersionV1(final Map<String, String> version) 
    throws IOException {
    return this.serializeJSON(version);
  }
  
  /**
   * Format a response from the DropCaches call
   * @param response A hash map with a response
   * @return A JSON structure
   * @throws IOException if the serialization failed
   */
  public ChannelBuffer formatDropCachesV1(final Map<String, String> response) 
    throws IOException {
    return this.serializeJSON(response);
  }
  
  /**
   * Helper object for the format calls to wrap the JSON response in a JSONP
   * function if requested. Used for code dedupe.
   * @param obj The object to serialize
   * @return A ChannelBuffer to pass on to the query
   * @throws IOException if serialization failed
   */
  private ChannelBuffer serializeJSON(final Object obj) throws IOException {
    if (query.hasQueryStringParam("jsonp")) {
      return ChannelBuffers.wrappedBuffer(
          JSON.serializeToJSONPBytes(query.getQueryStringParam("jsonp"), 
          obj));
    }
    return ChannelBuffers.wrappedBuffer(JSON.serializeToBytes(obj));
  }
}
