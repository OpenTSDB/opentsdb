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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
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
  private static final Logger LOG = 
    LoggerFactory.getLogger(HttpJsonSerializer.class);
  
  /** Type reference for incoming data points */
  private static TypeReference<ArrayList<IncomingDataPoint>> TR_INCOMING =
    new TypeReference<ArrayList<IncomingDataPoint>>() {};
  
  /** Type reference for uid assignments */
  private static TypeReference<HashMap<String, List<String>>> UID_ASSIGN =
    new TypeReference<HashMap<String, List<String>>>() {};
    
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
   * Parses one or more data points for storage
   * @return an array of data points to process for storage
   * @throws JSONException if parsing failed
   * @throws BadRequestException if the content was missing or parsing failed
   */
  @Override
  public List<IncomingDataPoint> parsePutV1() {
    if (!query.hasContent()) {
      throw new BadRequestException("Missing request content");
    }

    // convert to a string so we can handle character encoding properly
    final String content = query.getContent().trim();
    final int firstbyte = content.charAt(0);
    try {
      if (firstbyte == '{') {
        final IncomingDataPoint dp = 
          JSON.parseToObject(content, IncomingDataPoint.class);
        final ArrayList<IncomingDataPoint> dps = 
          new ArrayList<IncomingDataPoint>(1);
        dps.add(dp);
        return dps;
      } else {
        return JSON.parseToObject(content, TR_INCOMING);
      }
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException("Unable to parse the given JSON", iae);
    }
  }
  
  /**
   * Parses a suggestion query
   * @return a hash map of key/value pairs
   * @throws JSONException if parsing failed
   * @throws BadRequestException if the content was missing or parsing failed
   */
  @Override
  public HashMap<String, String> parseSuggestV1() {
    final String json = query.getContent();
    if (json == null || json.isEmpty()) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
          "Missing message content",
          "Supply valid JSON formatted data in the body of your request");
    }
    try {
      return JSON.parseToObject(query.getContent(), 
          new TypeReference<HashMap<String, String>>(){});
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException("Unable to parse the given JSON", iae);
    }
  }
  
  /**
   * Parses a list of metrics, tagk and/or tagvs to assign UIDs to
   * @return as hash map of lists for the different types
   * @throws JSONException if parsing failed
   * @throws BadRequestException if the content was missing or parsing failed
   */
  public HashMap<String, List<String>> parseUidAssignV1() {
    final String json = query.getContent();
    if (json == null || json.isEmpty()) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
          "Missing message content",
          "Supply valid JSON formatted data in the body of your request");
    }
    try {
      return JSON.parseToObject(json, UID_ASSIGN);
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException("Unable to parse the given JSON", iae);
    }
  }
  
  /**
   * Parses a timeseries data query
   * @return A TSQuery with data ready to validate
   * @throws JSONException if parsing failed
   * @throws BadRequestException if the content was missing or parsing failed
   */
  public TSQuery parseQueryV1() {
    final String json = query.getContent();
    if (json == null || json.isEmpty()) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
          "Missing message content",
          "Supply valid JSON formatted data in the body of your request");
    }
    try {
      return JSON.parseToObject(json, TSQuery.class);
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException("Unable to parse the given JSON", iae);
    }
  }
  
  /**
   * Formats the results of an HTTP data point storage request
   * @param results A map of results. The map will consist of:
   * <ul><li>success - (long) the number of successfully parsed datapoints</li>
   * <li>failed - (long) the number of datapoint parsing failures</li>
   * <li>errors - (ArrayList<HashMap<String, Object>>) an optional list of 
   * datapoints that had errors. The nested map has these fields:
   * <ul><li>error - (String) the error that occurred</li>
   * <li>datapoint - (IncomingDatapoint) the datapoint that generated the error
   * </li></ul></li></ul>
   * @return A JSON formatted byte array
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatPutV1(final Map<String, Object> results) {
    return this.serializeJSON(results);
  }
  
  /**
   * Formats a suggestion response
   * @param suggestions List of suggestions for the given type
   * @return A JSON formatted byte array
   * @throws JSONException if serialization failed
   */
  @Override
  public ChannelBuffer formatSuggestV1(final List<String> suggestions) {
    return this.serializeJSON(suggestions);
  }
  
  /**
   * Format the serializer status map
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatSerializersV1() {
    return serializeJSON(HttpQuery.getSerializerStatus());
  }
  
  /**
   * Format the list of implemented aggregators
   * @param aggregators The list of aggregation functions
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatAggregatorsV1(final Set<String> aggregators) {
    return this.serializeJSON(aggregators);
  }
  
  /**
   * Format a hash map of information about the OpenTSDB version
   * @param version A hash map with version information
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatVersionV1(final Map<String, String> version) {
    return this.serializeJSON(version);
  }
  
  /**
   * Format a response from the DropCaches call
   * @param response A hash map with a response
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatDropCachesV1(final Map<String, String> response) {
    return this.serializeJSON(response);
  }
  
  /**
   * Format a response from the Uid Assignment RPC
   * @param response A map of lists of pairs representing the results of the
   * assignment
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatUidAssignV1(final 
      Map<String, TreeMap<String, String>> response) {
    return this.serializeJSON(response);
  }
  
  /**
   * Format the results from a timeseries data query
   * @param data_query The TSQuery object used to fetch the results
   * @param results The data fetched from storage
   * @return A ChannelBuffer object to pass on to the caller
   */
  public ChannelBuffer formatQueryV1(final TSQuery data_query, 
      final List<DataPoints[]> results) {
    
    final boolean as_arrays = this.query.hasQueryStringParam("arrays");
    final String jsonp = this.query.getQueryStringParam("jsonp");
    
    // todo - this should be streamed at some point since it could be HUGE
    final ChannelBuffer response = ChannelBuffers.dynamicBuffer();
    final OutputStream output = new ChannelBufferOutputStream(response);
    try {
      // don't forget jsonp
      if (jsonp != null && !jsonp.isEmpty()) {
        output.write((jsonp + "(").getBytes(query.getCharset()));
      }
      JsonGenerator json = JSON.getFactory().createGenerator(output);
      json.writeStartArray();
      
      for (DataPoints[] separate_dps : results) {
        for (DataPoints dps : separate_dps) {
          json.writeStartObject();
          
          json.writeStringField("metric", dps.metricName());
          
          json.writeFieldName("tags");
          json.writeStartObject();
          if (dps.getTags() != null) {
            for (Map.Entry<String, String> tag : dps.getTags().entrySet()) {
              json.writeStringField(tag.getKey(), tag.getValue());
            }
          }
          json.writeEndObject();
          
          json.writeFieldName("aggregated_tags");
          json.writeStartArray();
          if (dps.getAggregatedTags() != null) {
            for (String atag : dps.getAggregatedTags()) {
              json.writeString(atag);
            }
          }
          json.writeEndArray();
          
          // now the fun stuff, dump the data
          json.writeFieldName("dps");
          
          // default is to write a map, otherwise write arrays
          if (as_arrays) {
            json.writeStartArray();
            for (final DataPoint dp : dps) {
              json.writeStartArray();
              json.writeNumber(dp.timestamp());
              json.writeNumber(
                  dp.isInteger() ? dp.longValue() : dp.doubleValue());
              json.writeEndArray();
            }
            json.writeEndArray();
          } else {
            json.writeStartObject();
            for (final DataPoint dp : dps) {
              json.writeNumberField(Long.toString(dp.timestamp()), 
                  dp.isInteger() ? dp.longValue() : dp.doubleValue());
            }
            json.writeEndObject();
          }

          // close the results for this particular query
          json.writeEndObject();
        }
      }
    
      // close
      json.writeEndArray();
      json.close();
      
      if (jsonp != null && !jsonp.isEmpty()) {
        output.write(")".getBytes());
      }
      return response;
    } catch (IOException e) {
      LOG.error("Unexpected exception", e);
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Helper object for the format calls to wrap the JSON response in a JSONP
   * function if requested. Used for code dedupe.
   * @param obj The object to serialize
   * @return A ChannelBuffer to pass on to the query
   * @throws JSONException if serialization failed
   */
  private ChannelBuffer serializeJSON(final Object obj) {
    if (query.hasQueryStringParam("jsonp")) {
      return ChannelBuffers.wrappedBuffer(
          JSON.serializeToJSONPBytes(query.getQueryStringParam("jsonp"), 
          obj));
    }
    return ChannelBuffers.wrappedBuffer(JSON.serializeToBytes(obj));
  }
}
