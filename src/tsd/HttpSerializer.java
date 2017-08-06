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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.tsd.AnnotationRpc.AnnotationBulkDelete;
import net.opentsdb.tsd.QueryRpc.LastPointQuery;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSONException;

/**
 * Abstract base class for Serializers; plugins that handle converting requests
 * and responses between OpenTSDB's internal data and various popular formats
 * such as JSON, XML, OData, etc. They can also be used to accept inputs from 
 * existing collection systems such as CollectD.
 * <p>
 * The serializer workflow is as follows:
 * <ul><li>Request comes in via the HTTP API</li>
 * <li>The proper serializer is instantiated via:
 * <ul><li>Query string parameter "serializer=&lt;shortName&gt;"</li>
 * <li>If no query string parameter is found, the Content-Type is parsed</li>
 * <li>Otherwise the default serializer is used</li></ul></li>
 * <li>The request is routed to an RPC handler</li>
 * <li>If the handler needs details for a complex request, it calls on the 
 * proper serializer's "parseX" method to get a query object</li>
 * <li>The RPC handler fetches and organizes the data</li>
 * <li>The handler passes the data to the proper serializer's "formatX" 
 * method</li>
 * <li>The serializer formats the data and sends it back as a byte array</li>
 * </ul>
 * <b>Warning:</b> Every HTTP request will instantiate a new serializer object
 * (except for a few that don't require it) so please avoid creating heavy
 * objects in the constructor, parse or format methods. Instead, use the 
 * {@link #initialize} method to instantiate thread-safe, static objects that
 * you need for de/serializtion. It will be called once on TSD startup.
 * <p>
 * <b>Note:</b> If a method needs to throw an exception due to user error, such
 * as missing data or a bad request, throw a {@link BadRequestException} with
 * a status code, error message and optional details.
 * <p>
 * Runtime exceptions, anything that goes wrong internally with your serializer,
 * will be returned with a 500 Internal Server Error status.
 * <p>
 * <b>Note:</b> You can change the HTTP status code before returning from a 
 * "formatX" method by accessing "this.query.response().setStatus()" and
 * providing an {@link HttpResponseStatus} object.
 * <p>
 * <b>Note:</b> You can also set response headers via 
 * "this.query.response().headers().set()". The "Content-Type" header will be set
 * automatically with the "response_content_type" field value that can be
 * overridden by the plugin. HttpQuery will also set some other headers before
 * returning
 * @since 2.0
 */
public abstract class HttpSerializer {
  /** Content type to use for matching a serializer to incoming requests */
  protected String request_content_type = "application/json";
  
  /** Content type to return with data from this serializer */
  protected String response_content_type = "application/json; charset=UTF-8";
  
  /** The query used for accessing the DefaultHttpResponse object and other 
   * information */
  protected final HttpQuery query;
  
  /**
   * Empty constructor required for plugin operation
   */
  public HttpSerializer() {
    this(null);
  }

  /**
   * Constructor that serializers must implement. This is how each plugin will 
   * get the request content and have the option to set headers or a custom
   * status code in the response.
   * <p>
   * <b>Note:</b> A new serializer is instantiated for every HTTP connection, so
   * don't do any heavy object creation here. Instead, use the 
   * {@link #initialize} method to setup static, thread-safe objects if you 
   * need stuff like that 
   * @param query
   */
  public HttpSerializer(final HttpQuery query) {
    this.query = query;
  }
  
  /**
   * Initializer called one time when the TSD starts up and loads serializer 
   * plugins. You should use this method to setup static, thread-safe objects
   * required for parsing or formatting data. 
   * @param tsdb The TSD this plugin belongs to. Use it to fetch config data
   * if require.
   */
  public abstract void initialize(final TSDB tsdb);
  
  /**
   * Called when the TSD is shutting down so implementations can gracefully 
   * close their objects or connections if necessary
   * @return An object, usually a Boolean, used to wait on during shutdown
   */
  public abstract Deferred<Object> shutdown();
  
  /** 
   * The version of this serializer plugin in the format "MAJOR.MINOR.MAINT"
   * The MAJOR version should match the major version of OpenTSDB, e.g. if the
   * plugin is associated with 2.0.1, your version should be 2.x.x. 
   * @return the version as a String
   */
  public abstract String version();
  
  /**
   * The simple name for this serializer referenced by users.
   * The name should be lower case, all one word without any odd characters
   * so it can be used in a query string. E.g. "json" or "xml" or "odata"
   * @return the name of the serializer
   */
  public abstract String shortName();

  /** @return the incoming content type */
  public String requestContentType() {
    return this.request_content_type;
  }
  
  /** @return the outgoing content type */
  public String responseContentType() {
    return this.response_content_type;
  }
  
  /**
   * Parses one or more data points for storage
   * @return an array of data points to process for storage
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public List<IncomingDataPoint> parsePutV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parsePutV1");
  }
  
  /**
   * Parses one or more data points for storage
   * @param <T> The type of incoming data points to parse.
   * @param type The type of the class to parse.
   * @param typeReference The reference to use for parsing.
   * @return an array of data points to process for storage
   * @throws BadRequestException if the plugin has not implemented this method
   * @since 2.4
   */
  public <T extends IncomingDataPoint> List<T> parsePutV1(final Class<T> type,
      final TypeReference<ArrayList<T>> typeReference) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED,
        "The requested API endpoint has not been implemented",
        this.getClass().getCanonicalName() +
            " has not implemented parsePutV1");
  }
  
  /**
   * Parses a suggestion query
   * @return a hash map of key/value pairs
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public HashMap<String, String> parseSuggestV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseSuggestV1");
  }
  
  /**
   * Parses a list of metrics, tagk and/or tagvs to assign UIDs to
   * @return as hash map of lists for the different types
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public HashMap<String, List<String>> parseUidAssignV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseUidAssignV1");
  }
  
  /**
   * Parses metrics, tagk or tagvs type and name to rename UID
   * @return as hash map of type and name
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public HashMap<String, String> parseUidRenameV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED,
        "The requested API endpoint has not been implemented",
        this.getClass().getCanonicalName() +
        " has not implemented parseUidRenameV1");
  }

  /**
   * Parses a SearchQuery request
   * @return The parsed search query
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public SearchQuery parseSearchQueryV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseSearchQueryV1");
  }
  
  /**
   * Parses a timeseries data query
   * @return A TSQuery with data ready to validate
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public TSQuery parseQueryV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseQueryV1");
  }
  
  /**
   * Parses a last data point query
   * @return A LastPointQuery to work with
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public LastPointQuery parseLastPointQueryV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseLastPointQueryV1");
  }
  
  /**
   * Parses a single UIDMeta object
   * @return the parsed meta data object
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public UIDMeta parseUidMetaV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseUidMetaV1");
  }
  
  /**
   * Parses a single TSMeta object
   * @return the parsed meta data object
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public TSMeta parseTSMetaV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseTSMetaV1");
  }
  
  /**
   * Parses a single Tree object
   * @return the parsed tree object
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public Tree parseTreeV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseTreeV1");
  }
  
  /**
   * Parses a single TreeRule object
   * @return the parsed rule object
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public TreeRule parseTreeRuleV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseTreeRuleV1");
  }
  
  /**
   * Parses one or more tree rules
   * @return A list of one or more rules
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public List<TreeRule> parseTreeRulesV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseTreeRulesV1");
  }
  
  /**
   * Parses a tree ID and optional list of TSUIDs to search for collisions or
   * not matched TSUIDs.
   * @return A map with "treeId" as an integer and optionally "tsuids" as a 
   * List&lt;String&gt; 
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public Map<String, Object> parseTreeTSUIDsListV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseTreeCollisionNotMatchedV1");
  }
  
  /**
   * Parses an annotation object
   * @return An annotation object
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public Annotation parseAnnotationV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseAnnotationV1");
  }
  
  /**
   * Parses a list of annotation objects
   * @return A list of annotation object
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public List<Annotation> parseAnnotationsV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseAnnotationsV1");
  }
  
  /**
   * Parses a bulk annotation deletion query object
   * @return Settings used to bulk delete annotations
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public AnnotationBulkDelete parseAnnotationBulkDeleteV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented parseAnnotationBulkDeleteV1");
  }
  
  /**
   * Formats the results of an HTTP data point storage request
   * @param results A map of results. The map will consist of:
   * <ul><li>success - (long) the number of successfully parsed datapoints</li>
   * <li>failed - (long) the number of datapoint parsing failures</li>
   * <li>errors - (ArrayList&lt;HashMap&lt;String, Object&gt;&gt;) an optional list of 
   * datapoints that had errors. The nested map has these fields:
   * <ul><li>error - (String) the error that occurred</li>
   * <li>datapoint - (IncomingDatapoint) the datapoint that generated the error
   * </li></ul></li></ul>
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatPutV1(final Map<String, Object> results) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatPutV1");
  }
  
  /**
   * Formats a suggestion response
   * @param suggestions List of suggestions for the given type
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatSuggestV1(final List<String> suggestions) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatSuggestV1");
  }

  /**
   * Format the serializers status map
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatSerializersV1() {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatSerializersV1");
  }
  
  /**
   * Format the list of implemented aggregators
   * @param aggregators The list of aggregation functions
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatAggregatorsV1(final Set<String> aggregators) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatAggregatorsV1");
  }
  
  /**
   * Format a hash map of information about the OpenTSDB version
   * @param version A hash map with version information
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatVersionV1(final Map<String, String> version) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatVersionV1");
  }
  
  /**
   * Format a response from the DropCaches call
   * @param response A hash map with a response
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatDropCachesV1(final Map<String, String> response) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatDropCachesV1");
  }
  
  /**
   * Format a response from the Uid Assignment RPC
   * @param response A map of lists of pairs representing the results of the
   * assignment
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatUidAssignV1(final 
      Map<String, TreeMap<String, String>> response) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatUidAssignV1");
  }
  
  /**
   * Format a response from the Uid Rename RPC
   * @param response A map of result and reason for error of the rename
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatUidRenameV1(final Map<String, String> response) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED,
        "The requested API endpoint has not been implemented",
        this.getClass().getCanonicalName() +
        " has not implemented formatUidRenameV1");
  }

  /**
   * Format the results from a timeseries data query
   * @param query The TSQuery object used to fetch the results
   * @param results The data fetched from storage
   * @param globals An optional list of global annotation objects
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatQueryV1(final TSQuery query, 
      final List<DataPoints[]> results, final List<Annotation> globals) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatQueryV1");
  }
  
  /**
   * Format the results from a timeseries data query
   * @param query The TSQuery object used to fetch the results
   * @param results The data fetched from storage
   * @param globals An optional list of global annotation objects
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   * @since 2.2
   */
  public Deferred<ChannelBuffer> formatQueryAsyncV1(final TSQuery query, 
      final List<DataPoints[]> results, final List<Annotation> globals) 
      throws IOException {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatQueryV1");
  }
  
  /**
   * Format a list of last data points
   * @param data_points The results of the query
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatLastPointQueryV1(
      final List<IncomingDataPoint> data_points) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatLastPointQueryV1");
  }
  
  /**
   * Format a single UIDMeta object
   * @param meta The UIDMeta object to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatUidMetaV1(final UIDMeta meta) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatUidMetaV1");
  }
  
  /**
   * Format a single TSMeta object
   * @param meta The TSMeta object to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatTSMetaV1(final TSMeta meta) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatTSMetaV1");
  }
  
  /**
   * Format a a list of TSMeta objects
   * @param metas The list of TSMeta objects to serialize
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatTSMetaListV1(final List<TSMeta> metas) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatTSMetaV1");
  }
  
  /**
   * Format a single Branch object
   * @param branch The branch to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatBranchV1(final Branch branch) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatBranchV1");
  }
  
  /**
   * Format a single tree object
   * @param tree tree to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatTreeV1(final Tree tree) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatTreeV1");
  }
  
  /**
   * Format a list of tree objects. Note that the list may be empty if no trees
   * were present.
   * @param trees A list of one or more trees to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatTreesV1(final List<Tree> trees) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatTreesV1");
  }
  
  /**
   * Format a single TreeRule object
   * @param rule The rule to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatTreeRuleV1(final TreeRule rule) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatTreeRuleV1");
  }
  
  /**
   * Format a map of one or more TSUIDs that collided or were not matched
   * @param results The list of results. Collisions: key = tsuid, value = 
   * collided TSUID. Not Matched: key = tsuid, value = message about non matched
   * rules.
   * @param is_collisions Whether or the map is a collision result set (true) or
   * a not matched set (false).
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatTreeCollisionNotMatchedV1(
      final Map<String, String> results, final boolean is_collisions) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatTreeCollisionNotMatched");
  }
  
  /**
   * Format the results of testing one or more TSUIDs through a tree's ruleset
   * @param results The list of results. Main map key is the tsuid. Child map:
   * "branch" : Parsed branch result, may be null
   * "meta" : TSMeta object, may be null
   * "messages" : An ArrayList&lt;String&gt; of one or more messages 
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatTreeTestV1(final 
      HashMap<String, HashMap<String, Object>> results) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatTreeTestV1");
  }
  
  /**
   * Format an annotation object
   * @param note The annotation object to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatAnnotationV1(final Annotation note) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatAnnotationV1");
  }
  
  /**
   * Format a list of annotation objects
   * @param notes The annotation objects to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatAnnotationsV1(final List<Annotation> notes) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatAnnotationsV1");
  }
  
  /**
   * Format the results of a bulk annotation deletion
   * @param request The request to handle.
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatAnnotationBulkDeleteV1(
      final AnnotationBulkDelete request) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatAnnotationBulkDeleteV1");
  }

  /**
   * Format a list of statistics
   * @param stats The statistics list to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatStatsV1(final List<IncomingDataPoint> stats) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatStatsV1");
  }
  
  /**
   * Format a list of thread statistics
   * @param stats The thread statistics list to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   * @since 2.2
   */
  public ChannelBuffer formatThreadStatsV1(final List<Map<String, Object>> stats) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatThreadStatsV1");
  }
  
  /**
   * format a list of region client statistics
   * @param stats The list of region client stats to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   * @since 2.2
   */
  public ChannelBuffer formatRegionStatsV1(final List<Map<String, Object>> stats) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatRegionStatsV1");
  }
  
  /**
   * Format a list of JVM statistics
   * @param map The JVM stats list to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   * @since 2.2
   */
  public ChannelBuffer formatJVMStatsV1(final Map<String, Map<String, Object>> map) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatJVMStatsV1");
  }
  
  /**
   * Format the query stats
   * @param query_stats Map of query statistics
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   * @since 2.2
   */
  public ChannelBuffer formatQueryStatsV1(final Map<String, Object> query_stats) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatQueryStatsV1");
  }
  
  /**
   * Format the response from a search query
   * @param results The query (hopefully filled with results) to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatSearchResultsV1(final SearchQuery results) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatSearchResultsV1");
  }
  
  /**
   * Format the running configuration
   * @param config The running config to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatConfigV1(final Config config) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatConfigV1");
  }
  
  /**
   * Format the loaded filter configurations
   * @param config The filters to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   */
  public ChannelBuffer formatFilterConfigV1(
      final Map<String, Map<String, String>> config) {
    throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
        "The requested API endpoint has not been implemented", 
        this.getClass().getCanonicalName() + 
        " has not implemented formatFilterConfigV1");
  }
  
  /**
   * Formats a 404 error when an endpoint or file wasn't found
   * <p>
   * <b>WARNING:</b> If overriding, make sure this method catches all errors and
   * returns a byte array with a simple string error at the minimum
   * @return A standard JSON error
   */
  public ChannelBuffer formatNotFoundV1() {
    StringBuilder output = 
      new StringBuilder(1024);
    if (query.hasQueryStringParam("jsonp")) {
      output.append(query.getQueryStringParam("jsonp") + "(");
    }
    output.append("{\"error\":{\"code\":");
    output.append(404);
    output.append(",\"message\":\"");
    if (query.apiVersion() > 0) {
      output.append("Endpoint not found");
    } else {
      output.append("Page not found");
    }
    output.append("\"}}");
    if (query.hasQueryStringParam("jsonp")) {
      output.append(")");
    }
    return ChannelBuffers.copiedBuffer(
        output.toString().getBytes(this.query.getCharset()));
  }
  
  /**
   * Format a bad request exception, indicating an invalid request from the
   * user
   * <p>
   * <b>WARNING:</b> If overriding, make sure this method catches all errors and
   * returns a byte array with a simple string error at the minimum
   * @param exception The exception to format
   * @return A standard JSON error
   */
  public ChannelBuffer formatErrorV1(final BadRequestException exception) {
    StringBuilder output = 
      new StringBuilder(exception.getMessage().length() * 2);
    final String jsonp = query.getQueryStringParam("jsonp");
    if (jsonp != null && !jsonp.isEmpty()) {
      output.append(query.getQueryStringParam("jsonp") + "(");
    }
    output.append("{\"error\":{\"code\":");
    output.append(exception.getStatus().getCode());
    final StringBuilder msg = new StringBuilder(exception.getMessage().length());
    HttpQuery.escapeJson(exception.getMessage(), msg);
    output.append(",\"message\":\"").append(msg.toString()).append("\"");
    if (!exception.getDetails().isEmpty()) {
      final StringBuilder details = new StringBuilder(
          exception.getDetails().length());
      HttpQuery.escapeJson(exception.getDetails(), details);
      output.append(",\"details\":\"").append(details.toString()).append("\"");
    }
    if (query.showStackTrace()) {
      ThrowableProxy tp = new ThrowableProxy(exception);
      tp.calculatePackagingData();
      final String pretty_exc = ThrowableProxyUtil.asString(tp);
      final StringBuilder trace = new StringBuilder(pretty_exc.length());
      HttpQuery.escapeJson(pretty_exc, trace);
      output.append(",\"trace\":\"").append(trace.toString()).append("\"");
    }
    output.append("}}");
    if (jsonp != null && !jsonp.isEmpty()) {
      output.append(")");
    }
    return ChannelBuffers.copiedBuffer(
        output.toString().getBytes(this.query.getCharset()));
  }
  
  /**
   * Format an internal error exception that was caused by the system
   * Should return a 500 error
   * <p>
   * <b>WARNING:</b> If overriding, make sure this method catches all errors and
   * returns a byte array with a simple string error at the minimum
   * @param exception The system exception to format
   * @return A standard JSON error
   */
  public ChannelBuffer formatErrorV1(final Exception exception) {
    String message = exception.getMessage();
    // NPEs have a null for the message string (why?!?!?!)
    if (exception.getClass() == NullPointerException.class) {
      message = "An internal null pointer exception was thrown";
    } else if (message == null) {
      message = "An unknown exception occurred";
    }
    StringBuilder output = 
      new StringBuilder(message.length() * 2);
    final String jsonp = query.getQueryStringParam("jsonp");
    if (jsonp != null && !jsonp.isEmpty()) {
      output.append(query.getQueryStringParam("jsonp") + "(");
    }
    output.append("{\"error\":{\"code\":");
    output.append(500);
    final StringBuilder msg = new StringBuilder(message.length());
    HttpQuery.escapeJson(message, msg);
    output.append(",\"message\":\"").append(msg.toString()).append("\"");
    if (query.showStackTrace()) {
      ThrowableProxy tp = new ThrowableProxy(exception);
      tp.calculatePackagingData();
      final String pretty_exc = ThrowableProxyUtil.asString(tp);
      final StringBuilder trace = new StringBuilder(pretty_exc.length());
      HttpQuery.escapeJson(pretty_exc, trace);
      output.append(",\"trace\":\"").append(trace.toString()).append("\"");
    }
    output.append("}}");
    if (jsonp != null && !jsonp.isEmpty()) {
      output.append(")");
    }
    return ChannelBuffers.copiedBuffer(
        output.toString().getBytes(this.query.getCharset()));
  }
}
