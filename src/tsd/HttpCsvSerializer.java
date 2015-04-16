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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.QueryException;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.tsd.AnnotationRpc.AnnotationBulkDelete;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * Implementation of the base serializer class with JSON as the format
 * <p>
 * <b>Note:</b> This class is not final and the implementations are not either
 * so that we can extend this default class with slightly different methods
 * when needed and retain everything else.
 * @since 2.0
 */
class HttpCsvSerializer extends HttpJsonSerializer {
  /**
   * Default constructor necessary for plugin implementation
   */
  public HttpCsvSerializer() {
    super();
  }
  
  /**
   * Constructor that sets the query object
   * @param query Request/resposne object
   */
  public HttpCsvSerializer(final HttpQuery query) {
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
    return "1.0.0";
  }

  /** @return the shortname */
  @Override
  public String shortName() {
    return "csv";
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
    return this.serializeCsv(results);
  }
  
  /**
   * Formats a suggestion response
   * @param suggestions List of suggestions for the given type
   * @return A JSON formatted byte array
   * @throws JSONException if serialization failed
   */
  @Override
  public ChannelBuffer formatSuggestV1(final List<String> suggestions) {
    return this.serializeCsv(suggestions);
  }
  
  /**
   * Format the serializer status map
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatSerializersV1() {
    return serializeCsv(HttpQuery.getSerializerStatus());
  }
  
  /**
   * Format the list of implemented aggregators
   * @param aggregators The list of aggregation functions
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatAggregatorsV1(final Set<String> aggregators) {
    return this.serializeCsv(aggregators);
  }
  
  /**
   * Format a hash map of information about the OpenTSDB version
   * @param version A hash map with version information
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatVersionV1(final Map<String, String> version) {
    return this.serializeCsv(version);
  }
  
  /**
   * Format a response from the DropCaches call
   * @param response A hash map with a response
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatDropCachesV1(final Map<String, String> response) {
    return this.serializeCsv(response);
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
    return this.serializeCsv(response);
  }
  
  /**
   * Format the results from a timeseries data query
   * @param data_query The TSQuery object used to fetch the results
   * @param results The data fetched from storage
   * @param globals An optional list of global annotation objects
   * @return A ChannelBuffer object to pass on to the caller
   */
  public ChannelBuffer formatQueryV1(final TSQuery data_query, 
      final List<DataPoints[]> results, final List<Annotation> globals) {
    try {
      return formatQueryAsyncV1(data_query, results, globals)
          .joinUninterruptibly();
    } catch (QueryException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Shouldn't be here", e);
    }
  }
  
  /**
   * Format the results from a timeseries data query
   * @param data_query The TSQuery object used to fetch the results
   * @param results The data fetched from storage
   * @param globals An optional list of global annotation objects
   * @return A Deferred<ChannelBuffer> object to pass on to the caller
   * @throws IOException if serialization failed
   * @since 2.2
   */
  public Deferred<ChannelBuffer> formatQueryAsyncV1(final TSQuery data_query, 
      final List<DataPoints[]> results, final List<Annotation> globals) 
          throws IOException {
    final long start = DateTime.currentTimeMillis();
    
    // buffers and an array list to stored the deferreds
    final ChannelBuffer response = ChannelBuffers.dynamicBuffer();
    final OutputStream output = new ChannelBufferOutputStream(response);
    // too bad an inner class can't modify a primitive. This is a work around 
    final List<Boolean> timeout_flag = new ArrayList<Boolean>(1);
    timeout_flag.add(false);
    
    
    // start the JSON generator and write the opening array
    final StringBuilder csv = new StringBuilder();
    
    //setup the header
    csv.append("Metric,");
    csv.append("Aggregator,");
    csv.append("Downsample,");
    csv.append("Rate,");
		// look for tags
		final Set<String> tagSet = new HashSet<String>();
		if (data_query.getQueries() != null && data_query.getQueries().size() > 0) {
			for (TSSubQuery tsQuery : data_query.getQueries()) {
				if (tsQuery.getTags() != null && tsQuery.getTags().size() > 0) {
					for (Map.Entry<String, String> entryTag : tsQuery.getTags()
					    .entrySet()) {
						tagSet.add(entryTag.getKey());
					}
				}
			}
		}
		for (String tag : tagSet) {
			csv.append(tag + ",");
		}
		csv.append("Timestamp,");
    csv.append("Value");
		csv.append("\n");
       
    
 
    /**
     * Every individual data point set (the result of a query and possibly a
     * group by) will initiate an asynchronous metric/tag UID to name resolution
     * and then print to the buffer.
     * NOTE that because this is asynchronous, the order of results is
     * indeterminate.
     */
    class DPsResolver implements Callback<Deferred<Object>, Object> {
      /** Has to be final to be shared with the nested classes */
      final StringBuilder metric = new StringBuilder(256);
      /** Resolved tags */
      final Map<String, String> tags = new HashMap<String, String>();
      /** Resolved aggregated tags */
      final List<String> agg_tags = new ArrayList<String>();
      /** A list storing the metric and tag resolve calls */
      final List<Deferred<Object>> resolve_deferreds = 
          new ArrayList<Deferred<Object>>();
      /** The data points to serialize */
      final DataPoints dps;
      
      public DPsResolver(final DataPoints dps) {
        this.dps = dps;
      }
      
      /** Resolves the metric UID to a name*/
      class MetricResolver implements Callback<Object, String> {
        public Object call(final String metric) throws Exception {
          DPsResolver.this.metric.append(metric);
          return null;
        }
      }
      
      /** Resolves the tag UIDs to a key/value string set */
      class TagResolver implements Callback<Object, Map<String, String>> {
        public Object call(final Map<String, String> tags) throws Exception {
          DPsResolver.this.tags.putAll(tags);
          return null;
        }
      }
      
      /** Resolves aggregated tags */
      class AggTagResolver implements Callback<Object, List<String>> {
        public Object call(final List<String> tags) throws Exception {
          DPsResolver.this.agg_tags.addAll(tags);
          return null;
        }
      }
      
      /** After the metric and tags have been resolved, this will print the
       * results to the output buffer in the proper format.
       */
      class WriteToBuffer implements Callback<Object, ArrayList<Object>> {
        final DataPoints dps;
        
        /**
         * Default ctor that takes a data point set
         * @param dps Datapoints to print
         */
        public WriteToBuffer(final DataPoints dps) {
          this.dps = dps;
        }
        
        /**
         * Handles writing the data to the output buffer. The results of the
         * deferreds don't matter as they will be stored in the class final
         * variables.
         */
        public Object call(final ArrayList<Object> deferreds) throws Exception {
          // default is to write a map, otherwise write arrays
          if (!timeout_flag.get(0)) {
            for (final DataPoint dp : dps) {
              if (dp.timestamp() < data_query.startTime() || 
                  dp.timestamp() > data_query.endTime()) {
                continue;
              }
              final long timestamp = data_query.getMsResolution() ? 
                  dp.timestamp() : dp.timestamp() / 1000;
              
              // metric name
  						csv.append(metric.toString() + ",");

  						// query stuff
  						final TSSubQuery orig_query = data_query.getQueries().get(
  						    dps.getQueryIndex());
  						csv.append(orig_query.getAggregator() + ",");
  						csv.append(orig_query.getDownsample() + ",");
  						csv.append(orig_query.getRate() + ",");

  						//populate the tags
  						for (String tag : tagSet) {
  							csv.append(tags.get(tag) + ",");
  						}
  						
  					  // timestamp
  						csv.append(timestamp + ",");

  						// values
  						if (dp.isInteger()) {
  							csv.append(dp.longValue());
  						} else {
  							csv.append(dp.doubleValue());
  						}

  						csv.append("\n");
            }
          }
          
          return null;
        }
      }
      
      /**
       * When called, initiates a resolution of metric and tag UIDs to names, 
       * then prints to the output buffer once they are completed.
       */
      public Deferred<Object> call(final Object obj) throws Exception {
        resolve_deferreds.add(dps.metricNameAsync()
            .addCallback(new MetricResolver()));
        resolve_deferreds.add(dps.getTagsAsync()
            .addCallback(new TagResolver()));
        resolve_deferreds.add(dps.getAggregatedTagsAsync()
            .addCallback(new AggTagResolver()));
        return Deferred.group(resolve_deferreds)
            .addCallback(new WriteToBuffer(dps));
      }

    }
    
    // We want the serializer to execute serially so we need to create a callback
    // chain so that when one DPsResolver is finished, it triggers the next to
    // start serializing.
    final Deferred<Object> cb_chain = new Deferred<Object>();

    for (DataPoints[] separate_dps : results) {
      for (DataPoints dps : separate_dps) {
        try {
          cb_chain.addCallback(new DPsResolver(dps));
        } catch (Exception e) {
          throw new RuntimeException("Unexpected error durring resolution", e);
        }
      }
    }
  
    /** Final callback to close out the JSON array and return our results */
    class FinalCB implements Callback<ChannelBuffer, Object> {
      public ChannelBuffer call(final Object obj)
          throws Exception {
        data_query.getQueryStats().setTimeSerialization(
            DateTime.currentTimeMillis() - start);
        data_query.getQueryStats().markComplete();
        
        //write it back to the output
        output.write(csv.toString().getBytes(query.getCharset()));
        
        return response;
      }
    }

    // trigger the callback chain here
    cb_chain.callback(null);
    return cb_chain.addCallback(new FinalCB());
  }
  
  /**
   * Format a list of last data points
   * @param data_points The results of the query
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatLastPointQueryV1(
      final List<IncomingDataPoint> data_points) {
    return this.serializeCsv(data_points);
  }
  
  /**
   * Format a single UIDMeta object
   * @param meta The UIDMeta object to serialize
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatUidMetaV1(final UIDMeta meta) {
    return this.serializeCsv(meta);
  }
  
  /**
   * Format a single TSMeta object
   * @param meta The TSMeta object to serialize
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatTSMetaV1(final TSMeta meta) {
    return this.serializeCsv(meta);
  }
  
  /**
   * Format a a list of TSMeta objects
   * @param meta The list of TSMeta objects to serialize
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatTSMetaListV1(final List<TSMeta> metas) {
    return this.serializeCsv(metas);
  }
  
  /**
   * Format a single Branch object
   * @param branch The branch to serialize
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatBranchV1(final Branch branch) {
    return this.serializeCsv(branch);
  }
  
  /**
   * Format a single tree object
   * @param tree A tree to format
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatTreeV1(final Tree tree) {
    return this.serializeCsv(tree);
  }
  
  /**
   * Format a list of tree objects. Note that the list may be empty if no trees
   * were present.
   * @param trees A list of one or more trees to serialize
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatTreesV1(final List<Tree> trees) {
    return this.serializeCsv(trees);
  }
  
  /**
   * Format a single TreeRule object
   * @param rule The rule to serialize
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatTreeRuleV1(final TreeRule rule) {
    return serializeCsv(rule);
  }
  
  /**
   * Format a map of one or more TSUIDs that collided or were not matched
   * @param results The list of results. Collisions: key = tsuid, value = 
   * collided TSUID. Not Matched: key = tsuid, value = message about non matched
   * rules.
   * @param is_collision Whether or the map is a collision result set (true) or
   * a not matched set (false).
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatTreeCollisionNotMatchedV1(
      final Map<String, String> results, final boolean is_collisions) {
    return serializeCsv(results);
  }
  
  /**
   * Format the results of testing one or more TSUIDs through a tree's ruleset
   * @param results The list of results. Main map key is the tsuid. Child map:
   * "branch" : Parsed branch result, may be null
   * "meta" : TSMeta object, may be null
   * "messages" : An ArrayList<String> of one or more messages 
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatTreeTestV1(final 
      HashMap<String, HashMap<String, Object>> results) {
    return serializeCsv(results);
  }
  
  /**
   * Format an annotation object
   * @param note The annotation object to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatAnnotationV1(final Annotation note) {
    return serializeCsv(note);
  }
  
  /**
   * Format a list of annotation objects
   * @param notes The annotation objects to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatAnnotationsV1(final List<Annotation> notes) {
    return serializeCsv(notes);
  }
  
  /**
   * Format the results of a bulk annotation deletion
   * @param notes The annotation deletion request to return
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatAnnotationBulkDeleteV1(
      final AnnotationBulkDelete request) {
    return serializeCsv(request);
  }
  
  /**
   * Format a list of statistics
   * @param note The statistics list to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatStatsV1(final List<IncomingDataPoint> stats) {
    return serializeCsv(stats);
  }
  
  /**
   * Format a list of thread statistics
   * @param stats The thread statistics list to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   * @since 2.2
   */
  public ChannelBuffer formatThreadStatsV1(final List<Map<String, Object>> stats) {
    return serializeCsv(stats);
  }
  
  /**
   * Format a list of JVM statistics
   * @param stats The JVM stats map to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   * @since 2.2
   */
  public ChannelBuffer formatJVMStatsV1(final Map<String, Map<String, Object>> stats) {
    return serializeCsv(stats);
  }
  
  /**
   * Format the query stats
   * @param query_stats Map of query statistics
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   * @since 2.2
   */
  public ChannelBuffer formatQueryStatsV1(
      final Map<String, List<Map<String, Object>>> query_stats) {
    return serializeCsv(query_stats);
  }
  
  /**
   * Format the response from a search query
   * @param note The query (hopefully filled with results) to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatSearchResultsV1(final SearchQuery results) {
    return serializeCsv(results);
  }
  
  /**
   * Format the running configuration
   * @param config The running config to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatConfigV1(final Config config) {
    TreeMap<String, String> map = new TreeMap<String, String>(config.getMap());
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (entry.getKey().toUpperCase().contains("PASS")) {
        map.put(entry.getKey(), "********");
      }
    }
    return serializeCsv(map);
  }
  
  /**
   * Helper object for the format calls to wrap the JSON response in a JSONP
   * function if requested. Used for code dedupe.
   * @param obj The object to serialize
   * @return A ChannelBuffer to pass on to the query
   * @throws JSONException if serialization failed
   */
  private ChannelBuffer serializeCsv(final Object obj) {
    return ChannelBuffers.wrappedBuffer(JSON.serializeToBytes(obj));
  }
}
