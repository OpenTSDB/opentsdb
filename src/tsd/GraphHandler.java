// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.Const;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.graph.Plot;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * Stateless handler of HTTP graph requests (the {@code /q} endpoint).
 */
final class GraphHandler implements HttpRpc {

  private static final Logger LOG =
    LoggerFactory.getLogger(GraphHandler.class);

  private static final boolean IS_WINDOWS = 
    System.getProperty("os.name").contains("Windows");
  
  /** Number of times we had to do all the work up to running Gnuplot. */
  private static final AtomicInteger graphs_generated
    = new AtomicInteger();
  /** Number of times a graph request was served from disk, no work needed. */
  private static final AtomicInteger graphs_diskcache_hit
    = new AtomicInteger();

  /** Keep track of the latency of graphing requests. */
  private static final Histogram graphlatency =
    new Histogram(16000, (short) 2, 100);

  /** Keep track of the latency (in ms) introduced by running Gnuplot. */
  private static final Histogram gnuplotlatency =
    new Histogram(16000, (short) 2, 100);

  /** Executor to run Gnuplot in separate bounded thread pool. */
  private final ThreadPoolExecutor gnuplot;

  /**
   * Constructor.
   */
  public GraphHandler() {
    // Gnuplot is mostly CPU bound and does only a little bit of IO at the
    // beginning to read the input data and at the end to write its output.
    // We want to avoid running too many Gnuplot instances concurrently as
    // it can steal a significant number of CPU cycles from us.  Instead, we
    // allow only one per core, and we nice it (the nicing is done in the
    // shell script we use to start Gnuplot).  Similarly, the queue we use
    // is sized so as to have a fixed backlog per core.
    final int ncores = Runtime.getRuntime().availableProcessors();
    gnuplot = new ThreadPoolExecutor(
      ncores, ncores,  // Thread pool of a fixed size.
      /* 5m = */ 300000, MILLISECONDS,        // How long to keep idle threads.
      new ArrayBlockingQueue<Runnable>(20 * ncores),  // XXX Don't hardcode?
      thread_factory);
    // ArrayBlockingQueue does not scale as much as LinkedBlockingQueue in terms
    // of throughput but we don't need high throughput here.  We use ABQ instead
    // of LBQ because it creates far fewer references.
  }

  public void execute(final TSDB tsdb, final HttpQuery query) {
    if (!query.hasQueryStringParam("json")
        && !query.hasQueryStringParam("png")
        && !query.hasQueryStringParam("ascii")) {
      String uri = query.request().getUri();
      if (uri.length() < 4) {  // Shouldn't happen...
        uri = "/";             // But just in case, redirect.
      } else {
        uri = "/#" + uri.substring(3);  // Remove "/q?"
      }
      query.redirect(uri);
      return;
    }
    try {
      doGraph(tsdb, query);
    } catch (IOException e) {
      query.internalError(e);
    } catch (IllegalArgumentException e) {
      query.badRequest(e.getMessage());
    }
  }

  private void doGraph(final TSDB tsdb, final HttpQuery query)
    throws IOException {
    final String basepath = getGnuplotBasePath(tsdb, query); 
    long start_time = DateTime.parseDateTimeString(
      query.getRequiredQueryStringParam("start"), 
      query.getQueryStringParam("tz"));
    final boolean nocache = query.hasQueryStringParam("nocache");
    if (start_time == -1) {
      throw BadRequestException.missingParameter("start");
    } else {
      // temp fixup to seconds from ms until the rest of TSDB supports ms
      // Note you can't append this to the DateTime.parseDateTimeString() call as
      // it clobbers -1 results
      start_time /= 1000;
    }
    long end_time = DateTime.parseDateTimeString(
        query.getQueryStringParam("end"), 
        query.getQueryStringParam("tz"));
    final long now = System.currentTimeMillis() / 1000;
    if (end_time == -1) {
      end_time = now;
    } else {
      // temp fixup to seconds from ms until the rest of TSDB supports ms
      // Note you can't append this to the DateTime.parseDateTimeString() call as
      // it clobbers -1 results
      end_time /= 1000;
    }
    final int max_age = computeMaxAge(query, start_time, end_time, now);
    if (!nocache && isDiskCacheHit(query, end_time, max_age, basepath)) {
      return;
    }
    Query[] tsdbqueries;
    List<String> options;
    tsdbqueries = parseQuery(tsdb, query);
    options = query.getQueryStringParams("o");
    if (options == null) {
      options = new ArrayList<String>(tsdbqueries.length);
      for (int i = 0; i < tsdbqueries.length; i++) {
        options.add("");
      }
    } else if (options.size() != tsdbqueries.length) {
      throw new BadRequestException(options.size() + " `o' parameters, but "
        + tsdbqueries.length + " `m' parameters.");
    }
    for (final Query tsdbquery : tsdbqueries) {
      try {
        tsdbquery.setStartTime(start_time);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("start time: " + e.getMessage());
      }
      try {
        tsdbquery.setEndTime(end_time);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("end time: " + e.getMessage());
      }
    }
    final Plot plot = new Plot(start_time, end_time,
          DateTime.timezones.get(query.getQueryStringParam("tz")));
    setPlotDimensions(query, plot);
    setPlotParams(query, plot);
    final int nqueries = tsdbqueries.length;
    @SuppressWarnings("unchecked")
    final HashSet<String>[] aggregated_tags = new HashSet[nqueries];
    int npoints = 0;
    for (int i = 0; i < nqueries; i++) {
      try {  // execute the TSDB query!
        // XXX This is slow and will block Netty.  TODO(tsuna): Don't block.
        // TODO(tsuna): Optimization: run each query in parallel.
        final DataPoints[] series = tsdbqueries[i].run();
        for (final DataPoints datapoints : series) {
          plot.add(datapoints, options.get(i));
          aggregated_tags[i] = new HashSet<String>();
          aggregated_tags[i].addAll(datapoints.getAggregatedTags());
          npoints += datapoints.aggregatedSize();
        }
      } catch (RuntimeException e) {
        logInfo(query, "Query failed (stack trace coming): "
                + tsdbqueries[i]);
        throw e;
      }
      tsdbqueries[i] = null;  // free()
    }
    tsdbqueries = null;  // free()

    if (query.hasQueryStringParam("ascii")) {
      respondAsciiQuery(query, max_age, basepath, plot);
      return;
    }

    try {
      gnuplot.execute(new RunGnuplot(query, max_age, plot, basepath,
                                     aggregated_tags, npoints));
    } catch (RejectedExecutionException e) {
      query.internalError(new Exception("Too many requests pending,"
                                        + " please try again later", e));
    }
  }

  /**
   * Decides how long we're going to allow the client to cache our response.
   * <p>
   * Based on the query, we'll decide whether or not we want to allow the
   * client to cache our response and for how long.
   * @param query The query to serve.
   * @param start_time The start time on the query (32-bit unsigned int, secs).
   * @param end_time The end time on the query (32-bit unsigned int, seconds).
   * @param now The current time (32-bit unsigned int, seconds).
   * @return A positive integer, in seconds.
   */
  private static int computeMaxAge(final HttpQuery query,
                                   final long start_time, final long end_time,
                                   final long now) {
    // If the end time is in the future (1), make the graph uncacheable.
    // Otherwise, if the end time is far enough in the past (2) such that
    // no TSD can still be writing to rows for that time span and it's not
    // specified in a relative fashion (3) (e.g. "1d-ago"), make the graph
    // cacheable for a day since it's very unlikely that any data will change
    // for this time span.
    // Otherwise (4), allow the client to cache the graph for ~0.1% of the
    // time span covered by the request e.g., for 1h of data, it's OK to
    // serve something 3s stale, for 1d of data, 84s stale.
    if (end_time > now) {                            // (1)
      return 0;
    } else if (end_time < now - Const.MAX_TIMESPAN   // (2)
               && !DateTime.isRelativeDate(
                   query.getQueryStringParam("start"))    // (3)
               && !DateTime.isRelativeDate(
                   query.getQueryStringParam("end"))) {
      return 86400;
    } else {                                         // (4)
      return (int) (end_time - start_time) >> 10;
    }
  }

  // Runs Gnuplot in a subprocess to generate the graph.
  private static final class RunGnuplot implements Runnable {

    private final HttpQuery query;
    private final int max_age;
    private final Plot plot;
    private final String basepath;
    private final HashSet<String>[] aggregated_tags;
    private final int npoints;

    public RunGnuplot(final HttpQuery query,
                      final int max_age,
                      final Plot plot,
                      final String basepath,
                      final HashSet<String>[] aggregated_tags,
                      final int npoints) {
      this.query = query;
      this.max_age = max_age;
      this.plot = plot;
      if (IS_WINDOWS)
        this.basepath = basepath.replace("\\", "\\\\").replace("/", "\\\\");
      else
        this.basepath = basepath;
      this.aggregated_tags = aggregated_tags;
      this.npoints = npoints;
    }

    public void run() {
      try {
        execute();
      } catch (BadRequestException e) {
        query.badRequest(e.getMessage());
      } catch (GnuplotException e) {
        query.badRequest("<pre>" + e.getMessage() + "</pre>");
      } catch (RuntimeException e) {
        query.internalError(e);
      } catch (IOException e) {
        query.internalError(e);
      }
    }

    private void execute() throws IOException {
      final int nplotted = runGnuplot(query, basepath, plot);
      if (query.hasQueryStringParam("json")) {
        final HashMap<String, Object> results = new HashMap<String, Object>();
        results.put("plotted", nplotted);
        results.put("points", npoints);
        // 1.0 returned an empty inner array if the 1st hashset was null, to do
        // the same we need to fudge it with an empty set
        if (aggregated_tags != null && aggregated_tags.length > 0 &&
            aggregated_tags[0] == null) {
          aggregated_tags[0] = new HashSet<String>();
        }
        results.put("etags", aggregated_tags);
        results.put("timing", query.processingTimeMillis());
        query.sendReply(JSON.serializeToBytes(results));
        writeFile(query, basepath + ".json", JSON.serializeToBytes(results));
      } else if (query.hasQueryStringParam("png")) {
        query.sendFile(basepath + ".png", max_age);
      } else {
        query.internalError(new Exception("Should never be here!"));
      }

      // TODO(tsuna): Expire old files from the on-disk cache.
      graphlatency.add(query.processingTimeMillis());
      graphs_generated.incrementAndGet();
    }

  }

  /** Shuts down the thread pool used to run Gnuplot.  */
  public void shutdown() {
    gnuplot.shutdown();
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("http.latency", graphlatency, "type=graph");
    collector.record("http.latency", gnuplotlatency, "type=gnuplot");
    collector.record("http.graph.requests", graphs_diskcache_hit, "cache=disk");
    collector.record("http.graph.requests", graphs_generated, "cache=miss");
  }

  /** Returns the base path to use for the Gnuplot files. */
  private String getGnuplotBasePath(final TSDB tsdb, final HttpQuery query) { 
    final Map<String, List<String>> q = query.getQueryString();
    q.remove("ignore");
    // Super cheap caching mechanism: hash the query string.
    final HashMap<String, List<String>> qs =
      new HashMap<String, List<String>>(q);
    // But first remove the parameters that don't influence the output.
    qs.remove("png");
    qs.remove("json");
    qs.remove("ascii");
    return tsdb.getConfig().getString("tsd.http.cachedir") + Integer.toHexString(qs.hashCode()); 
  }

  /**
   * Checks whether or not it's possible to re-serve this query from disk.
   * @param query The query to serve.
   * @param end_time The end time on the query (32-bit unsigned int, seconds).
   * @param max_age The maximum time (in seconds) we wanna allow clients to
   * cache the result in case of a cache hit.
   * @param basepath The base path used for the Gnuplot files.
   * @return {@code true} if this request was served from disk (in which
   * case processing can stop here), {@code false} otherwise (in which case
   * the query needs to be processed).
   */
  private boolean isDiskCacheHit(final HttpQuery query,
                                 final long end_time,
                                 final int max_age,
                                 final String basepath) throws IOException {
    final String cachepath = basepath + (query.hasQueryStringParam("ascii")
                                         ? ".txt" : ".png");
    final File cachedfile = new File(cachepath);
    if (cachedfile.exists()) {
      final long bytes = cachedfile.length();
      if (bytes < 21) {  // Minimum possible size for a PNG: 21 bytes.
                         // For .txt files, <21 bytes is almost impossible.
        logWarn(query, "Cached " + cachepath + " is too small ("
                + bytes + " bytes) to be valid.  Ignoring it.");
        return false;
      }
      if (staleCacheFile(query, end_time, max_age, cachedfile)) {
        return false;
      }
      if (query.hasQueryStringParam("json")) {
        HashMap<String, Object> map = loadCachedJson(query, end_time, 
            max_age, basepath);
        if (map == null) {
          map = new HashMap<String, Object>();
        }
        map.put("timing", query.processingTimeMillis());
        map.put("cachehit", "disk");
        query.sendReply(JSON.serializeToBytes(map));
      } else if (query.hasQueryStringParam("png")
                 || query.hasQueryStringParam("ascii")) {
        query.sendFile(cachepath, max_age);
      } else {
        query.sendReply(HttpQuery.makePage("TSDB Query", "Your graph is ready",
            "<img src=\"" + query.request().getUri() + "&amp;png\"/><br/>"
            + "<small>(served from disk cache)</small>"));
      }
      graphs_diskcache_hit.incrementAndGet();
      return true;
    }
    // We didn't find an image.  Do a negative cache check.  If we've seen
    // this query before but there was no result, we at least wrote the JSON.
    final HashMap<String, Object> map = loadCachedJson(query, end_time, 
        max_age, basepath);
    // If we don't have a JSON file it's a complete cache miss.  If we have
    // one, and it says 0 data points were plotted, it's a negative cache hit.
    if (map == null || !map.containsKey("plotted") || 
        ((Integer)map.get("plotted")) == 0) {
      return false;
    }
    if (query.hasQueryStringParam("json")) {
      map.put("timing", query.processingTimeMillis());
      map.put("cachehit", "disk");
      query.sendReply(JSON.serializeToBytes(map));
    } else if (query.hasQueryStringParam("png")) {
      query.sendReply(" ");  // Send back an empty response...
    } else {
        query.sendReply(HttpQuery.makePage("TSDB Query", "No results",
            "Sorry, your query didn't return anything.<br/>"
            + "<small>(served from disk cache)</small>"));
    }
    graphs_diskcache_hit.incrementAndGet();
    return true;
  }

  /**
   * Returns whether or not the given cache file can be used or is stale.
   * @param query The query to serve.
   * @param end_time The end time on the query (32-bit unsigned int, seconds).
   * @param max_age The maximum time (in seconds) we wanna allow clients to
   * cache the result in case of a cache hit.  If the file is exactly that
   * old, it is not considered stale.
   * @param cachedfile The file to check for staleness.
   */
  private static boolean staleCacheFile(final HttpQuery query,
                                        final long end_time,
                                        final long max_age,
                                        final File cachedfile) {
    final long mtime = cachedfile.lastModified() / 1000;
    if (mtime <= 0) {
      return true;  // File doesn't exist, or can't be read.
    }

    final long now = System.currentTimeMillis() / 1000;
    // How old is the cached file, in seconds?
    final long staleness = now - mtime;
    if (staleness < 0) {  // Can happen if the mtime is "in the future".
      logWarn(query, "Not using file @ " + cachedfile + " with weird"
              + " mtime in the future: " + mtime);
      return true;  // Play it safe, pretend we can't use this file.
    }

    // Case 1: The end time is an absolute point in the past.
    // We might be able to re-use the cached file.
    if (0 < end_time && end_time < now) {
      // If the file was created prior to the end time, maybe we first
      // executed this query while the result was uncacheable.  We can
      // tell by looking at the mtime on the file.  If the file was created
      // before the query end time, then it contains partial results that
      // shouldn't be served again.
      return mtime < end_time;
    }

    // Case 2: The end time of the query is now or in the future.
    // The cached file contains partial data and can only be re-used if it's
    // not too old.
    if (staleness > max_age) {
      logInfo(query, "Cached file @ " + cachedfile.getPath() + " is "
              + staleness + "s stale, which is more than its limit of "
              + max_age + "s, and needs to be regenerated.");
      return true;
    }
    return false;
  }

  /**
   * Writes the given byte array into a file.
   * This function logs an error but doesn't throw if it fails.
   * @param query The query being handled (for logging purposes).
   * @param path The path to write to.
   * @param contents The contents to write into the file.
   */
  private static void writeFile(final HttpQuery query,
                                final String path,
                                final byte[] contents) {
    try {
      final FileOutputStream out = new FileOutputStream(path);
      try {
        out.write(contents);
      } finally {
        out.close();
      }
    } catch (FileNotFoundException e) {
      logError(query, "Failed to create file " + path, e);
    } catch (IOException e) {
      logError(query, "Failed to write file " + path, e);
    }
  }

  /**
   * Reads a file into a byte array.
   * @param query The query being handled (for logging purposes).
   * @param file The file to read.
   * @param max_length The maximum number of bytes to read from the file.
   * @return {@code null} if the file doesn't exist or is empty or couldn't be
   * read, otherwise a byte array of up to {@code max_length} bytes.
   */
  private static byte[] readFile(final HttpQuery query,
                                 final File file,
                                 final int max_length) {
    final int length = (int) file.length();
    if (length <= 0) {
      return null;
    }
    FileInputStream in;
    try {
      in = new FileInputStream(file.getPath());
    } catch (FileNotFoundException e) {
      return null;
    }
    try {
      final byte[] buf = new byte[Math.min(length, max_length)];
      final int read = in.read(buf);
      if (read != buf.length) {
        logError(query, "When reading " + file + ": read only "
                 + read + " bytes instead of " + buf.length);
        return null;
      }
      return buf;
    } catch (IOException e) {
      logError(query, "Error while reading " + file, e);
      return null;
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        logError(query, "Error while closing " + file, e);
      }
    }
  }

  /**
   * Attempts to read the cached {@code .json} file for this query.
   * @param query The query to serve.
   * @param end_time The end time on the query (32-bit unsigned int, seconds).
   * @param max_age The maximum time (in seconds) we wanna allow clients to
   * cache the result in case of a cache hit.
   * @param basepath The base path used for the Gnuplot files.
   * @return {@code null} in case no file was found, or the contents of the
   * file if it was found.
   * @throws IOException If the file cannot be loaded
   * @throws JsonMappingException If the JSON cannot be parsed to a HashMap
   * @throws JsonParseException If the JSON is improperly formatted
   */
  @SuppressWarnings("unchecked")
  private HashMap<String, Object> loadCachedJson(final HttpQuery query,
                                       final long end_time,
                                       final long max_age,
                                       final String basepath) 
                                       throws JsonParseException, 
                                       JsonMappingException, IOException {
    final String json_path = basepath + ".json";
    File json_cache = new File(json_path);
    if (staleCacheFile(query, end_time, max_age, json_cache)) {
      return null;
    }
    final byte[] json = readFile(query, json_cache, 4096);
    if (json == null) {
      return null;
    }
    json_cache = null;
    
    return (HashMap<String, Object>) JSON.parseToObject(json, HashMap.class);
  }

  /** Parses the {@code wxh} query parameter to set the graph dimension. */
  static void setPlotDimensions(final HttpQuery query, final Plot plot) {
    final String wxh = query.getQueryStringParam("wxh");
    if (wxh != null && !wxh.isEmpty()) {
      final int wxhlength = wxh.length();
      if (wxhlength < 7) {  // 100x100 minimum.
        throw new BadRequestException("Parameter wxh too short: " + wxh);
      }
      final int x = wxh.indexOf('x', 3);  // Start at 2 as min size is 100x100
      if (x < 0) {
        throw new BadRequestException("Invalid wxh parameter: " + wxh);
      }
      try {
        final short width = Short.parseShort(wxh.substring(0, x));
        final short height = Short.parseShort(wxh.substring(x + 1, wxhlength));
        try {
          plot.setDimensions(width, height);
        } catch (IllegalArgumentException e) {
          throw new BadRequestException("Invalid wxh parameter: " + wxh + ", "
                                        + e.getMessage());
        }
      } catch (NumberFormatException e) {
        throw new BadRequestException("Can't parse wxh '" + wxh + "': "
                                      + e.getMessage());
      }
    }
  }

  /**
   * Formats and quotes the given string so it's a suitable Gnuplot string.
   * @param s The string to stringify.
   * @return A string suitable for use as a literal string in Gnuplot.
   */
  private static String stringify(final String s) {
    final StringBuilder buf = new StringBuilder(1 + s.length() + 1);
    buf.append('"');
    HttpQuery.escapeJson(s, buf);  // Abusing this function gets the job done.
    buf.append('"');
    return buf.toString();
  }

  /**
   * Pops out of the query string the given parameter.
   * @param querystring The query string.
   * @param param The name of the parameter to pop out.
   * @return {@code null} if the parameter wasn't passed, otherwise the
   * value of the last occurrence of the parameter.
   */
  private static String popParam(final Map<String, List<String>> querystring,
                                     final String param) {
    final List<String> params = querystring.remove(param);
    if (params == null) {
      return null;
    }
    return params.get(params.size() - 1);
  }

  /**
   * Applies the plot parameters from the query to the given plot.
   * @param query The query from which to get the query string.
   * @param plot The plot on which to apply the parameters.
   */
  static void setPlotParams(final HttpQuery query, final Plot plot) {
    final HashMap<String, String> params = new HashMap<String, String>();
    final Map<String, List<String>> querystring = query.getQueryString();
    String value;
    if ((value = popParam(querystring, "yrange")) != null) {
      params.put("yrange", value);
    }
    if ((value = popParam(querystring, "y2range")) != null) {
      params.put("y2range", value);
    }
    if ((value = popParam(querystring, "ylabel")) != null) {
      params.put("ylabel", stringify(value));
    }
    if ((value = popParam(querystring, "y2label")) != null) {
      params.put("y2label", stringify(value));
    }
    if ((value = popParam(querystring, "yformat")) != null) {
      params.put("format y", stringify(value));
    }
    if ((value = popParam(querystring, "y2format")) != null) {
      params.put("format y2", stringify(value));
    }
    if ((value = popParam(querystring, "xformat")) != null) {
      params.put("format x", stringify(value));
    }
    if ((value = popParam(querystring, "ylog")) != null) {
      params.put("logscale y", "");
    }
    if ((value = popParam(querystring, "y2log")) != null) {
      params.put("logscale y2", "");
    }
    if ((value = popParam(querystring, "key")) != null) {
      params.put("key", value);
    }
    if ((value = popParam(querystring, "title")) != null) {
      params.put("title", stringify(value));
    }
    if ((value = popParam(querystring, "bgcolor")) != null) {
      params.put("bgcolor", value);
    }
    if ((value = popParam(querystring, "fgcolor")) != null) {
      params.put("fgcolor", value);
    }
    if ((value = popParam(querystring, "smooth")) != null) {
      params.put("smooth", value);
    }
    // This must remain after the previous `if' in order to properly override
    // any previous `key' parameter if a `nokey' parameter is given.
    if ((value = popParam(querystring, "nokey")) != null) {
      params.put("key", null);
    }
    plot.setParams(params);
  }

  /**
   * Runs Gnuplot in a subprocess to generate the graph.
   * <strong>This function will block</strong> while Gnuplot is running.
   * @param query The query being handled (for logging purposes).
   * @param basepath The base path used for the Gnuplot files.
   * @param plot The plot object to generate Gnuplot's input files.
   * @return The number of points plotted by Gnuplot (0 or more).
   * @throws IOException if the Gnuplot files can't be written, or
   * the Gnuplot subprocess fails to start, or we can't read the
   * graph from the file it produces, or if we have been interrupted.
   * @throws GnuplotException if Gnuplot returns non-zero.
   */
  static int runGnuplot(final HttpQuery query,
                        final String basepath,
                        final Plot plot) throws IOException {
    final int nplotted = plot.dumpToFiles(basepath);
    final long start_time = System.nanoTime();
    final Process gnuplot = new ProcessBuilder(GNUPLOT,
      basepath + ".out", basepath + ".err", basepath + ".gnuplot").start();
    final int rv;
    try {
      rv = gnuplot.waitFor();  // Couldn't find how to do this asynchronously.
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();  // Restore the interrupted status.
      throw new IOException("interrupted", e);  // I hate checked exceptions.
    } finally {
      // We need to always destroy() the Process, otherwise we "leak" file
      // descriptors and pipes.  Unless I'm blind, this isn't actually
      // documented in the Javadoc of the !@#$%^ JDK, and in Java 6 there's no
      // way to ask the stupid-ass ProcessBuilder to not create fucking pipes.
      // I think when the GC kicks in the JVM may run some kind of a finalizer
      // that closes the pipes, because I've never seen this issue on long
      // running TSDs, except where ulimit -n was low (the default, 1024).
      gnuplot.destroy();
    }
    gnuplotlatency.add((int) ((System.nanoTime() - start_time) / 1000000));
    if (rv != 0) {
      final byte[] stderr = readFile(query, new File(basepath + ".err"),
                                     4096);
      // Sometimes Gnuplot will error out but still create the file.
      new File(basepath + ".png").delete();
      if (stderr == null) {
        throw new GnuplotException(rv);
      }
      throw new GnuplotException(new String(stderr));
    }
    // Remove the files for stderr/stdout if they're empty.
    deleteFileIfEmpty(basepath + ".out");
    deleteFileIfEmpty(basepath + ".err");
    return nplotted;
  }

  private static void deleteFileIfEmpty(final String path) {
    final File file = new File(path);
    if (file.length() <= 0) {
      file.delete();
    }
  }

  /**
   * Respond to a query that wants the output in ASCII.
   * <p>
   * When a query specifies the "ascii" query string parameter, we send the
   * data points back to the client in plain text instead of sending a PNG.
   * @param query The query we're currently serving.
   * @param max_age The maximum time (in seconds) we wanna allow clients to
   * cache the result in case of a cache hit.
   * @param basepath The base path used for the Gnuplot files.
   * @param plot The plot object to generate Gnuplot's input files.
   */
  private static void respondAsciiQuery(final HttpQuery query,
                                        final int max_age,
                                        final String basepath,
                                        final Plot plot) {
    final String path = basepath + ".txt";
    PrintWriter asciifile;
    try {
      asciifile = new PrintWriter(path);
    } catch (IOException e) {
      query.internalError(e);
      return;
    }
    try {
      final StringBuilder tagbuf = new StringBuilder();
      for (final DataPoints dp : plot.getDataPoints()) {
        final String metric = dp.metricName();
        tagbuf.setLength(0);
        for (final Map.Entry<String, String> tag : dp.getTags().entrySet()) {
          tagbuf.append(' ').append(tag.getKey())
            .append('=').append(tag.getValue());
        }
        for (final DataPoint d : dp) {
          asciifile.print(metric);
          asciifile.print(' ');
          asciifile.print((d.timestamp() / 1000));
          asciifile.print(' ');
          if (d.isInteger()) {
            asciifile.print(d.longValue());
          } else {
            final double value = d.doubleValue();
            if (value != value || Double.isInfinite(value)) {
              throw new IllegalStateException("NaN or Infinity:" + value
                + " d=" + d + ", query=" + query);
            }
            asciifile.print(value);
          }
          asciifile.print(tagbuf);
          asciifile.print('\n');
        }
      }
    } finally {
      asciifile.close();
    }
    try {
      query.sendFile(path, max_age);
    } catch (IOException e) {
      query.internalError(e);
    }
  }

  /**
   * Parses the {@code /q} query in a list of {@link Query} objects.
   * @param tsdb The TSDB to use.
   * @param query The HTTP query for {@code /q}.
   * @return The corresponding {@link Query} objects.
   * @throws BadRequestException if the query was malformed.
   * @throws IllegalArgumentException if the metric or tags were malformed.
   */
  private static Query[] parseQuery(final TSDB tsdb, final HttpQuery query) {
    final List<String> ms = query.getQueryStringParams("m");
    if (ms == null) {
      throw BadRequestException.missingParameter("m");
    }
    final Query[] tsdbqueries = new Query[ms.size()];
    int nqueries = 0;
    for (final String m : ms) {
      // m is of the following forms:
      //   agg:[interval-agg:][rate:]metric[{tag=value,...}]
      // Where the parts in square brackets `[' .. `]' are optional.
      final String[] parts = Tags.splitString(m, ':');
      int i = parts.length;
      if (i < 2 || i > 4) {
        throw new BadRequestException("Invalid parameter m=" + m + " ("
          + (i < 2 ? "not enough" : "too many") + " :-separated parts)");
      }
      final Aggregator agg = getAggregator(parts[0]);
      i--;  // Move to the last part (the metric name).
      final HashMap<String, String> parsedtags = new HashMap<String, String>();
      final String metric = Tags.parseWithMetric(parts[i], parsedtags);
      final boolean rate = "rate".equals(parts[--i]);
      if (rate) {
        i--;  // Move to the next part.
      }
      final Query tsdbquery = tsdb.newQuery();
      try {
        tsdbquery.setTimeSeries(metric, parsedtags, agg, rate);
      } catch (NoSuchUniqueName e) {
        throw new BadRequestException(e.getMessage());
      }
      // downsampling function & interval.
      if (i > 0) {
        final int dash = parts[1].indexOf('-', 1);  // 1st char can't be `-'.
        if (dash < 0) {
          throw new BadRequestException("Invalid downsampling specifier '"
                                        + parts[1] + "' in m=" + m);
        }
        Aggregator downsampler;
        try {
          downsampler = Aggregators.get(parts[1].substring(dash + 1));
        } catch (NoSuchElementException e) {
          throw new BadRequestException("No such downsampling function: "
                                        + parts[1].substring(dash + 1));
        }
        final int interval = (int) DateTime.parseDuration(parts[1].substring(0, dash));
        tsdbquery.downsample(interval, downsampler);
      } else {
        tsdbquery.downsample(1000, agg);
      }
      tsdbqueries[nqueries++] = tsdbquery;
    }
    return tsdbqueries;
  }

  /**
   * Returns the aggregator with the given name.
   * @param name Name of the aggregator to get.
   * @throws BadRequestException if there's no aggregator with this name.
   */
  private static final Aggregator getAggregator(final String name) {
    try {
      return Aggregators.get(name);
    } catch (NoSuchElementException e) {
      throw new BadRequestException("No such aggregation function: " + name);
    }
  }

  private static final PlotThdFactory thread_factory = new PlotThdFactory();

  private static final class PlotThdFactory implements ThreadFactory {
    private final AtomicInteger id = new AtomicInteger(0);

    public Thread newThread(final Runnable r) {
      return new Thread(r, "Gnuplot #" + id.incrementAndGet());
    }
  }

  /** Name of the wrapper script we use to execute Gnuplot.  */
  private static final String WRAPPER = 
    IS_WINDOWS ? "mygnuplot.bat" : "mygnuplot.sh";
  
  /** Path to the wrapper script.  */
  private static final String GNUPLOT;
  static {
    GNUPLOT = findGnuplotHelperScript();
  }

  /**
   * Iterate through the class path and look for the Gnuplot helper script.
   * @return The path to the wrapper script.
   */
  private static String findGnuplotHelperScript() {
    final URL url = GraphHandler.class.getClassLoader().getResource(WRAPPER);
    if (url == null) {
      throw new RuntimeException("Couldn't find " + WRAPPER + " on the"
        + " CLASSPATH: " + System.getProperty("java.class.path"));
    }
    final String path = url.getFile();
    LOG.debug("Using Gnuplot wrapper at {}", path);
    final File file = new File(path);
    final String error;
    if (!file.exists()) {
      error = "non-existent";
    } else if (!file.canExecute()) {
      error = "non-executable";
    } else if (!file.canRead()) {
      error = "unreadable";
    } else {
      return path;
    }
    throw new RuntimeException("The " + WRAPPER + " found on the"
      + " CLASSPATH (" + path + ") is a " + error + " file...  WTF?"
      + "  CLASSPATH=" + System.getProperty("java.class.path"));
  }

  // ---------------- //
  // Logging helpers. //
  // ---------------- //

  static void logInfo(final HttpQuery query, final String msg) {
    LOG.info(query.channel().toString() + ' ' + msg);
  }

  static void logWarn(final HttpQuery query, final String msg) {
    LOG.warn(query.channel().toString() + ' ' + msg);
  }

  static void logError(final HttpQuery query, final String msg) {
    LOG.error(query.channel().toString() + ' ' + msg);
  }

  static void logError(final HttpQuery query, final String msg,
                       final Throwable e) {
    LOG.error(query.channel().toString() + ' ' + msg, e);
  }

}
