// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import net.opentsdb.core.Const;
import net.opentsdb.core.Internal;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.query.QueryUtil;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.ByteArrayPair;
import net.opentsdb.utils.Exceptions;
import net.opentsdb.utils.Pair;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

/**
 * Lookup series related to a metric, tagk, tagv or any combination thereof.
 * This class doesn't handle wild-card searching yet.
 * 
 * When dealing with tags, we can lookup on tagks, tagvs or pairs. Thus:
 * tagk, null  <- lookup all series with a tagk
 * tagk, tagv  <- lookup all series with a tag pair
 * null, tagv  <- lookup all series with a tag value somewhere
 * 
 * The user can supply multiple tags in a query so the logic is a little goofy
 * but here it is:
 * - Different tagks are AND'd, e.g. given "host=web01 dc=lga" we will lookup
 *   series that contain both of those tag pairs. Also when given "host= dc="
 *   then we lookup series with both tag keys regardless of their values.
 * - Tagks without a tagv will override tag pairs. E.g. "host=web01 host=" will
 *   return all series with the "host" tagk.
 * - Tagvs without a tagk are OR'd. Given "=lga =phx" the lookup will fetch 
 *   anything with either "lga" or "phx" as the value for a pair. When combined
 *   with a tagk, e.g. "host=web01 =lga" then it will return any series with the
 *   tag pair AND any tag with the "lga" value.
 *  
 * To avoid running performance degrading regexes in HBase regions, we'll double
 * filter when necessary. If tagks are present, those are used in the rowkey 
 * filter and a secondary filter is applied in the TSD with remaining tagvs.
 * E.g. the query "host=web01 =lga" will issue a rowkey filter with "host=web01"
 * then within the TSD scanner, we'll filter out only the rows that contain an
 * "lga" tag value. We don't know where in a row key the tagv may fall, so we
 * would have to first match on the pair, then backtrack to find the value and 
 * make sure the pair is skipped. Thus its easier on the region server to execute
 * a simpler rowkey regex, pass all the results to the TSD, then let us filter on
 * tag values only when necessary. (if a query only has tag values, then this is
 * moot and we can pass them in a rowkey filter since they're OR'd).
 * 
 * @since 2.1
 */
public class TimeSeriesLookup {
  private static final Logger LOG = 
      LoggerFactory.getLogger(TimeSeriesLookup.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  
  /** The query with metrics and/or tags to use */
  private final SearchQuery query;
  
  /** Whether or not to dump the output to standard out for CLI commands */
  private boolean to_stdout;
  
  /** The TSD to use for lookups */
  private final TSDB tsdb;
  
  /** The metric UID if given by the query, post resolution */
  private byte[] metric_uid;
  
  /** Tag UID pairs if given in the query. Key or value may be null. */
  private List<ByteArrayPair> pairs;
  
  /** The compiled row key regex for HBase filtering */
  private String rowkey_regex;
  
  /** Post scan filtering if we have a lot of values to look at */
  private String tagv_filter;
  
  /** The results to send to the caller */
  private final List<byte[]> tsuids;
  
  /**
   * Default ctor
   * @param tsdb The TSD to which we belong
   * @param metric A metric to match on, may be null
   * @param tags One or more tags to match on, may be null
   */
  public TimeSeriesLookup(final TSDB tsdb, final SearchQuery query) {
    this.tsdb = tsdb;
    this.query = query;
    tsuids = Collections.synchronizedList(new ArrayList<byte[]>());
  }
  
  /**
   * Lookup time series associated with the given metric, tagk, tagv or tag 
   * pairs. Either the meta table or the data table will be scanned. If no
   * metric is given, a full table scan must be performed and this call may take
   * a long time to complete. 
   * When dumping to stdout, if an ID can't be looked up, it will be logged and
   * skipped.
   * @return A list of TSUIDs matching the given lookup query.
   * @throws NoSuchUniqueName if any of the given names fail to resolve to a 
   * UID.
   */
  public List<byte[]> lookup() {
    try {
      return lookupAsync().join();
    } catch (InterruptedException e) {
      LOG.error("Interrupted performing lookup", e);
      Thread.currentThread().interrupt();
      return null;
    } catch (DeferredGroupException e) {
      final Throwable ex = Exceptions.getCause(e);
      if (ex instanceof NoSuchUniqueName) {
        throw (NoSuchUniqueName)ex;
      }
      throw new RuntimeException("Unexpected exception", ex);
    } catch (NoSuchUniqueName e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Unexpected exception", e);
    }
  }
  
  /**
   * Lookup time series associated with the given metric, tagk, tagv or tag 
   * pairs. Either the meta table or the data table will be scanned. If no
   * metric is given, a full table scan must be performed and this call may take
   * a long time to complete. 
   * When dumping to stdout, if an ID can't be looked up, it will be logged and
   * skipped.
   * @return A list of TSUIDs matching the given lookup query.
   * @throws NoSuchUniqueName if any of the given names fail to resolve to a 
   * UID.
   * @since 2.2
   */
  public Deferred<List<byte[]>> lookupAsync() {
    final Pattern tagv_regex = tagv_filter != null ? 
        Pattern.compile(tagv_filter) : null;
    
    // we don't really know what size the UIDs will resolve to so just grab
    // a decent amount.
    final StringBuffer buf = to_stdout ? new StringBuffer(2048) : null;
    final long start = System.currentTimeMillis();
    final int limit;
    if (query.getLimit() > 0) {
      if (query.useMeta() || Const.SALT_WIDTH() < 1) {
        limit = query.getLimit();
      } else if (query.getLimit() < Const.SALT_BUCKETS()) {
        limit = 1;
      } else {
        limit = query.getLimit() / Const.SALT_BUCKETS();
      }
    } else {
      limit = 0;
    }
        
    class ScannerCB implements Callback<List<byte[]>, ArrayList<ArrayList<KeyValue>>> {
      private final Scanner scanner;
      // used to avoid dupes when scanning the data table
      private byte[] last_tsuid = null;
      private int rows_read;
      
      ScannerCB(final Scanner scanner) {
        this.scanner = scanner;
      }
      
      Deferred<List<byte[]>> scan() {
        return scanner.nextRows().addCallback(this);
      }
      
      @Override
      public List<byte[]> call(final ArrayList<ArrayList<KeyValue>> rows)
          throws Exception {
        if (rows == null) {
          scanner.close();
          if (query.useMeta() || Const.SALT_WIDTH() < 1) {
            LOG.debug("Lookup query matched " + tsuids.size() + " time series in " +
                (System.currentTimeMillis() - start) + " ms");
          }
          return tsuids;
        }
        
        for (final ArrayList<KeyValue> row : rows) {
          if (limit > 0 && rows_read >= limit) {
            // little recursion to close the scanner and log above.
            return call(null);
          }
          final byte[] tsuid = query.useMeta() ? row.get(0).key() : 
            UniqueId.getTSUIDFromKey(row.get(0).key(), TSDB.metrics_width(), 
                Const.TIMESTAMP_BYTES);
          
          // TODO - there MUST be a better way than creating a ton of temp
          // string objects.
          if (tagv_regex != null && 
              !tagv_regex.matcher(new String(tsuid, CHARSET)).find()) {
            continue;
          }
          
          if (to_stdout) {
            if (last_tsuid != null && Bytes.memcmp(last_tsuid, tsuid) == 0) {
              continue;
            }
            last_tsuid = tsuid;
            
            try {
              buf.append(UniqueId.uidToString(tsuid)).append(" ");
              buf.append(RowKey.metricNameAsync(tsdb, tsuid)
                  .joinUninterruptibly());
              buf.append(" ");
              
              final List<byte[]> tag_ids = UniqueId.getTagPairsFromTSUID(tsuid);
              final Map<String, String> resolved_tags = 
                  Tags.resolveIdsAsync(tsdb, tag_ids).joinUninterruptibly();
              for (final Map.Entry<String, String> tag_pair : 
                  resolved_tags.entrySet()) {
                buf.append(tag_pair.getKey()).append("=")
                   .append(tag_pair.getValue()).append(" ");
              }
            } catch (NoSuchUniqueId nsui) {
              LOG.error("Unable to resolve UID in TSUID (" + 
                  UniqueId.uidToString(tsuid) + ") " + nsui.getMessage());
            }
            buf.setLength(0); // reset the buffer so we can re-use it
          } else {
            tsuids.add(tsuid);
          }
          ++rows_read;
        }
        
        scan();
        return tsuids;
      }
      
      @Override
      public String toString() {
        return "Scanner callback";
      }
    }
    
    class CompleteCB implements Callback<List<byte[]>, ArrayList<List<byte[]>>> {
      @Override
      public List<byte[]> call(final ArrayList<List<byte[]>> unused) throws Exception {
        LOG.debug("Lookup query matched " + tsuids.size() + " time series in " +
            (System.currentTimeMillis() - start) + " ms");
        return tsuids;
      }
      @Override
      public String toString() {
        return "Final async lookup callback";
      }
    }
    
    class UIDCB implements Callback<Deferred<List<byte[]>>, Object> {
      @Override
      public Deferred<List<byte[]>> call(Object arg0) throws Exception {
        if (!query.useMeta() && Const.SALT_WIDTH() > 0 && metric_uid != null) {
          final ArrayList<Deferred<List<byte[]>>> deferreds = 
              new ArrayList<Deferred<List<byte[]>>>(Const.SALT_BUCKETS());
          for (int i = 0; i < Const.SALT_BUCKETS(); i++) {
            deferreds.add(new ScannerCB(getScanner(i)).scan());
          }
          return Deferred.group(deferreds).addCallback(new CompleteCB());
        } else {
          return new ScannerCB(getScanner(0)).scan();
        }
      }
      @Override
      public String toString() {
        return "UID resolution callback";
      }
    }
    
    return resolveUIDs().addCallbackDeferring(new UIDCB());
  }
  
  /**
   * Resolves the metric and tag strings to their UIDs
   * @return A deferred to wait on for resolution to complete.
   */
  private Deferred<Object> resolveUIDs() {
    
    class TagsCB implements Callback<Object, ArrayList<Object>> {
      @Override
      public Object call(final ArrayList<Object> ignored) throws Exception {
        rowkey_regex = getRowKeyRegex();
        return null;
      }
    }
    
    class PairResolution implements Callback<Object, ArrayList<byte[]>> {
      @Override
      public Object call(final ArrayList<byte[]> tags) throws Exception {
        if (tags.size() < 2) {
          throw new IllegalArgumentException("Somehow we received an array "
              + "that wasn't two bytes in size! " + tags);
        }
        pairs.add(new ByteArrayPair(tags.get(0), tags.get(1)));
        return Deferred.fromResult(null);
      }
    }
    
    class TagResolution implements Callback<Deferred<Object>, Object> {
      @Override
      public Deferred<Object> call(final Object unused) throws Exception {
        if (query.getTags() == null || query.getTags().isEmpty()) {
          return Deferred.fromResult(null);
        }
        
        pairs = Collections.synchronizedList(
            new ArrayList<ByteArrayPair>(query.getTags().size()));
        final ArrayList<Deferred<Object>> deferreds = 
            new ArrayList<Deferred<Object>>(pairs.size());
        
        for (final Pair<String, String> tags : query.getTags()) {
          final ArrayList<Deferred<byte[]>> deferred_tags = 
              new ArrayList<Deferred<byte[]>>(2);
          if (tags.getKey() != null && !tags.getKey().equals("*")) {
            deferred_tags.add(tsdb.getUIDAsync(UniqueIdType.TAGK, tags.getKey()));
          } else {
            deferred_tags.add(Deferred.<byte[]>fromResult(null));
          }
          if (tags.getValue() != null && !tags.getValue().equals("*")) {
            deferred_tags.add(tsdb.getUIDAsync(UniqueIdType.TAGV, tags.getValue()));
          } else {
            deferred_tags.add(Deferred.<byte[]>fromResult(null));
          }
          deferreds.add(Deferred.groupInOrder(deferred_tags)
              .addCallback(new PairResolution()));
        }
        return Deferred.group(deferreds).addCallback(new TagsCB());
      }
    }
    
    class MetricCB implements Callback<Deferred<Object>, byte[]> {
      @Override
      public Deferred<Object> call(final byte[] uid) throws Exception {
        metric_uid = uid;
        LOG.debug("Found UID (" + UniqueId.uidToString(metric_uid) + 
            ") for metric (" + query.getMetric() + ")");
        return new TagResolution().call(null);
      }
    }
    
    if (query.getMetric() != null && !query.getMetric().isEmpty() && 
        !query.getMetric().equals("*")) {
      return tsdb.getUIDAsync(UniqueIdType.METRIC, query.getMetric())
          .addCallbackDeferring(new MetricCB());
    } else {
      try {
        return new TagResolution().call(null);
      } catch (Exception e) {
        return Deferred.fromError(e);
      } 
    }
  }
  
  /** Compiles a scanner with the given salt ID if salting is enabled AND we're
   * not scanning the meta table.
   * @param salt An ID for the salt bucket
   * @return A scanner to send to HBase.
   */
  private Scanner getScanner(final int salt) {
    final Scanner scanner = tsdb.getClient().newScanner(
        query.useMeta() ? tsdb.metaTable() : tsdb.dataTable());
    scanner.setFamily(query.useMeta() ? TSMeta.FAMILY : TSDB.FAMILY());
    
    if (metric_uid != null) {
      byte[] key;
      if (query.useMeta() || Const.SALT_WIDTH() < 1) {
        key = metric_uid;
      } else {
        key = new byte[Const.SALT_WIDTH() + TSDB.metrics_width()];
        key[0] = (byte)salt;
        System.arraycopy(metric_uid, 0, key, Const.SALT_WIDTH(), metric_uid.length);
      }
      scanner.setStartKey(key);
      long uid = UniqueId.uidToLong(metric_uid, TSDB.metrics_width());
      uid++;
      if (uid < Internal.getMaxUnsignedValueOnBytes(TSDB.metrics_width())) {
        // if random metrics are enabled we could see a metric with the max UID
        // value. If so, we need to leave the stop key as null
        if (query.useMeta() || Const.SALT_WIDTH() < 1) {
          key = UniqueId.longToUID(uid, TSDB.metrics_width());
        } else {
          key = new byte[Const.SALT_WIDTH() + TSDB.metrics_width()];
          key[0] = (byte)salt;
          System.arraycopy(UniqueId.longToUID(uid, TSDB.metrics_width()), 0, 
              key, Const.SALT_WIDTH(), metric_uid.length);
        }
        scanner.setStopKey(key);  
      }
    }
    
    if (rowkey_regex != null) {
      scanner.setKeyRegexp(rowkey_regex, CHARSET);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Scanner regex: " + QueryUtil.byteRegexToString(rowkey_regex));
      }
    }
    
    return scanner;
  }
  
  /**
   * Constructs a row key regular expression to pass to HBase if the user gave
   * some tags in the query
   * @return The regular expression to use.
   */
  private String getRowKeyRegex() {
    final StringBuilder tagv_buffer = new StringBuilder();
    // remember, tagks are sorted in the row key so we need to supply a sorted
    // regex or matching will fail.
    Collections.sort(pairs);
    
    final short name_width = TSDB.tagk_width();
    final short value_width = TSDB.tagv_width();
    final short tagsize = (short) (name_width + value_width);
    
    int index = 0;
    final StringBuilder buf = new StringBuilder(
        22  // "^.{N}" + "(?:.{M})*" + "$" + wiggle
        + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
           * (pairs.size())));
    buf.append("(?s)^.{").append(query.useMeta() ? TSDB.metrics_width() : 
      TSDB.metrics_width() + Const.SALT_WIDTH())
      .append("}");
    if (!query.useMeta()) {
      buf.append("(?:.{").append(Const.TIMESTAMP_BYTES).append("})*");
    }
    buf.append("(?:.{").append(tagsize).append("})*");
    
    // at the top of the list will be the null=tagv pairs. We want to compile
    // a separate regex for them.
    for (; index < pairs.size(); index++) {
      if (pairs.get(index).getKey() != null) {
        break;
      }
      
      if (index > 0) {
        buf.append("|");
      }
      buf.append("(?:.{").append(name_width).append("})");
      buf.append("\\Q");
      QueryUtil.addId(buf, pairs.get(index).getValue(), true);
    }
    buf.append("(?:.{").append(tagsize).append("})*")
       .append("$");
    
    if (index > 0 && index < pairs.size()) {
      // we had one or more tagvs to lookup AND we have tagk or tag pairs to
      // filter on, so we dump the previous regex into the tagv_filter and
      // continue on with a row key
      tagv_buffer.append(buf.toString());
      LOG.debug("Setting tagv filter: " + QueryUtil.byteRegexToString(buf.toString()));
    } else if (index >= pairs.size()) {
      // in this case we don't have any tagks to deal with so we can just
      // pass the previously compiled regex to the rowkey filter of the 
      // scanner
      LOG.debug("Setting scanner row key filter with tagvs only: " + 
          QueryUtil.byteRegexToString(buf.toString()));
      if (tagv_buffer.length() > 0) {
        tagv_filter = tagv_buffer.toString();
      }
      return buf.toString();
    }
    
    // catch any left over tagk/tag pairs
    if (index < pairs.size()){
      buf.setLength(0);
      buf.append("(?s)^.{").append(query.useMeta() ? TSDB.metrics_width() : 
        TSDB.metrics_width() + Const.SALT_WIDTH())
         .append("}");
      if (!query.useMeta()) {
        buf.append("(?:.{").append(Const.TIMESTAMP_BYTES).append("})*");
      }
      
      ByteArrayPair last_pair = null;
      for (; index < pairs.size(); index++) {
        if (last_pair != null && last_pair.getValue() == null &&
            Bytes.memcmp(last_pair.getKey(), pairs.get(index).getKey()) == 0) {
          // tagk=null is a wildcard so we don't need to bother adding 
          // tagk=tagv pairs with the same tagk.
          LOG.debug("Skipping pair due to wildcard: " + pairs.get(index));
        } else if (last_pair != null && 
            Bytes.memcmp(last_pair.getKey(), pairs.get(index).getKey()) == 0) {
          // in this case we're ORing e.g. "host=web01|host=web02"
          buf.append("|\\Q");
          QueryUtil.addId(buf, pairs.get(index).getKey(), false);
          QueryUtil.addId(buf, pairs.get(index).getValue(), true);
        } else {
          if (last_pair != null) {
            buf.append(")");
          }
          // moving on to the next tagk set
          buf.append("(?:.{6})*"); // catch tag pairs in between
          buf.append("(?:");
          if (pairs.get(index).getKey() != null && 
              pairs.get(index).getValue() != null) {
            buf.append("\\Q");
            QueryUtil.addId(buf, pairs.get(index).getKey(), false);
            QueryUtil.addId(buf, pairs.get(index).getValue(), true);
          } else {
            buf.append("\\Q");
            QueryUtil.addId(buf, pairs.get(index).getKey(), true);
            buf.append("(?:.{").append(value_width).append("})+");
          }
        }
        last_pair = pairs.get(index);
      }
      buf.append(")(?:.{").append(tagsize).append("})*").append("$");
    }
    if (tagv_buffer.length() > 0) {
      tagv_filter = tagv_buffer.toString();
    }
    return buf.toString();
  }

  /** @param to_stdout Whether or not to dump to standard out as we scan */
  public void setToStdout(final boolean to_stdout) {
    this.to_stdout = to_stdout;
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("query={")
       .append(query)
       .append("}, to_stdout=")
       .append(to_stdout)
       .append(", metric_uid=")
       .append(metric_uid == null ? "null" : Arrays.toString(metric_uid))
       .append(", pairs=")
       .append(pairs)
       .append(", rowkey_regex=")
       .append(rowkey_regex)
       .append(", tagv_filter=")
       .append(tagv_filter);
    return buf.toString();
       
  }
}
