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

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.regex.Pattern;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.HBaseConst;
import net.opentsdb.uid.IdUtils;
import net.opentsdb.utils.ByteArrayPair;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  /** The query with metrics and/or tags to use */
  private final ResolvedSearchQuery query;
  
  /** The TSD to use for lookups */
  private final TSDB tsdb;
  
  /**
   * Default ctor
   * @param tsdb The TSD to which we belong
   * @param metric A metric to match on, may be null
   * @param tags One or more tags to match on, may be null
   */
  public TimeSeriesLookup(final TSDB tsdb, final ResolvedSearchQuery query) {
    this.tsdb = tsdb;
    this.query = query;
  }

  /**
   * Lookup time series associated with the given metric, tagk, tagv or tag
   * pairs. If no metric is given, a full table scan must be performed and this
   * call may take a long time to complete.
   *
   * @return A list of TSUIDs matching the given lookup query.
   */
  public List<byte[]> lookup() {
    final StringBuilder tagv_filter = new StringBuilder();
    final Scanner scanner = getScanner(tagv_filter);
    final List<byte[]> tsuids = new ArrayList<byte[]>();
    final Pattern tagv_regex = tagv_filter.length() > 1 ? 
        Pattern.compile(tagv_filter.toString()) : null;
    // we don't really know what size the UIDs will resolve to so just grab
    // a decent amount.
    final long start = System.currentTimeMillis();
    
    ArrayList<ArrayList<KeyValue>> rows;
    
    try {
      // synchronous to avoid stack overflows when scanning across the main data
      // table.
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          final byte[] tsuid = row.get(0).key();
          
          // TODO - there MUST be a better way than creating a ton of temp
          // string objects.
          if (tagv_regex != null && 
              !tagv_regex.matcher(new String(tsuid, HBaseConst.CHARSET)).find()) {
            continue;
          }

          tsuids.add(tsuid);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Shouldn't be here", e);
    } finally {
      scanner.close();
    }

    LOG.debug("Lookup query matched {} time series in {} ms", tsuids.size(), System.currentTimeMillis() - start);
    return tsuids;
  }

  /**
   * Configures the scanner for iterating over the meta table. If the metric has
   * been set, then we scan a small slice of the table where the metric lies,
   * otherwise we have to scan the whole table. If tags are given then we setup
   * a row key regex
   *
   * @return A configured scanner
   */
  private Scanner getScanner(final StringBuilder tagv_filter) {
    final Scanner scanner = tsdb.getHBaseStore().newScanner(tsdb.metaTable());

    // if a metric id is given, we need to set the start key to the UID and the
    // stop key to the next row by incrementing the UID.
    if (query.getMetric() != null) {
      scanner.setStartKey(query.getMetric());

      // TODO - see what happens when this rolls over
      final long stopKey = IdUtils.uidToLong(query.getMetric(), Const.METRICS_WIDTH) + 1;
      scanner.setStopKey(IdUtils.longToUID(stopKey, Const.METRICS_WIDTH));
    } else {
      LOG.debug("Performing full table scan, no metric provided");
    }
    
    if (!query.getTags().isEmpty()) {
      // remember, tagks are sorted in the row key so we need to supply a sorted
      // regex or matching will fail.
      final SortedSet<ByteArrayPair> tags = query.getTags();

      final short name_width = Const.TAG_NAME_WIDTH;
      final short value_width = Const.TAG_VALUE_WIDTH;
      final short tagsize = (short) (name_width + value_width);

      boolean isFirstTag = true;

      final StringBuilder buf = new StringBuilder(
          22  // "^.{N}" + "(?:.{M})*" + "$" + wiggle
          + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
             * (tags.size())));
      buf.append("(?s)^.{").append(Const.METRICS_WIDTH)
        .append("}");
      buf.append("(?:.{").append(tagsize).append("})*");

      final PeekingIterator<ByteArrayPair> tagIterator = Iterators.peekingIterator(tags.iterator());

      // at the top of the list will be the null=tagv pairs. We want to compile
      // a separate regex for them.
      while (tagIterator.hasNext() && tagIterator.peek().getKey() == null) {
        if (!isFirstTag) {
          buf.append("|");
        }
        buf.append("(?:.{").append(name_width).append("})");
        buf.append("\\Q");
        addId(buf, tagIterator.next().getValue());
        buf.append("\\E");

        isFirstTag = false;
      }
      buf.append("(?:.{").append(tagsize).append("})*")
         .append("$");
      
      if (!isFirstTag && tagIterator.hasNext()) {
        // we had one or more tagvs to lookup AND we have tagk or tag pairs to
        // filter on, so we dump the previous regex into the tagv_filter and
        // continue on with a row key
        tagv_filter.append(buf);
        LOG.debug("Setting tagv filter: {}", buf);
      } else if (!tagIterator.hasNext()) {
        // in this case we don't have any tagks to deal with so we can just
        // pass the previously compiled regex to the rowkey filter of the 
        // scanner
        scanner.setKeyRegexp(buf.toString(), HBaseConst.CHARSET);
        LOG.debug("Setting scanner row key filter with tagvs only: {}", buf);
      }
      
      // catch any left over tagk/tag pairs
      if (tagIterator.hasNext()) {
        buf.setLength(0);
        buf.append("(?s)^.{").append(Const.METRICS_WIDTH)
           .append("}");
        
        ByteArrayPair last_pair = null;
        while (tagIterator.hasNext()) {
          final ByteArrayPair tag = tagIterator.next();

          if (last_pair != null && last_pair.getValue() == null &&
              Bytes.memcmp(last_pair.getKey(), tag.getKey()) == 0) {
            // tagk=null is a wildcard so we don't need to bother adding 
            // tagk=tagv pairs with the same tagk.
            LOG.debug("Skipping pair due to wildcard: {}", tag);
          } else if (last_pair != null && 
              Bytes.memcmp(last_pair.getKey(), tag.getKey()) == 0) {
            // in this case we're ORing e.g. "host=web01|host=web02"
            buf.append("|\\Q");
            addId(buf, tag.getKey());
            addId(buf, tag.getValue());
            buf.append("\\E");
          } else {
            if (last_pair != null) {
              buf.append(")");
            }
            // moving on to the next tagk set
            buf.append("(?:.{6})*"); // catch tag pairs in between
            buf.append("(?:");
            if (tag.getKey() != null &&
                tag.getValue() != null) {
              buf.append("\\Q");
              addId(buf, tag.getKey());
              addId(buf, tag.getValue());
              buf.append("\\E");
            } else {
              buf.append("\\Q");
              addId(buf, tag.getKey());
              buf.append("\\E");
              buf.append("(?:.{").append(value_width).append("})+");
            }
          }
          last_pair = tag;
        }
        buf.append(")(?:.{").append(tagsize).append("})*").append("$");
        
        scanner.setKeyRegexp(buf.toString(), HBaseConst.CHARSET);
        LOG.debug("Setting scanner row key filter: {}", buf);
      }
    }

    return scanner;
  }
  
  /**
   * Appends the given ID to the given buffer, escaping where appropriate
   * @param buf The string buffer to append to
   * @param id The ID to append
   */
  private static void addId(final StringBuilder buf, final byte[] id) {
    boolean backslash = false;
    for (final byte b : id) {
      buf.append((char) (b & 0xFF));
      if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
        // So we just terminated the quoted section because we just added \E
        // to `buf'.  So let's put a litteral \E now and start quoting again.
        buf.append("\\\\E\\Q");
      } else {
        backslash = b == '\\';
      }
    }
  }
}
