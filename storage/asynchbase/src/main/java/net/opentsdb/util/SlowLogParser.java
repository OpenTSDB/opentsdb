/*
 * // This file is part of OpenTSDB.
 * // Copyright (C) 2021  The OpenTSDB Authors.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //   http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */

package net.opentsdb.util;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.SchemaFactory;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This is the start of a little utility to parse a slow region log, for now just
 * looking at multi-action puts, to figure out what metrics and tags are appearing
 * the most in order to go after those that may be writing too much data.
 * </p>
 * Right now it's pretty simplistic and may only work on logs from HBase 1.3.
 * Haven't tried later versions. Also it expects the table name to NOT start
 * with a 'p'.
 *
 * TODO - make this more flexible.
 *
 * @since 3.0
 */
public class SlowLogParser {
  private static Logger LOG = LoggerFactory.getLogger(SlowLogParser.class);

  static Pattern HASH = Pattern.compile("^[a-f0-9]+$");

  private final TSDB tsdb;
  private Schema schema;
  Map<String, KeyTracker> results;

  class KeyTracker {
    String metric;
    int hits;
    List<Integer> baseTimestamp = Lists.newArrayList();
    Multimap<String, String> tags = MultimapBuilder
            .treeKeys()
            .hashSetValues()
            .build();
  }

  SlowLogParser(final TSDB tsdb) {
    this.tsdb = tsdb;

    TimeSeriesDataSourceFactory factory = tsdb.getRegistry().getDefaultPlugin(TimeSeriesDataSourceFactory.class);
    if (factory == null) {
      LOG.error("No default TimeSeriesDataSourceFactory.");
      try {
        tsdb.shutdown().join();
      } catch (Exception e) {
        e.printStackTrace();
      }
      System.exit(1);
    }
    if (!(factory instanceof SchemaFactory)) {
      LOG.error("Not a Schema factory!");
    }
    schema = (Schema) ((SchemaFactory) factory).consumer();
    results = Maps.newHashMap();
  }

  void run(String path) throws Exception {
    try {
      if (Strings.isNullOrEmpty(path)) {
        LOG.error("No path.");
        tsdb.shutdown().join();
        System.exit(1);
      }

      String paths[];
      if (path.contains(",")) {
        paths = path.split(",");
      } else {
        paths = new String[] { path };
      }

      byte[] readBuf = new byte[1024];
      for (int x = 0; x < paths.length; x++) {
        InputStream inputStream = new FileInputStream(paths[x].trim());
        byte[] lineBuf = new byte[4096];
        int lineEnd = 0;
        int read = 0;
        while ((read = inputStream.read(readBuf)) >= 0) {
          for (int i = 0; i < readBuf.length; i++) {
            if (readBuf[i] == '\n') {
              parseLine(lineBuf, lineEnd);
              // reset
              lineEnd = 0;
            } else {
              if (lineEnd + 1 == lineBuf.length) {
                lineBuf = Arrays.copyOf(lineBuf, lineBuf.length + 1024);
              }
              lineBuf[lineEnd++] = readBuf[i];
            }
          }
        }
      }

      List<KeyTracker> trackers = Lists.newArrayList(results.values());
      class Sorter implements Comparator<KeyTracker> {

        @Override
        public int compare(KeyTracker o1, KeyTracker o2) {
          if (o1.hits == o2.hits) {
            return 0;
          }
          return o1.hits > o2.hits ? -1 : 1;
        }
      }
      trackers.sort(new Sorter());
      for (KeyTracker tracker : trackers) {
        StringBuilder buf = new StringBuilder()
                .append("metric=")
                .append(tracker.metric)
                .append(", hits=")
                .append(tracker.hits)
                .append(", timestamps=")
                .append(tracker.baseTimestamp)
                .append(", tags=")
                .append(tracker.tags);
        LOG.info(buf.toString());
      }

      System.out.println("FINISHED!");
      tsdb.shutdown().join();
      System.exit(0);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    ((DefaultTSDB) tsdb).shutdown().join();
  }

  void parseLine(byte[] lineBuf, int len) {
    try {
      int idx = 0;
      // lines start with timestamp e.g.: '2021-06-13 13:07:04,484 :'
      idx += 26;

      // skip whatever this is, says 'SV' for us.
      idx = nextData(lineBuf, idx, len);
      if (idx < 0) {
        return;
      }

      // skip user
      idx = nextData(lineBuf, idx, len);
      if (idx < 0) {
        return;
      }

      // ? mark?
      idx = nextData(lineBuf, idx, len);
      if (idx < 0) {
        return;
      }

      // T? What's that?
      idx = nextData(lineBuf, idx, len);
      if (idx < 0) {
        return;
      }

      // IP, don't need it yet
      idx = nextData(lineBuf, idx, len);
      if (idx < 0) {
        return;
      }

      // Time or size 1
      idx = nextData(lineBuf, idx, len);
      if (idx < 0) {
        return;
      }

      // time or size 2
      idx = nextData(lineBuf, idx, len);
      if (idx < 0) {
        return;
      }

      // time or size 3
      idx = nextData(lineBuf, idx, len);
      if (idx < 0) {
        return;
      }

      // op
      int start = idx;
      idx = nextData(lineBuf, idx, len);
      String op = new String(lineBuf, start, idx - start - 1);
      if (op.equalsIgnoreCase("scan")) {
        processScan(lineBuf, idx, len);
      } else if (op.equalsIgnoreCase("multi")) {
        processMulti(lineBuf, idx, len);
      } else {
        System.out.println("Unhandled op: " + op);
      }
    } catch (Exception e) {
      throw new RuntimeException("Bad line:\n" + new String(lineBuf, 0, len, StandardCharsets.UTF_8), e);
    }
  }

  void processScan(byte[] lineBuf, int idx, int len) {
    // table
    idx = nextData(lineBuf, idx, len);
    if (idx < 0) {
      return;
    }

    // region hash
    idx = nextData(lineBuf, idx, len);
    if (idx < 0) {
      return;
    }

    // space?
    idx = nextData(lineBuf, idx, len);
    if (idx < 0) {
      return;
    }

    // filter maybe?
    idx = nextData(lineBuf, idx, len);
    if (idx < 0) {
      return;
    }

    // ?
    idx = nextData(lineBuf, idx, len);
    if (idx < 0) {
      return;
    }

    // ?
    idx = nextData(lineBuf, idx, len);
    if (idx < 0) {
      return;
    }

    // ?
    idx = nextData(lineBuf, idx, len);
    if (idx < 0) {
      return;
    }

    // filter name in format `name: "class"`
    idx = nextData(lineBuf, idx, len);
    if (idx < 0) {
      return;
    }
    // EOL
  }

  void processMulti(byte[] lineBuf, int idx, int len) {
    // loop of table and possibly the region
    while (idx < len) {
      // table
      int start = idx;
      idx = nextData(lineBuf, idx, len);
      // validate as the line ends with the table repeating... weird.
      String table = new String(lineBuf, start, idx - start - 1);

      start = idx;
      // could be the region hash or just another table for some wierd reason.
      idx = nextData(lineBuf, idx, len);
      if (idx < 0) {
        return;
      }
      String regionMaybe = new String(lineBuf, start, idx - start - 1);
      if (!HASH.matcher(regionMaybe).find()) {
        idx = start;
        continue;
      }

      // was a hash, yay!
      // loop till we hit another table. Multiple commands per region.
      while (idx < len) {
        if (lineBuf[idx] != 'p') {
          // TODO - if the table starts with a `p` or whatever command, we're
          // goobered.
          idx = start;
          break;
        }
        idx += 2;

        // hopefully just the key?
        start = idx;
        idx = nextData(lineBuf, idx, len);
        String key = hbaseToHex(lineBuf, start, idx - start - 1);

        LOG.info("Wrote key {} to region {}",
                key, regionMaybe);

        processKey(key);
      }
    }

  }

  void processKey(String hexKey) {
    // wasteful yeah, but works.
    hexKey = hexKey.replace("\\x", "");
    byte[] rowKey = UniqueId.stringToUid(hexKey);

    // parse metric and tags
    byte[] metric = Arrays.copyOfRange(rowKey, schema.saltWidth(),
            schema.saltWidth() + schema.uidWidth(UniqueIdType.METRIC));
    try {
      String metricString = schema.getName(UniqueIdType.METRIC, metric, null).join();
      KeyTracker tracker = results.get(metricString);
      if (tracker == null) {
        tracker = new KeyTracker();
        tracker.metric = metricString;
        results.put(metricString, tracker);
      }
      tracker.hits++;

      // timestamp
      int time = (int) schema.baseTimestamp(rowKey);
      tracker.baseTimestamp.add(time);

      // tags
      int i = schema.saltWidth() + schema.metricWidth() + Schema.TIMESTAMP_BYTES;
      while (i < rowKey.length) {
        if (i + schema.tagkWidth() >= rowKey.length) {
          LOG.error("Ummm not a row key as the tag key is borked at idx: " + i + " of " + rowKey.length);
          break;
        }
        String tagKey = schema.getName(UniqueIdType.TAGK,
                Arrays.copyOfRange(rowKey, i, i + schema.tagkWidth()), null).join();
        i += schema.tagkWidth();

        if (i + schema.tagvWidth() >= rowKey.length) {
          LOG.error("Ummm not a row key as the tag value is borked at idx: " + i + " of " + rowKey.length);
          break;
        }
        String tagValue = schema.getName(UniqueIdType.TAGV,
                Arrays.copyOfRange(rowKey, i, i + schema.tagvWidth()), null).join();
        i += schema.tagvWidth();

        tracker.tags.put(tagKey, tagValue);

        // soooo the log seems to be appending the value to the row key so, with an
        // append, we only have two bytes (or sometimes one at the top of the hour)
        // so we can stop if we only have less than the tag key left.
        if (i + schema.tagkWidth() >= rowKey.length) {
          break;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  int nextData(byte[] buf, int start, int len) {
    while (start < len) {
      if (buf[start++] == '\t') {
        return start;
      }
    }
    // end of line
    return -1;
  }

  byte[] charConverter = new byte[1];
  String hbaseToHex(byte[] buffer, int idx, int len) {
    StringBuilder buf = new StringBuilder();
    int escaped = buffer[idx] == '\\' ? 1 : 0;
    if (escaped == 1) {
      buf.append("\\");
      idx++;
    }

    for (int i = idx; i < idx + len; i++) {
      char c = (char) buffer[i];
      if (c == '\\') {
        buf.append(c);
        escaped = 1;
      } else if (escaped > 0) {
        buf.append(c);
        if (++escaped >= 4) {
          escaped = 0;
        }
      } else {
        // not escaped so convert it!
        charConverter[0] = (byte) c;
        String hex = UniqueId.uidToString(charConverter);
        buf.append("\\x")
                .append(hex.toUpperCase(Locale.ROOT));
      }
    }
    return buf.toString();
  }

  String getMetric(String key, int saltWidth) {
    int len = 4 * schema.metricWidth();
    if (saltWidth > 0) {
      return key.substring(saltWidth * 4, saltWidth * 4 + len);
    }
    return key.substring(0, len);
  }

  public static void main(String[] args) throws Exception {
    final Configuration config = new Configuration(args);
    final TSDB tsdb = new DefaultTSDB(config);
    tsdb.initializeRegistry(true).join(300_000);
    SlowLogParser parser = new SlowLogParser(tsdb);

    if (args == null || args.length < 1) {
      System.err.println("Must have at least one argument that is either a file " +
              "or a comma separate list of files to process.");
      System.exit(1);
    }

    parser.run(args[args.length - 1]);
  }
}
