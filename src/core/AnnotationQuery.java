package net.opentsdb.core;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.opentsdb.uid.NoSuchUniqueName;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A query to retrieve annotation data from the TSDB.
 */
public class AnnotationQuery {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(AnnotationQuery.class);

  private TSDB tsdb;
  private byte[] metric;
  private long startTime;
  private long endTime;
  private List<byte[]> tags;

  public AnnotationQuery(TSDB tsdb, long startTime, long endTime,
      Map<String, String> tags) {
    this.tsdb = tsdb;
    this.startTime = startTime;
    this.endTime = endTime;
    this.tags = Tags.resolveAll(tsdb, tags);
    try {
      this.metric = tsdb.metrics.getId(Const.ANNOTATION_NAME);
    } catch (NoSuchUniqueName e) {
      LOGGER.debug("not yet stored a timeline annotation", e);
    }
  }

  public List<Annotation> run() {
    final List<Annotation> result = new ArrayList<Annotation>();

    if (metric != null) {
      final Scanner scanner = getScanner();
      final short metricWidth = tsdb.metrics.width();
      ArrayList<ArrayList<KeyValue>> rows;

      try {
        while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
          for (final ArrayList<KeyValue> row : rows) {
            final byte[] key = row.get(0).key();
            if (Bytes.memcmp(metric, key, 0, metricWidth) != 0) {
              throw new IllegalDataException(
                  "HBase returned a row that doesn't match" + " our scanner ("
                      + scanner + ")! " + row + " does not start" + " with "
                      + Arrays.toString(metric));
            }
            for (KeyValue keyValue : row) {
              result.add(new Annotation(getTimestamp(keyValue.key(),
                  keyValue.qualifier()), keyValue.value()));
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Should never be here", e);
      }
    }

    return result;
  }

  /**
   * Creates the {@link Scanner} to use for this query.
   */
  private Scanner getScanner() throws HBaseException {
    final short metricWidth = tsdb.metrics.width();
    final byte[] startRow = new byte[metricWidth + Const.TIMESTAMP_BYTES];
    final byte[] endRow = new byte[metricWidth + Const.TIMESTAMP_BYTES];

    Bytes.setInt(startRow, (int) startTime, metricWidth);
    Bytes.setInt(endRow, (int) endTime, metricWidth);
    System.arraycopy(metric, 0, startRow, 0, metricWidth);
    System.arraycopy(metric, 0, endRow, 0, metricWidth);

    final Scanner scanner = tsdb.client.newScanner(tsdb.table);
    scanner.setStartKey(startRow);
    scanner.setStopKey(endRow);
    if (tags.size() > 0) {
      createAndSetFilter(scanner);
    }
    scanner.setFamily(TSDB.FAMILY);

    return scanner;
  }

  /**
   * Sets the server-side regexp filter on the scanner. In order to find the
   * rows with the relevant tags, we use a server-side filter that matches a
   * regular expression on the row key.
   * 
   * @param scanner
   *          The scanner on which to add the filter.
   */
  void createAndSetFilter(final Scanner scanner) {
    final short nameWidth = tsdb.tag_names.width();
    final short valueWidth = tsdb.tag_values.width();
    final short tagsize = (short) (nameWidth + valueWidth);
    // Generate a regexp for our tags. Say we have 2 tags: { 0 0 1 0 0 2 }
    // and { 4 5 6 9 8 7 }, the regexp will be:
    // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
    final StringBuilder buf = new StringBuilder(15 // "^.{N}" + "(?:.{M})*"
                                                   // + "$"
        + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
        * tags.size()));
    // In order to avoid re-allocations, reserve a bit more w/ groups ^^^

    // Alright, let's build this regexp. From the beginning...
    buf.append("(?s)" // Ensure we use the DOTALL flag.
        + "^.{")
    // ... start by skipping the metric ID and timestamp.
        .append(tsdb.metrics.width() + Const.TIMESTAMP_BYTES).append("}");
    final Iterator<byte[]> tags = this.tags.iterator();
    byte[] tag = tags.hasNext() ? tags.next() : null;
    // Tags and group_bys are already sorted. We need to put them in the
    // regexp in order by ID, which means we just merge two sorted lists.
    do {
      // Skip any number of tags.
      buf.append("(?:.{").append(tagsize).append("})*\\Q");
      addId(buf, tag);
      tag = tags.hasNext() ? tags.next() : null;
    } while (tag != null);
    // Skip any number of tags before the end.
    buf.append("(?:.{").append(tagsize).append("})*$");
    scanner.setKeyRegexp(buf.toString(), Charset.forName("ISO-8859-1"));
  }

  /**
   * Appends the given ID to the given buffer, followed by "\\E".
   */
  private void addId(final StringBuilder buf, final byte[] id) {
    boolean backslash = false;
    for (final byte b : id) {
      buf.append((char) (b & 0xFF));
      if (b == 'E' && backslash) { // If we saw a `\' and now we have a
                                   // `E'.
        // So we just terminated the quoted section because we just
        // added \E
        // to `buf'. So let's put a litteral \E now and start quoting
        // again.
        buf.append("\\\\E\\Q");
      } else {
        backslash = b == '\\';
      }
    }
    buf.append("\\E");
  }

  private long getTimestamp(byte[] key, byte[] qualifier) {
    final int baseTime = Bytes.getInt(key, tsdb.metrics.width());
    final short delta = (short) ((Bytes.getShort(qualifier) & 0xFFFF) >>> Const.FLAG_BITS);
    return baseTime + delta;
  }
}
