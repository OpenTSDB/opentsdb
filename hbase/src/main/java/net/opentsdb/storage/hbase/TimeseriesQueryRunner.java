
package net.opentsdb.storage.hbase;

import com.google.common.collect.Lists;
import org.hbase.async.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Lookup time series associated with the given metric, tagk, tagv or tag pairs.
 * If no metric is given, a full table scan must be performed and this call may
 * take a long time to complete.
 *
 * To avoid running performance degrading regexes in HBase regions, we'll double
 * filter when necessary. If tagks are present, those are used in the rowkey
 * filter and a secondary filter is applied in the TSD with remaining tagvs.
 * E.g. the query "host=web01 =lga" will issue a rowkey filter with "host=web01"
 * then within the TSD scanner, we'll filter out only the rows that contain an
 * "lga" tag value. We don't know where in a row key the tagv may fall, so we
 * would have to first match on the pair, then backtrack to find the value and
 * make sure the pair is skipped. Thus its easier on the region server to
 * execute a simpler rowkey regex, pass all the results to the TSD, then let us
 * filter on tag values only when necessary. (if a query only has tag values,
 * then this is moot and we can pass them in a rowkey filter since they're
 * OR'd).
 *
 * @see net.opentsdb.core.UniqueIdClient#executeTimeSeriesQuery
 */
class TimeseriesQueryRunner extends RowProcessor<List<byte[]>> {
  private final Pattern tagKeyRegex;
  private final List<byte[]> tsuids;

  public TimeseriesQueryRunner(final Pattern tagKeyRegex) {
    this.tagKeyRegex = tagKeyRegex;
    this.tsuids = Lists.newArrayList();
  }

  /**
   * Return the list of all matching TSUIDs. This is called once the {@link
   * org.hbase.async.Scanner} is exhausted.
   *
   * @return the list of matching TSUIDs
   */
  @Override
  protected List<byte[]> getResult() {
    return tsuids;
  }

  /**
   * Process each row returned by the scanner and add all TSUIDs that matches
   * the {@link #tagKeyRegex} to the list of matching TSUIDs.
   *
   * @param row A row as returned by the {@link org.hbase.async.Scanner}
   */
  @Override
  protected void processRow(final ArrayList<KeyValue> row) {
    final byte[] tsuid = row.get(0).key();

    // TODO - there MUST be a better way than creating a ton of temp
    // string objects.
    if (tagKeyRegex == null ||
        tagKeyRegex.matcher(new String(tsuid, HBaseConst.CHARSET)).find()) {
      tsuids.add(tsuid);
    }
  }
}
