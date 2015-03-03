
package net.opentsdb.storage.hbase;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

class LastWriteTimesQueryRunner extends RowProcessor<Map<byte[], Long>> {
  private final Bytes.ByteMap<Long> tsuids;
  private final Pattern scanPattern;

  LastWriteTimesQueryRunner(final Pattern scanPattern) {
    this.scanPattern = scanPattern;
    this.tsuids = new Bytes.ByteMap<Long>();
  }

  @Override
  protected Bytes.ByteMap<Long> getResult() {
    return tsuids;
  }

  @Override
  protected void processRow(final ArrayList<KeyValue> row) {
    final byte[] tsuid = row.get(0).key();

    // TODO - there MUST be a better way than creating a ton of temp
    // string objects.
    if (scanPattern == null ||
        scanPattern.matcher(new String(tsuid, HBaseConst.CHARSET)).find()) {
      tsuids.put(tsuid, row.get(0).timestamp());
    }
  }
}
