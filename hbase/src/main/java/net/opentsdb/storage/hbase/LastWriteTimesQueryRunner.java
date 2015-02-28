
package net.opentsdb.storage.hbase;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

import java.util.ArrayList;
import java.util.Map;

class LastWriteTimesQueryRunner extends RowProcessor<Map<byte[], Long>> {
  private final Bytes.ByteMap<Long> tsuids;

  LastWriteTimesQueryRunner() {
    tsuids = new Bytes.ByteMap<Long>();
  }

  @Override
  protected Bytes.ByteMap<Long> getResult() {
    return tsuids;
  }

  @Override
  protected void processRow(final ArrayList<KeyValue> row) {
    tsuids.put(row.get(0).key(), row.get(0).timestamp());
  }
}
