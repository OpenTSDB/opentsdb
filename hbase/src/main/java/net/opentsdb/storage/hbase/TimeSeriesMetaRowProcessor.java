package net.opentsdb.storage.hbase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.uid.IdUtils;
import org.hbase.async.KeyValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

class TimeSeriesMetaRowProcessor extends RowProcessor<List<TSMeta>> {
  private final Pattern scanPattern;
  private final ObjectMapper jsonMapper;
  private final List<TSMeta> metas;

  TimeSeriesMetaRowProcessor(final Pattern scanPattern,
                             final ObjectMapper jsonMapper) {
    this.scanPattern = scanPattern;
    this.jsonMapper = jsonMapper;
    this.metas = Lists.newArrayList();
  }

  @Override
  protected List<TSMeta> getResult() {
    return metas;
  }

  @Override
  protected void processRow(final ArrayList<KeyValue> row) {
    final KeyValue cell = row.get(0);

    // TODO - there MUST be a better way than creating a ton of temp
    // string objects.
    if (scanPattern == null ||
        scanPattern.matcher(new String(cell.key(), HBaseConst.CHARSET)).find()) {

      if (cell.value() == null || cell.value().length < 1) {
        throw new IllegalArgumentException("Empty column value");
      }

      try {
        final TSMeta meta = jsonMapper.readValue(cell.value(), TSMeta.class);

        // fix in case the tsuid is missing
        if (meta.getTSUID() == null || meta.getTSUID().isEmpty()) {
          meta.setTSUID(IdUtils.uidToString(cell.key()));
        }

        metas.add(meta);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse timeseries meta" +
            " object for ID " + IdUtils.uidToString(cell.key()), e);
      }
    }
  }
}
