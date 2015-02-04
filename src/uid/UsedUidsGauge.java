package net.opentsdb.uid;

import com.codahale.metrics.CachedGauge;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.Const;
import net.opentsdb.storage.TsdbStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

class UsedUidsGauge extends CachedGauge<Long> {
  private static final Logger LOG = LoggerFactory.getLogger(UsedUidsGauge.class);

  private static final short TIMEOUT = 10;

  private static final Object LOCK = new Object();
  private static Deferred<Map<String, Long>> usedUids;

  private final UniqueIdType type;
  private final TsdbStore store;
  private final byte[] table;

  private Deferred previousUsedUids;

  UsedUidsGauge(UniqueIdType type, TsdbStore store, final byte[] table) {
    super(TIMEOUT, TimeUnit.MINUTES);

    this.type = checkNotNull(type);
    this.store = checkNotNull(store);
    this.table = checkNotNull(table);
  }

  @Override
  protected Long loadValue() {
    final byte[][] kinds = {
        Const.METRICS_QUAL.getBytes(Const.CHARSET_ASCII),
        Const.TAG_NAME_QUAL.getBytes(Const.CHARSET_ASCII),
        Const.TAG_VALUE_QUAL.getBytes(Const.CHARSET_ASCII)
    };

    synchronized (LOCK) {
      // If the values has not been loaded since the last time we were here
      // then it is time to load new values.
      if (usedUids == previousUsedUids) {
        usedUids = UniqueId.getUsedUIDs(store, kinds, table);
      }

      // Regardless of whether this instance started to load the new values
      // or not we want to record which instance we use so that we can check
      // it next time.
      previousUsedUids = usedUids;
    }

    try {
      return usedUids.joinUninterruptibly().get(type.toValue());
    } catch (Exception e) {
      LOG.error("Received an error while attempting to fetch the number of " +
          "used uids", e);
    }

    return Long.valueOf(-1);
  }
}
