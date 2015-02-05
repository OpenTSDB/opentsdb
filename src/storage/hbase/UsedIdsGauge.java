package net.opentsdb.storage.hbase;

import com.codahale.metrics.CachedGauge;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.uid.UniqueIdType;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link com.codahale.metrics.CachedGauge} that continuously measures the
 * number of used IDs.
 */
class UsedIdsGauge extends CachedGauge<Long> {
  private static final Logger LOG = LoggerFactory.getLogger(UsedIdsGauge.class);

  /**
   * The number of minutes between it fetches the information from HBase.
   */
  private static final short TIMEOUT = 10;

  /**
   * We fetch the information for all types of IDs at the same time so we need a
   * lock so other instances don't go and fetch their own.
   */
  private static final Object LOCK = new Object();

  /**
   * The most current ID usage information.
   */
  private static Deferred<Map<UniqueIdType, Long>> usedIds;

  /**
   * The type of IDs this instance represents.
   */
  private final UniqueIdType type;

  /**
   * The HBase client used for talking to HBase.
   */
  private final HBaseClient client;

  /**
   * Which table to look in.
   */
  private final byte[] table;

  /**
   * This is the last deferred that this instance read the usage information
   * from. When this is equal to {@link UsedIdsGauge#usedIds} and it is time
   * to take another samepl, then it is time to load new values.
   */
  private Deferred previousUsedIds;

  UsedIdsGauge(UniqueIdType type, HBaseClient client, final byte[] table) {
    super(TIMEOUT, TimeUnit.MINUTES);

    this.type = checkNotNull(type);
    this.client = checkNotNull(client);
    this.table = checkNotNull(table);
  }

  @Override
  protected Long loadValue() {
    synchronized (LOCK) {
      // If the values has not been loaded since the last time we were here
      // then it is time to load new values.
      if (usedIds == previousUsedIds) {
        usedIds = getUsedIDs(client, table);
      }

      // Regardless of whether this instance started to load the new values
      // or not we want to record which instance we use so that we can check
      // it next time.
      previousUsedIds = usedIds;
    }

    try {
      return usedIds.joinUninterruptibly().get(type);
    } catch (Exception e) {
      LOG.error("Received an error while attempting to fetch the number of " +
          "used IDs for {}", type, e);
      return (long) -1;
    }
  }

  /**
   * Returns a map of max IDs from HBase.
   *
   * @param client The HBaseClient to use for looking up the information.
   * @param table  The table to look in.
   * @return A map with the {@link net.opentsdb.uid.UniqueIdType} as the key and
   * the maximum assigned ID as the value
   */
  static Deferred<Map<UniqueIdType, Long>> getUsedIDs(final HBaseClient client,
                                                      final byte[] table) {
    final class GetCB implements Callback<Map<UniqueIdType, Long>, ArrayList<KeyValue>> {
      @Override
      public Map<UniqueIdType, Long> call(final ArrayList<KeyValue> row) {
        final Map<UniqueIdType, Long> results = Maps.newEnumMap(UniqueIdType.class);

        for (final KeyValue column : row) {
          UniqueIdType type = UniqueIdType.fromValue(
                  new String(column.qualifier(), HBaseConst.CHARSET));
          final long count = Bytes.getLong(column.value());
          results.put(type, count);
        }

        return results;
      }
    }

    final GetRequest get = new GetRequest(table, HBaseConst.UniqueId.MAXID_ROW)
            .family(HBaseConst.UniqueId.ID_FAMILY);

    return client.get(get).addCallback(new GetCB());
  }
}
