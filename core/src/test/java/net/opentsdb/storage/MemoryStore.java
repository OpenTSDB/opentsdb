package net.opentsdb.storage;

import static net.opentsdb.uid.IdUtils.uidToString;

import net.opentsdb.core.DataPoints;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.TimeseriesId;
import net.opentsdb.uid.UniqueIdType;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * TsdbStore implementation useful in testing calls to and from storage with actual pretend data.
 * The underlying data store is an incredibly ugly nesting of ByteMaps from AsyncHbase so it stores
 * and orders byte arrays similar to HBase. A MemoryStore instance represents a SINGLE table in
 * HBase but it provides support for column families and timestamped entries.
 *
 * <p>It's not a perfect implementation but is useful for the majority of unit tests. Gets, puts,
 * cas, deletes and scans are currently supported. See notes for each method below about what does
 * and doesn't work.
 *
 * <p>Regarding timestamps, whenever you execute an RPC request, the {@code current_timestamp} will
 * be incremented by one millisecond. By default the timestamp starts at 1/1/2014 00:00:00 but you
 * can set it to any value at any time. If a PutRequest comes in with a specific time, that time
 * will be stored and the timestamp will not be incremented.
 *
 * @since 2.0
 */
public class MemoryStore extends TsdbStore {
  private static final Charset ASCII = Charsets.ISO_8859_1;

  private final Table<LabelId, String, LabelMeta> uid_table;
  private final Table<String, Long, Annotation> annotation_table;

  private final Map<TimeseriesId, NavigableMap<Long, Number>> datapoints;

  private final Table<String, UniqueIdType, LabelId> uid_forward_mapping;
  private final Table<LabelId, UniqueIdType, String> uid_reverse_mapping;

  public MemoryStore() {
    uid_table = HashBasedTable.create();
    annotation_table = HashBasedTable.create();
    uid_forward_mapping = HashBasedTable.create();
    uid_reverse_mapping = HashBasedTable.create();
    datapoints = Maps.newHashMap();
  }

  @Nonnull
  @Override
  public ListenableFuture<Void> addPoint(final TimeseriesId tsuid,
                                         final long timestamp,
                                         final float value) {
    return addPoint(tsuid, value, timestamp);
  }

  @Nonnull
  @Override
  public ListenableFuture<Void> addPoint(final TimeseriesId tsuid,
                                         final long timestamp,
                                         final double value) {
    return addPoint(tsuid, value, timestamp);
  }

  @Nonnull
  @Override
  public ListenableFuture<Void> addPoint(final TimeseriesId tsuid,
                                         final long timestamp,
                                         final long value) {
    return addPoint(tsuid, (Number) value, timestamp);
  }

  private ListenableFuture<Void> addPoint(final TimeseriesId tsuid,
                                          final Number value,
                                          final long timestamp) {
    /*
     * TODO(luuse): tsuid neither implements #equals, #hashCode or Comparable.
     * Should implement a custom TimeseriesId for MemoryStore that implements all
     * of these.
     */
    NavigableMap<Long, Number> tsuidDps = datapoints.get(tsuid);

    if (tsuidDps == null) {
      tsuidDps = Maps.newTreeMap();
      datapoints.put(tsuid, tsuidDps);
    }

    tsuidDps.put(timestamp, value);

    return Futures.immediateFuture(null);
  }

  @Override
  public void close() {
  }

  @Nonnull
  @Override
  public ListenableFuture<Optional<LabelId>> getId(String name,
                                                   UniqueIdType type) {
    LabelId id = uid_forward_mapping.get(name, type);
    return Futures.immediateFuture(Optional.fromNullable(id));
  }

  @Nonnull
  @Override
  public ListenableFuture<Optional<String>> getName(final LabelId id,
                                                    final UniqueIdType type) {
    final String name = uid_reverse_mapping.get(id, type);
    return Futures.immediateFuture(Optional.fromNullable(name));
  }

  @Nonnull
  @Override
  public ListenableFuture<LabelMeta> getMeta(final LabelId uid,
                                             final UniqueIdType type) {
    final String qualifier = type.toString().toLowerCase() + "_meta";
    final LabelMeta meta = uid_table.get(uid, qualifier);
    return Futures.immediateFuture(meta);
  }

  @Override
  public ListenableFuture<Boolean> updateMeta(final LabelMeta meta) {
    uid_table.put(
        meta.identifier(),
        meta.type().toString().toLowerCase() + "_meta",
        meta);

    return Futures.immediateFuture(Boolean.TRUE);
  }

  @Override
  public ListenableFuture<Void> deleteUID(final String name, UniqueIdType type) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ListenableFuture<List<byte[]>> executeTimeSeriesQuery(final ResolvedSearchQuery query) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Nonnull
  @Override
  public ListenableFuture<LabelId> allocateUID(final String name,
                                               final UniqueIdType type) {
    LabelId id;

    do {
      id = new MemoryLabelId();
      // Make sure the new id is unique
    } while (uid_reverse_mapping.containsRow(id));

    return allocateUID(name, id, type);
  }

  @Nonnull
  @Override
  public ListenableFuture<LabelId> allocateUID(final String name,
                                               final LabelId id,
                                               final UniqueIdType type) {
    if (uid_reverse_mapping.contains(id, type)) {
      throw new IllegalArgumentException("An ID with " + id + " already exists");
    }

    uid_reverse_mapping.put(id, type, name);

    if (uid_forward_mapping.contains(name, type)) {
      return Futures.immediateFuture(uid_forward_mapping.get(name, type));
    }

    uid_forward_mapping.put(name, type, id);

    return Futures.immediateFuture(id);
  }

  /**
   * Attempts to mark an Annotation object for deletion. Note that if the annoation does not exist
   * in storage, this delete call will not throw an error.
   *
   * @param annotation
   * @return A meaningless Deferred for the caller to wait on until the call is complete. The value
   * may be null.
   */
  @Override
  public ListenableFuture<Void> delete(Annotation annotation) {

    final String tsuid = annotation.timeSeriesId() != null && !annotation.timeSeriesId().isEmpty() ?
        annotation.timeSeriesId() : "";

    final long start = annotation.startTime() % 1000 == 0 ?
        annotation.startTime() / 1000 : annotation.startTime();
    if (start < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    annotation_table.remove(tsuid, start);

    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Boolean> updateAnnotation(Annotation annotation) {
    final Annotation changedAnnotation = annotation_table.put(annotation.timeSeriesId(),
        annotation.startTime(), annotation);
    return Futures.immediateFuture(!annotation.equals(changedAnnotation));
  }

  @Override
  public ListenableFuture<List<Annotation>> getGlobalAnnotations(final long startTime,
                                                                 final long endTime) {
    //some sanity check should happen before this is called actually.

    if (startTime < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    List<Annotation> annotations = new ArrayList<>();
    Collection<Annotation> globals = annotation_table.row("").values();

    for (Annotation global : globals) {
      if (startTime <= global.startTime() && global.startTime() <= endTime) {
        annotations.add(global);
      }
    }
    return Futures.immediateFuture(annotations);
  }

  @Override
  public ListenableFuture<Integer> deleteAnnotationRange(final byte[] tsuid,
                                                         final long startTime,
                                                         final long endTime) {

    //some sanity check should happen before this is called actually.
    //however we need to modify the start_time and end_time

    if (startTime < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    final long start = startTime % 1000 == 0 ? startTime / 1000 : startTime;
    final long end = endTime % 1000 == 0 ? endTime / 1000 : endTime;
    String key = "";

    ArrayList<Annotation> del_list = new ArrayList<>();
    if (tsuid != null) {
      //convert byte array to string, sloppy but seems to be the best way
      key = uidToString(tsuid);
    }

    Collection<Annotation> globals = annotation_table.row(key).values();
    for (Annotation global : globals) {
      if (start <= global.startTime() && global.startTime() <= end) {
        del_list.add(global);
      }
    }

    for (Annotation a : del_list) {
      delete(a);
    }
    return Futures.immediateFuture(del_list.size());

  }

  /**
   * Attempts to fetch a global or local annotation from storage
   *
   * @param tsuid The TSUID as a byte array. May be null if retrieving a global annotation
   * @param startTime The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  @Override
  public ListenableFuture<Annotation> getAnnotation(final byte[] tsuid, final long startTime) {

    if (startTime < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    long time = startTime % 1000 == 0 ? startTime / 1000 : startTime;

    String key = "";
    if (tsuid != null) {
      //convert byte array to proper string
      key = uidToString(tsuid);
    }
    return Futures.immediateFuture(annotation_table.get(key, time));
  }

  /**
   * Finds all the {@link net.opentsdb.core.Span}s that match this query. This is what actually
   * scans the HBase table and loads the data into {@link net.opentsdb.core.Span}s.
   *
   * @param query
   * @return A map from HBase row key to the {@link net.opentsdb.core.Span} for that row key. Since
   * a {@link net.opentsdb.core.Span} actually contains multiple HBase rows, the row key stored in
   * the map has its timestamp zero'ed out.
   * @throws IllegalArgumentException if bad data was retreived from HBase.
   */
  @Override
  public ListenableFuture<ImmutableList<DataPoints>> executeQuery(final Object query) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ListenableFuture<List<IdentifierDecorator>> executeIdQuery(final IdQuery query) {
    Predicate<UniqueIdType> typeMatchFunction = new Predicate<UniqueIdType>() {
      @Override
      public boolean apply(@Nullable final UniqueIdType input) {
        return query.getType() == null || query.getType() == input;
      }
    };

    Predicate<String> nameMatchFunction = new Predicate<String>() {
      @Override
      public boolean apply(@Nullable final String name) {
        return query.getQuery() == null || name.startsWith(query.getQuery());
      }
    };

    final List<IdentifierDecorator> result = new ArrayList<>();

    for (final Table.Cell<String, UniqueIdType, LabelId> cell : uid_forward_mapping.cellSet()) {
      if (typeMatchFunction.apply(cell.getColumnKey()) &&
          nameMatchFunction.apply(cell.getRowKey())) {
        result.add(new IdentifierDecorator() {
          @Override
          public LabelId getId() {
            return cell.getValue();
          }

          @Override
          public UniqueIdType getType() {
            return cell.getColumnKey();
          }

          @Override
          public String getName() {
            return cell.getRowKey();
          }
        });
      }
    }

    return Futures.immediateFuture(result);
  }

}
