
package net.opentsdb.storage;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.DataPoints;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.TimeseriesId;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;

import net.opentsdb.uid.UniqueIdType;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.uid.IdUtils.uidToString;

/**
 * TsdbStore implementation useful in testing calls to and from
 * storage with actual pretend data. The underlying data store is an
 * incredibly ugly nesting of ByteMaps from AsyncHbase so it stores and
 * orders byte arrays similar to HBase. A MemoryStore instance represents a
 * SINGLE table in HBase but it provides support for column families and
 * timestamped entries.
 * <p>
 * It's not a perfect implementation but is useful for the majority of unit
 * tests. Gets, puts, cas, deletes and scans are currently supported. See
 * notes for each method below about what does and doesn't work.
 * <p>
 * Regarding timestamps, whenever you execute an RPC request, the
 * {@code current_timestamp} will be incremented by one millisecond. By default
 * the timestamp starts at 1/1/2014 00:00:00 but you can set it to any value
 * at any time. If a PutRequest comes in with a specific time, that time will
 * be stored and the timestamp will not be incremented.
 * <p>
 * @since 2.0
 */
public class MemoryStore implements TsdbStore {
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
  public Deferred<Void> addPoint(@Nonnull final TimeseriesId tsuid,
                                   final long timestamp,
                                   final float value) {
    return addPoint(tsuid, value, timestamp);
  }

  @Nonnull
  @Override
  public Deferred<Void> addPoint(@Nonnull final TimeseriesId tsuid,
                                   final long timestamp,
                                   final double value) {
    return addPoint(tsuid, value, timestamp);
  }

  @Nonnull
  @Override
  public Deferred<Void> addPoint(@Nonnull final TimeseriesId tsuid,
                                   final long timestamp,
                                   final long value) {
    return addPoint(tsuid, (Number) value, timestamp);
  }

  private Deferred<Void> addPoint(final TimeseriesId tsuid,
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

    return Deferred.fromResult(null);
  }

  @Override
  public void close() {
  }

  @Nonnull
  @Override
  public Deferred<Optional<LabelId>> getId(@Nonnull String name,
                                           @Nonnull UniqueIdType type) {
    LabelId id = uid_forward_mapping.get(name, type);
    return Deferred.fromResult(Optional.fromNullable(id));
  }

  @Nonnull
  @Override
  public Deferred<Optional<String>> getName(@Nonnull final LabelId id,
                                            @Nonnull final UniqueIdType type) {
    final String name = uid_reverse_mapping.get(id, type);
    return Deferred.fromResult(Optional.fromNullable(name));
  }

  @Nonnull
  @Override
  public Deferred<LabelMeta> getMeta(@Nonnull final LabelId uid,
                                     @Nonnull final UniqueIdType type) {
    final String qualifier = type.toString().toLowerCase() + "_meta";
    final LabelMeta meta = uid_table.get(uid, qualifier);
    return Deferred.fromResult(meta);
  }

  @Override
  public Deferred<Boolean> updateMeta(final LabelMeta meta) {
    uid_table.put(
        meta.identifier(),
        meta.type().toString().toLowerCase() + "_meta",
        meta);

    return Deferred.fromResult(Boolean.TRUE);
  }

  @Override
  public Deferred<Void> deleteUID(final String name, UniqueIdType type) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<List<byte[]>> executeTimeSeriesQuery(final ResolvedSearchQuery query) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Nonnull
  @Override
  public Deferred<LabelId> allocateUID(@Nonnull final String name,
                                       @Nonnull final UniqueIdType type) {
    LabelId id;

    do {
      id = new MemoryLabelId();
      // Make sure the new id is unique
    } while (uid_reverse_mapping.containsRow(id));

    return allocateUID(name, id, type);
  }

  @Nonnull
  @Override
  public Deferred<LabelId> allocateUID(@Nonnull final String name,
                                       @Nonnull final LabelId id,
                                       @Nonnull final UniqueIdType type) {
    if (uid_reverse_mapping.contains(id, type)) {
      throw new IllegalArgumentException("An ID with " + id + " already exists");
    }

    uid_reverse_mapping.put(id, type, name);

    if (uid_forward_mapping.contains(name, type)) {
      return Deferred.fromResult(uid_forward_mapping.get(name,type));
    }

    uid_forward_mapping.put(name, type, id);

    return Deferred.fromResult(id);
  }

  /**
   * Attempts to mark an Annotation object for deletion. Note that if the
   * annoation does not exist in storage, this delete call will not throw an
   * error.
   *
   * @param annotation
   * @return A meaningless Deferred for the caller to wait on until the call is
   * complete. The value may be null.
   */
  @Override
  public Deferred<Void> delete(Annotation annotation) {

    final String tsuid = annotation.getTSUID() != null && !annotation.getTSUID().isEmpty() ?
            annotation.getTSUID() : "";

    final long start = annotation.getStartTime() % 1000 == 0 ?
            annotation.getStartTime() / 1000 : annotation.getStartTime();
    if (start < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    annotation_table.remove(tsuid, start);

    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Boolean> updateAnnotation(Annotation original, Annotation annotation) {

    String tsuid = !Strings.isNullOrEmpty(annotation.getTSUID()) ?
            annotation.getTSUID() : "";

    final long start = annotation.getStartTime() % 1000 == 0 ?
            annotation.getStartTime() / 1000 : annotation.getStartTime();
    if (start < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    Annotation note = annotation_table.get(tsuid, start);

    if ((null == note && null == original) ||
            (null != note && null != original && note.equals(original))) {
      annotation_table.remove(tsuid, start);
      annotation_table.put(tsuid, start, annotation);
      return Deferred.fromResult(true);
    }
    return Deferred.fromResult(false);
  }

  @Override
  public Deferred<List<Annotation>> getGlobalAnnotations(final long start_time,
                                                         final long end_time) {
    //some sanity check should happen before this is called actually.

    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    List<Annotation> annotations = new ArrayList<>();
    Collection<Annotation> globals = annotation_table.row("").values();

    for (Annotation global: globals) {
      if (start_time <= global.getStartTime() && global.getStartTime() <= end_time) {
        annotations.add(global);
      }
    }
    return Deferred.fromResult(annotations);
  }

  @Override
  public Deferred<Integer> deleteAnnotationRange(final byte[] tsuid,
                                                 final long start_time,
                                                 final long end_time) {

    //some sanity check should happen before this is called actually.
    //however we need to modify the start_time and end_time

    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    final long start = start_time % 1000 == 0 ? start_time / 1000 : start_time;
    final long end = end_time % 1000 == 0 ? end_time / 1000 : end_time;
    String key = "";

    ArrayList<Annotation> del_list = new ArrayList<>();
    if (tsuid != null) {
      //convert byte array to string, sloppy but seems to be the best way
      key = uidToString(tsuid);
    }

    Collection<Annotation> globals = annotation_table.row(key).values();
    for (Annotation global: globals) {
      if (start <= global.getStartTime() && global.getStartTime() <= end) {
        del_list.add(global);
      }
    }

    for (Annotation a : del_list) {
      delete(a);
    }
    return Deferred.fromResult(del_list.size());

  }

    /**
   * Attempts to fetch a global or local annotation from storage
   * @param tsuid The TSUID as a byte array. May be null if retrieving a global
   * annotation
   * @param start_time The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  @Override
  public Deferred<Annotation> getAnnotation(final byte[] tsuid, final long start_time) {

    if (start_time < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    long time = start_time % 1000 == 0 ? start_time / 1000 : start_time;

    String key = "";
    if (tsuid != null) {
      //convert byte array to proper string
      key = uidToString(tsuid);
    }
    return Deferred.fromResult(annotation_table.get(key, time));
  }

  /**
   * Finds all the {@link net.opentsdb.core.Span}s that match this query.
   * This is what actually scans the HBase table and loads the data into
   * {@link net.opentsdb.core.Span}s.
   * @return A map from HBase row key to the {@link net.opentsdb.core.Span} for that row key.
   * Since a {@link net.opentsdb.core.Span} actually contains multiple HBase rows, the row key
   * stored in the map has its timestamp zero'ed out.
   * @throws IllegalArgumentException if bad data was retreived from HBase.
   * @param query
   */
  @Override
  public Deferred<ImmutableList<DataPoints>> executeQuery(final Object query) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<List<IdentifierDecorator>> executeIdQuery(final IdQuery query) {
    Predicate<UniqueIdType> typeMatchFunction = new Predicate<UniqueIdType>() {
      @Override
      public boolean apply(final UniqueIdType input) {
        return query.getType() == null || query.getType() == input;
      }
    };

    Predicate<String> nameMatchFunction = new Predicate<String>() {
      @Override
      public boolean apply(final String name) {
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

    return Deferred.fromResult(result);
  }

  private static class MemoryLabelId implements LabelId<MemoryLabelId> {
    private final UUID uuid;

    MemoryLabelId() {
      this(UUID.randomUUID());
    }

    MemoryLabelId(@Nonnull final UUID uuid) {
      this.uuid = checkNotNull(uuid);
    }

    @Override
    public int compareTo(@Nonnull final MemoryLabelId other) {
      return uuid.compareTo(other.uuid);
    }

    @Override
    public boolean equals(final Object that) {
      if (that == this)
        return true;

      if (that instanceof MemoryLabelId) {
        return ((MemoryLabelId) that).uuid.equals(uuid);
      }

      return false;
    }
  }
}
