package net.opentsdb.uid;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.stats.Metrics.name;
import static net.opentsdb.stats.Metrics.tag;

import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.TsdbStore;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a table of Unique IDs, manages the lookup and creation of IDs.
 * <p/>
 * Don't attempt to use {@code equals()} or {@code hashCode()} on this class.
 */
public class UniqueId {
  private static final Logger LOG = LoggerFactory.getLogger(UniqueId.class);

  /** The TsdbStore to use. */
  private final TsdbStore tsdb_store;

  /** The type of UID represented by this cache */
  private final UniqueIdType type;

  /** Cache for forward mappings (name to ID). */
  private final ConcurrentHashMap<String, LabelId> name_cache =
      new ConcurrentHashMap<>();

  /**
   * Cache for backward mappings (ID to name).
   */
  private final ConcurrentHashMap<LabelId, String> id_cache =
      new ConcurrentHashMap<>();

  /** Map of pending UID assignments */
  private final HashMap<String, Deferred<LabelId>> pending_assignments =
      new HashMap<>();

  /** Number of times we avoided reading from TsdbStore thanks to the cache. */
  private final Counter cache_hits;
  /** Number of times we had to read from TsdbStore and populate the cache. */
  private final Counter cache_misses;

  /**
   * The event bus to which the id changes done by this instance will be published.
   */
  private final EventBus idEventBus;

  /**
   * Constructor.
   *
   * @param tsdb_store The TsdbStore to use.
   * @param type The type of UIDs this instance represents
   * @param metrics
   * @param idEventBus
   */
  public UniqueId(final TsdbStore tsdb_store,
                  final UniqueIdType type,
                  final MetricRegistry metrics,
                  final EventBus idEventBus) {
    this.tsdb_store = checkNotNull(tsdb_store);
    this.type = checkNotNull(type);
    this.idEventBus = checkNotNull(idEventBus);

    cache_hits = new Counter();
    cache_misses = new Counter();
    registerMetrics(metrics);

  }

  private void registerMetrics(final MetricRegistry registry) {
    Metrics.Tag typeTag = tag("kind", type.toValue());
    registry.register(name("uid.cache-hit", typeTag), cache_hits);
    registry.register(name("uid.cache-miss", typeTag), cache_misses);

    registry.register(name("uid.cache-size", typeTag), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return name_cache.size() + id_cache.size();
      }
    });
  }

  /**
   * Causes this instance to discard all its in-memory caches.
   *
   * @since 1.1
   */
  public void dropCaches() {
    name_cache.clear();
    id_cache.clear();
  }

  /**
   * Finds the name associated with a given ID.
   *
   * @param id The ID associated with that name.
   * @throws NoSuchUniqueId if the given ID is not assigned.
   * @throws IllegalArgumentException if the ID given in argument is encoded on the wrong number of
   * bytes.
   * @see #getId(String)
   * @see #createId(String)
   * @since 1.1
   */
  @Nonnull
  public Deferred<String> getName(@Nonnull final LabelId id) {
    final String name = getNameFromCache(id);
    if (name != null) {
      cache_hits.inc();
      return Deferred.fromResult(name);
    }
    cache_misses.inc();
    class GetNameCB implements Callback<String, Optional<String>> {
      @Override
      public String call(final Optional<String> name) {
        if (name.isPresent()) {
          addNameToCache(id, name.get());
          addIdToCache(name.get(), id);
          return name.get();
        }

        throw new NoSuchUniqueId(type, id);
      }
    }
    return tsdb_store.getName(id, type).addCallback(new GetNameCB());
  }

  @Nullable
  private String getNameFromCache(@Nonnull final LabelId id) {
    return id_cache.get(id);
  }

  private void addNameToCache(@Nonnull final LabelId id,
                              @Nonnull final String name) {
    String found = id_cache.get(id);
    if (found == null) {
      found = id_cache.putIfAbsent(id, name);
    }
    if (found != null && !found.equals(name)) {
      throw new IllegalStateException("id=" + id + " => name="
                                      + name + ", already mapped to " + found);
    }
  }

  @Nonnull
  public Deferred<LabelId> getId(@Nonnull final String name) {
    final LabelId id = getIdFromCache(name);
    if (id != null) {
      cache_hits.inc();
      return Deferred.fromResult(id);
    }
    cache_misses.inc();
    class GetIdCB implements Callback<LabelId, Optional<LabelId>> {
      @Override
      public LabelId call(final Optional<LabelId> id) {
        if (id.isPresent()) {
          addIdToCache(name, id.get());
          addNameToCache(id.get(), name);
          return id.get();
        }

        throw new NoSuchUniqueName(type.toValue(), name);
      }
    }
    return tsdb_store.getId(name, type).addCallback(new GetIdCB());
  }

  @Nullable
  private LabelId getIdFromCache(@Nonnull final String name) {
    return name_cache.get(name);
  }

  private void addIdToCache(@Nonnull final String name,
                            @Nonnull final LabelId id) {
    LabelId found = name_cache.get(name);
    if (found == null) {
      found = name_cache.putIfAbsent(name, id);
    }
    if (found != null && !found.equals(id)) {
      throw new IllegalStateException("name=" + name + " => id="
                                      + id + ", already mapped to " + found);
    }
  }

  /** Adds the bidirectional mapping in the cache. */
  private void cacheMapping(@Nonnull final String name,
                            @Nonnull final LabelId id) {
    addIdToCache(name, id);
    addNameToCache(id, name);
  }

  /**
   * Create an id with the specified name.
   *
   * @param name The name of the new id
   * @return A deferred with the byte uid if the id was successfully created
   */
  @Nonnull
  public Deferred<LabelId> createId(@Nonnull final String name) {
    Deferred<LabelId> assignment;
    synchronized (pending_assignments) {
      assignment = pending_assignments.get(name);
      if (assignment == null) {
        // to prevent UID leaks that can be caused when multiple time
        // series for the same metric or tags arrive, we need to write a
        // deferred to the pending map as quickly as possible. Then we can
        // start the assignment process after we've stashed the deferred
        // and released the lock
        assignment = new Deferred<>();
        pending_assignments.put(name, assignment);
      } else {
        LOG.info("Already waiting for UID assignment: {}", name);
        return assignment;
      }
    }

    // start the assignment dance after stashing the deferred
    Deferred<LabelId> uid = tsdb_store.allocateUID(name, type);

    uid.addCallback(new Callback<Object, LabelId>() {
      @Override
      public LabelId call(final LabelId uid) throws Exception {
        cacheMapping(name, uid);

        LOG.info("Completed pending assignment for: {}", name);
        synchronized (pending_assignments) {
          pending_assignments.remove(name);
        }

        idEventBus.post(new LabelCreatedEvent(uid, name, type));

        return uid;
      }
    });

    return uid;
  }

  /**
   * Reassigns the UID to a different name (non-atomic).
   * <p/>
   * Whatever was the UID of {@code oldname} will be given to {@code newname}. {@code oldname} will
   * no longer be assigned a UID.
   * <p/>
   * Beware that the assignment change is <b>not atommic</b>.  If two threads or processes attempt
   * to rename the same UID differently, the result is unspecified and might even be inconsistent.
   * This API is only here for administrative purposes, not for normal programmatic interactions.
   *
   * @param oldname The old name to rename.
   * @param newname The new name.
   * @throws NoSuchUniqueName if {@code oldname} wasn't assigned.
   * @throws IllegalArgumentException if {@code newname} was already assigned.
   */
  public Deferred<Void> rename(final String oldname, final String newname) {
    return checkUidExists(newname).addCallbackDeferring(new Callback<Deferred<Void>, Boolean>() {
      @Override
      public Deferred<Void> call(final Boolean exists) {
        if (exists) {
          throw new IllegalArgumentException("An UID with name " + newname + " " +
                                             "for " + type + " already exists");
        }

        return getId(oldname).addCallbackDeferring(new Callback<Deferred<Void>, LabelId>() {
          @Override
          public Deferred<Void> call(final LabelId old_uid) {
            tsdb_store.allocateUID(newname, old_uid, type);

            // Update cache.
            addIdToCache(newname, old_uid);  // add     new name -> ID
            id_cache.put(old_uid, newname);  // update  ID -> new name
            name_cache.remove(oldname);      // remove  old name -> ID

            // Delete the old forward mapping.
            return tsdb_store.deleteUID(oldname, type);
          }
        });
      }
    });
  }

  private Deferred<Boolean> checkUidExists(String newname) {
    class NoId implements Callback<Boolean, LabelId> {
      @Override
      public Boolean call(LabelId uid) throws Exception {
        return uid != null;
      }
    }

    class NoSuchId implements Callback<Object, Exception> {
      @Nullable
      @Override
      public Object call(Exception e) throws Exception {
        return null;
      }
    }

    return getId(newname).addCallbacks(new NoId(), new NoSuchId());
  }

  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", type)
        .toString();
  }
}
