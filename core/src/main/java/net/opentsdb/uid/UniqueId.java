package net.opentsdb.uid;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.transform;
import static net.opentsdb.stats.Metrics.name;
import static net.opentsdb.stats.Metrics.tag;

import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.TsdbStore;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a table of Unique IDs, manages the lookup and creation of IDs.
 *
 * <p>Don't attempt to use {@code equals()} or {@code hashCode()} on this class.
 */
public class UniqueId {
  private static final Logger LOG = LoggerFactory.getLogger(UniqueId.class);

  /** The TsdbStore to use. */
  private final TsdbStore store;

  /** The type of UID represented by this cache. */
  private final UniqueIdType type;

  /** Cache for forward mappings (name to ID). */
  private final ConcurrentHashMap<String, LabelId> nameCache = new ConcurrentHashMap<>();

  /** Cache for backward mappings (ID to name). */
  private final ConcurrentHashMap<LabelId, String> idCache = new ConcurrentHashMap<>();

  /** Map of pending UID assignments. */
  private final HashMap<String, ListenableFuture<LabelId>> pendingAssignments = new HashMap<>();

  /** Number of times we avoided reading from TsdbStore thanks to the cache. */
  private final Counter cacheHits;
  /** Number of times we had to read from TsdbStore and populate the cache. */
  private final Counter cacheMisses;

  /**
   * The event bus to which the id changes done by this instance will be published.
   */
  private final EventBus idEventBus;

  /**
   * Constructor.
   *
   * @param store The TsdbStore to use.
   * @param type The type of UIDs this instance represents
   * @param metrics
   * @param idEventBus
   */
  public UniqueId(final TsdbStore store,
                  final UniqueIdType type,
                  final MetricRegistry metrics,
                  final EventBus idEventBus) {
    this.store = checkNotNull(store);
    this.type = checkNotNull(type);
    this.idEventBus = checkNotNull(idEventBus);

    cacheHits = new Counter();
    cacheMisses = new Counter();
    registerMetrics(metrics);

  }

  private void registerMetrics(final MetricRegistry registry) {
    Metrics.Tag typeTag = tag("kind", type.toValue());
    registry.register(name("uid.cache-hit", typeTag), cacheHits);
    registry.register(name("uid.cache-miss", typeTag), cacheMisses);

    registry.register(name("uid.cache-size", typeTag), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return nameCache.size() + idCache.size();
      }
    });
  }

  /**
   * Causes this instance to discard all its in-memory caches.
   *
   * @since 1.1
   */
  public void dropCaches() {
    nameCache.clear();
    idCache.clear();
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
  public ListenableFuture<String> getName(final LabelId id) {
    final String name = getNameFromCache(id);
    if (name != null) {
      cacheHits.inc();
      return Futures.immediateFuture(name);
    }
    cacheMisses.inc();
    class GetNameCB implements Function<Optional<String>, String> {
      @Nullable
      @Override
      public String apply(@Nullable final Optional<String> name) {
        checkNotNull(name);

        if (name.isPresent()) {
          addNameToCache(id, name.get());
          addIdToCache(name.get(), id);
          return name.get();
        }

        throw new NoSuchUniqueId(type, id);
      }
    }

    return transform(store.getName(id, type), new GetNameCB());
  }

  @Nullable
  private String getNameFromCache(final LabelId id) {
    return idCache.get(id);
  }

  private void addNameToCache(final LabelId id,
                              final String name) {
    String found = idCache.get(id);
    if (found == null) {
      found = idCache.putIfAbsent(id, name);
    }
    if (found != null && !found.equals(name)) {
      throw new IllegalStateException("id=" + id + " => name="
                                      + name + ", already mapped to " + found);
    }
  }

  @Nonnull
  public ListenableFuture<LabelId> getId(final String name) {
    final LabelId id = getIdFromCache(name);
    if (id != null) {
      cacheHits.inc();
      return Futures.immediateFuture(id);
    }
    cacheMisses.inc();
    class GetIdCB implements Function<Optional<LabelId>, LabelId> {
      @Nullable
      @Override
      public LabelId apply(@Nullable final Optional<LabelId> id) {
        checkNotNull(id);

        if (id.isPresent()) {
          addIdToCache(name, id.get());
          addNameToCache(id.get(), name);
          return id.get();
        }

        throw new NoSuchUniqueName(type.toValue(), name);
      }
    }

    return transform(store.getId(name, type), new GetIdCB());
  }

  @Nullable
  private LabelId getIdFromCache(final String name) {
    return nameCache.get(name);
  }

  private void addIdToCache(final String name,
                            final LabelId id) {
    LabelId found = nameCache.get(name);
    if (found == null) {
      found = nameCache.putIfAbsent(name, id);
    }
    if (found != null && !found.equals(id)) {
      throw new IllegalStateException("name=" + name + " => id="
                                      + id + ", already mapped to " + found);
    }
  }

  /** Adds the bidirectional mapping in the cache. */
  private void cacheMapping(final String name,
                            final LabelId id) {
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
  public ListenableFuture<LabelId> createId(final String name) {
    ListenableFuture<LabelId> assignment;
    synchronized (pendingAssignments) {
      assignment = pendingAssignments.get(name);
      if (assignment == null) {
        // to prevent UID leaks that can be caused when multiple time
        // series for the same metric or tags arrive, we need to write a
        // deferred to the pending map as quickly as possible. Then we can
        // start the assignment process after we've stashed the deferred
        // and released the lock
        assignment = SettableFuture.create();
        pendingAssignments.put(name, assignment);
      } else {
        LOG.info("Already waiting for UID assignment: {}", name);
        return assignment;
      }
    }

    // start the assignment dance after stashing the deferred
    ListenableFuture<LabelId> uid = store.allocateUID(name, type);

    return transform(uid, new Function<LabelId, LabelId>() {
      @Nullable
      @Override
      public LabelId apply(@Nullable final LabelId uid) {
        checkNotNull(uid);

        cacheMapping(name, uid);

        LOG.info("Completed pending assignment for: {}", name);
        synchronized (pendingAssignments) {
          pendingAssignments.remove(name);
        }

        idEventBus.post(new LabelCreatedEvent(uid, name, type));

        return uid;
      }
    });
  }

  /**
   * Reassigns the UID to a different name (non-atomic).
   *
   * <p>Whatever was the UID of {@code oldname} will be given to {@code newname}. {@code oldname}
   * will no longer be assigned a UID.
   *
   * <p>Beware that the assignment change is <b>not atommic</b>.  If two threads or processes
   * attempt to rename the same UID differently, the result is unspecified and might even be
   * inconsistent. This API is only here for administrative purposes, not for normal programmatic
   * interactions.
   *
   * @param oldname The old name to rename.
   * @param newname The new name.
   * @throws NoSuchUniqueName if {@code oldname} wasn't assigned.
   * @throws IllegalArgumentException if {@code newname} was already assigned.
   */
  public ListenableFuture<Void> rename(final String oldname, final String newname) {
    return transform(checkUidExists(newname), new AsyncFunction<Boolean, Void>() {
      @Override
      public ListenableFuture<Void> apply(final Boolean exists) throws Exception {
        if (exists) {
          throw new IllegalArgumentException("An UID with name " + newname + ' '
                                             + "for " + type + " already exists");
        }

        return transform(getId(oldname), new AsyncFunction<LabelId, Void>() {
          @Override
          public ListenableFuture<Void> apply(final LabelId oldUid) throws Exception {
            store.allocateUID(newname, oldUid, type);

            // Update cache.
            addIdToCache(newname, oldUid);  // add     new name -> ID
            idCache.put(oldUid, newname);   // update  ID -> new name
            nameCache.remove(oldname);      // remove  old name -> ID

            // Delete the old forward mapping.
            return store.deleteUID(oldname, type);
          }
        });
      }
    });
  }

  private ListenableFuture<Boolean> checkUidExists(String name) {
    final SettableFuture<Boolean> exists = SettableFuture.create();

    Futures.addCallback(getId(name), new FutureCallback<LabelId>() {
      @Override
      public void onSuccess(@Nullable final LabelId result) {
        exists.set(result != null);
      }

      @Override
      public void onFailure(final Throwable throwable) {
        exists.set(false);
      }
    });

    return exists;
  }

  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", type)
        .toString();
  }
}
