package net.opentsdb.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static net.opentsdb.stats.Metrics.name;

import net.opentsdb.search.IdChangeIndexerListener;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.stats.StopTimerCallback;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdLookupStrategy;
import net.opentsdb.uid.IdLookupStrategy.WildcardIdLookupStrategy;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.StaticTimeseriesId;
import net.opentsdb.uid.TimeseriesId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.uid.callbacks.StripedToMap;
import net.opentsdb.utils.Pair;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class IdClient {
  /** Unique IDs for the metric names. */
  final UniqueId metrics;
  /** Unique IDs for the tag names. */
  final UniqueId tagKeys;
  /** Unique IDs for the tag values. */
  final UniqueId tagValues;

  private final TsdbStore store;

  private final IdLookupStrategy tagkLookupStrategy;
  private final IdLookupStrategy tagvLookupStrategy;
  private final IdLookupStrategy metricLookupStrategy;

  private final SearchPlugin searchPlugin;

  private final Timer tsuidQueryTimer;

  @Inject
  public IdClient(final TsdbStore store,
                  final Config config,
                  final MetricRegistry metricsRegistry,
                  final EventBus idEventBus,
                  final SearchPlugin searchPlugin) {
    checkNotNull(config);

    this.store = checkNotNull(store);
    this.searchPlugin = checkNotNull(searchPlugin);

    tagkLookupStrategy = lookupStrategy(
        config.getBoolean("tsd.core.auto_create_tagks"));
    tagvLookupStrategy = lookupStrategy(
        config.getBoolean("tsd.core.auto_create_tagvs"));
    metricLookupStrategy = lookupStrategy(
        config.getBoolean("tsd.core.auto_create_metrics"));

    metrics = new UniqueId(store, UniqueIdType.METRIC, metricsRegistry, idEventBus);
    tagKeys = new UniqueId(store, UniqueIdType.TAGK, metricsRegistry, idEventBus);
    tagValues = new UniqueId(store, UniqueIdType.TAGV, metricsRegistry, idEventBus);

    tsuidQueryTimer = metricsRegistry.timer(name("tsuid.query-time"));

    // Notify the search plugin about new and deleted labels
    idEventBus.register(new IdChangeIndexerListener(store, searchPlugin));
  }

  /**
   * Ensures that a given string is a valid metric name or tag name/value.
   *
   * @param what A human readable description of what's being validated.
   * @param s The string to validate.
   * @throws IllegalArgumentException if the string isn't valid.
   */
  public static void validateUidName(final String what, final String s) {
    if (s == null) {
      throw new IllegalArgumentException("Invalid " + what + ": null");
    }
    final int n = s.length();
    for (int i = 0; i < n; i++) {
      final char c = s.charAt(i);
      if (!(('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
            || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.'
            || c == '/' || Character.isLetter(c))) {
        throw new IllegalArgumentException("Invalid " + what
                                           + " (\"" + s + "\"): illegal character: " + c);
      }
    }
  }

  /**
   * Get a fitting {@link net.opentsdb.uid.IdLookupStrategy} based on whether IDs should be created
   * if they exist or not.
   *
   * @param shouldCreate Whether the returned lookup strategy should create missing IDs or not
   * @return A fitting instantiated {@link net.opentsdb.uid.IdLookupStrategy}
   */
  private IdLookupStrategy lookupStrategy(final boolean shouldCreate) {
    if (shouldCreate) {
      return IdLookupStrategy.CreatingIdLookupStrategy.instance;
    }

    return IdLookupStrategy.SimpleIdLookupStrategy.instance;
  }

  /**
   * Get the IDs for all tag keys and tag values in the provided {@link java.util.Map} using the
   * provided tag key and tag value {@link net.opentsdb.uid.IdLookupStrategy}. The returned value is
   * a future that on completion contains a list of striped IDs with the tag key ID on odd indexes
   * and tag value IDs on even indexes.
   *
   * @param tags The names for which to lookup the IDs for
   * @param tagKeyStrategy The strategy to use for looking up tag keys
   * @param tagValueStrategy The strategy to use for looking up tag values
   * @return A future that on completion contains a striped list of all IDs
   */
  @Nonnull
  private ListenableFuture<List<LabelId>> getTagIds(final Map<String, String> tags,
                                                    final IdLookupStrategy tagKeyStrategy,
                                                    final IdLookupStrategy tagValueStrategy) {
    final ImmutableList.Builder<ListenableFuture<LabelId>> tagIds = ImmutableList.builder();

    // For each tag, start resolving the tag name and the tag value.
    for (final Map.Entry<String, String> entry : tags.entrySet()) {
      tagIds.add(tagKeyStrategy.getId(tagKeys, entry.getKey()));
      tagIds.add(tagValueStrategy.getId(tagValues, entry.getValue()));
    }

    return allAsList(tagIds.build());
  }

  /**
   * Resolve the names behind all the {@link LabelId}s in the provided striped list.
   *
   * @param tags The IDs of the tag keys and values to resolve
   * @return A map with the names of the tags
   * @throws NoSuchUniqueId
   */
  @Nonnull
  public ListenableFuture<Map<String, String>> getTagNames(final List<LabelId> tags)
      throws NoSuchUniqueId {
    final List<ListenableFuture<String>> futures = new ArrayList<>(tags.size());

    final Iterator<LabelId> iterator = tags.iterator();
    while (iterator.hasNext()) {
      futures.add(tagKeys.getName(iterator.next()));
      futures.add(tagValues.getName(iterator.next()));
    }

    return transform(allAsList(futures), new StripedToMap<String>());
  }

  /**
   * Given an {@link net.opentsdb.uid.IdQuery} instance this method will perform a search using the
   * configured {@link net.opentsdb.search.SearchPlugin}.
   *
   * @param query The query specifying the search parameters.
   * @return A future that on completion will contain the result of the query.
   */
  public ListenableFuture<List<IdentifierDecorator>> suggest(final IdQuery query) {
    return searchPlugin.executeIdQuery(query);
  }

  UniqueId uniqueIdInstanceForType(UniqueIdType type) {
    switch (type) {
      case METRIC:
        return metrics;
      case TAGK:
        return tagKeys;
      case TAGV:
        return tagValues;
      default:
        throw new IllegalArgumentException(type + " is unknown");
    }
  }

  /**
   * Discards all in-memory caches.
   *
   * @since 1.1
   */
  public void dropCaches() {
    metrics.dropCaches();
    tagKeys.dropCaches();
    tagValues.dropCaches();
  }

  /**
   * Attempts to assign a UID to a name for the given type Used by the UniqueIdRpc call to generate
   * IDs for new metrics, tagks or tagvs. The name must pass validation and if it's already assigned
   * a UID, this method will throw an error with the proper UID. Otherwise if it can create the UID,
   * it will be returned
   *
   * @param type The type of uid to assign, metric, tagk or tagv
   * @param name The name of the uid object
   * @return A byte array with the UID if the assignment was successful
   * @throws IllegalArgumentException if the name is invalid or it already exists
   * @since 2.0
   */
  @Nonnull
  public LabelId assignUid(final UniqueIdType type,
                           final String name) {

    validateUidName(type.toString(), name);
    UniqueId instance = uniqueIdInstanceForType(type);

    try {
      try {
        final LabelId uid = instance.getId(name).get();
        throw new IllegalArgumentException("Name already exists with UID: " + uid);
      } catch (NoSuchUniqueName nsue) {
        return instance.createId(name).get();
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * TODO. This does not exactly mirror what assignUid does. We should merge the two.
   */
  public ListenableFuture<LabelId> createId(final UniqueIdType type, final String name) {
    validateUidName(type.toString(), name);
    UniqueId instance = uniqueIdInstanceForType(type);
    return instance.createId(name);
  }

  /**
   * Attempts to find the name for a unique identifier given a type
   *
   * @param type The type of UID
   * @param uid The UID to search for
   * @return The name of the UID object if found
   * @throws IllegalArgumentException if the type is not valid
   * @throws net.opentsdb.uid.NoSuchUniqueId if the UID was not found
   * @since 2.0
   */
  @Nonnull
  public ListenableFuture<String> getUidName(final UniqueIdType type,
                                             final LabelId uid) {
    checkNotNull(uid, "Missing UID");
    UniqueId uniqueId = uniqueIdInstanceForType(type);
    return uniqueId.getName(uid);
  }

  /**
   * Attempts to find the UID matching a given name
   *
   * @param type The type of UID
   * @param name The name to search for
   * @throws IllegalArgumentException if the type is not valid
   * @since 2.0
   */
  @Nonnull
  public ListenableFuture<LabelId> getUID(final UniqueIdType type,
                                          final String name) {
    checkArgument(!Strings.isNullOrEmpty(name), "Missing UID name");
    UniqueId uniqueId = uniqueIdInstanceForType(type);
    return uniqueId.getId(name);
  }

  /**
   * Returns a initialized TSUID for this metric and these tags.
   *
   * @param metric The metric to use in the TSUID
   * @param tags The string tags to use in the TSUID
   * @since 2.0
   */
  ListenableFuture<TimeseriesId> getTSUID(final String metric,
                                          final Map<String, String> tags) {
    // Lookup or create the metric ID.
    final ListenableFuture<LabelId> metric_id = metricLookupStrategy.getId(metrics, metric);

    // Copy the metric ID at the beginning of the row key.
    class CopyMetricInRowKeyCB implements Function<LabelId, TimeseriesId> {
      private final List<LabelId> tagIds;

      public CopyMetricInRowKeyCB(final List<LabelId> tagIds) {
        this.tagIds = tagIds;
      }

      @Override
      public TimeseriesId apply(@Nullable final LabelId metricid) {
        return new StaticTimeseriesId(metricid, tagIds);
      }
    }

    // Copy the tag IDs in the row key.
    class CopyTagsInRowKeyCB implements AsyncFunction<List<LabelId>, TimeseriesId> {
      @Override
      public ListenableFuture<TimeseriesId> apply(final List<LabelId> tags) {
        // Once we've resolved all the tags, schedule the copy of the metric
        // ID and return the row key we produced.
        return transform(metric_id, new CopyMetricInRowKeyCB(tags));
      }
    }

    // Kick off the resolution of all tags.
    return transform(getTagIds(tags, tagkLookupStrategy, tagvLookupStrategy),
        new CopyTagsInRowKeyCB());
  }

  /**
   * Lookup time series related to a metric, tagk, tagv or any combination thereof.
   *
   * <p>When dealing with tags, we can lookup on tagks, tagvs or pairs. Thus: tagk, null  <- lookup
   * all series with a tagk tagk, tagv  <- lookup all series with a tag pair null, tagv  <- lookup
   * all series with a tag value somewhere
   *
   * <p>The user can supply multiple tags in a query so the logic is a little goofy but here it is:
   * - Different tagks are AND'd, e.g. given "host=web01 dc=lga" we will lookup series that contain
   * both of those tag pairs. Also when given "host= dc=" then we lookup series with both tag keys
   * regardless of their values. - Tagks without a tagv will override tag pairs. E.g. "host=web01
   * host=" will return all series with the "host" tagk. - Tagvs without a tagk are OR'd. Given
   * "=lga =phx" the lookup will fetch anything with either "lga" or "phx" as the value for a pair.
   * When combined with a tagk, e.g. "host=web01 =lga" then it will return any series with the tag
   * pair AND any tag with the "lga" value.
   */
  public ListenableFuture<List<byte[]>> executeTimeSeriesQuery(final SearchQuery query) {
    final Timer.Context timerContext = tsuidQueryTimer.time();

    ListenableFuture<List<byte[]>> tsuids = transform(resolve(query),
        new AsyncFunction<ResolvedSearchQuery, List<byte[]>>() {
          @Override
          public ListenableFuture<List<byte[]>> apply(final ResolvedSearchQuery resolvedQuery)
              throws Exception {
            return store.executeTimeSeriesQuery(resolvedQuery);
          }
        });

    StopTimerCallback.stopOn(timerContext, tsuids);

    return tsuids;
  }

  /**
   * Resolve the string representation of a search query to an ID representation.
   */
  ListenableFuture<ResolvedSearchQuery> resolve(final SearchQuery query) {
    final IdLookupStrategy lookupStrategy = WildcardIdLookupStrategy.instance;
    final ListenableFuture<LabelId> metric = lookupStrategy.getId(metrics, query.getMetric());
    final ListenableFuture<SortedSet<Pair<LabelId, LabelId>>> tags = resolveTags(query.getTags());

    return transform(metric, new AsyncFunction<LabelId, ResolvedSearchQuery>() {
      @Override
      public ListenableFuture<ResolvedSearchQuery> apply(final LabelId metricId) throws Exception {
        return transform(tags,
            new Function<SortedSet<Pair<LabelId, LabelId>>, ResolvedSearchQuery>() {
              @Nullable
              @Override
              public ResolvedSearchQuery apply(@Nullable final SortedSet<Pair<LabelId, LabelId>> tagIds) {
                return new ResolvedSearchQuery(metricId, tagIds);
              }
            });
      }
    });
  }

  private ListenableFuture<SortedSet<Pair<LabelId, LabelId>>> resolveTags(final List<Pair<String, String>> tags) {
    if (tags != null && !tags.isEmpty()) {
      final IdLookupStrategy lookupStrategy = WildcardIdLookupStrategy.instance;
      final List<ListenableFuture<Pair<LabelId, LabelId>>> pairs = new ArrayList<>(tags.size());

      for (Pair<String, String> tag : tags) {
        final ListenableFuture<LabelId> tagk = lookupStrategy.getId(tagKeys, tag.getKey());
        final ListenableFuture<LabelId> tagv = lookupStrategy.getId(tagValues, tag.getValue());


        pairs.add(transform(tagk, new TagKeyResolvedFunction(tagv)));
      }

      return transform(allAsList(pairs), new TagSortCallback());
    }

    SortedSet<Pair<LabelId, LabelId>> of = ImmutableSortedSet.of();
    return Futures.immediateFuture(of);
  }

  private static class TagSortCallback
      implements Function<List<Pair<LabelId, LabelId>>, SortedSet<Pair<LabelId, LabelId>>> {
    @Nullable
    @Override
    public SortedSet<Pair<LabelId, LabelId>> apply(@Nullable final List<Pair<LabelId, LabelId>> tags) {
      return ImmutableSortedSet.copyOf(tags);
    }
  }

  private static class TagKeyResolvedFunction
      implements AsyncFunction<LabelId, Pair<LabelId, LabelId>> {
    private final ListenableFuture<LabelId> tagv;

    public TagKeyResolvedFunction(final ListenableFuture<LabelId> tagv) {
      this.tagv = tagv;
    }

    @Nullable
    @Override
    public ListenableFuture<Pair<LabelId, LabelId>> apply(@Nullable final LabelId tagkId) {
      return transform(tagv, new TagValueResolvedCallback(tagkId));
    }
  }

  private static class TagValueResolvedCallback
      implements Function<LabelId, Pair<LabelId, LabelId>> {
    private final LabelId tagkId;

    public TagValueResolvedCallback(final LabelId tagkId) {
      this.tagkId = tagkId;
    }

    @Nullable
    @Override
    public Pair<LabelId, LabelId> apply(@Nullable final LabelId tagvId) {
      return Pair.create(tagkId, tagvId);
    }
  }
}
