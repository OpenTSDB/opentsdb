package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.CreatingIdLookupStrategy;
import net.opentsdb.uid.IdLookupStrategy;
import net.opentsdb.uid.SimpleIdLookupStrategy;
import net.opentsdb.uid.StaticTimeseriesId;
import net.opentsdb.uid.TimeseriesId;
import net.opentsdb.uid.callbacks.StripedTagIdsToList;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdUtils;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdType;
import com.typesafe.config.Config;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.utils.ByteArrayPair;
import net.opentsdb.utils.Pair;
import net.opentsdb.stats.StopTimerCallback;
import org.hbase.async.Bytes;

import javax.inject.Inject;
import javax.inject.Singleton;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static net.opentsdb.core.StringCoder.toBytes;
import static net.opentsdb.stats.Metrics.name;

@Singleton
public class UniqueIdClient {
  private static final SortResolvedTagsCB SORT_CB = new SortResolvedTagsCB();

  private final Config config;
  private final TsdbStore tsdbStore;

  /** Unique IDs for the metric names. */
  final UniqueId metrics;
  /** Unique IDs for the tag names. */
  final UniqueId tag_names;
  /** Unique IDs for the tag values. */
  final UniqueId tag_values;

  /** Name of the table in which UID information is stored. */
  final byte[] uidtable;

  private final SearchPlugin searchPlugin;

  private final Timer tsuidQueryTimer;

  @Inject
  public UniqueIdClient(final TsdbStore tsdbStore,
                        final Config config,
                        final MetricRegistry metricsRegistry,
                        final EventBus idEventBus,
                        final SearchPlugin searchPlugin) {
    this.tsdbStore = checkNotNull(tsdbStore);
    this.config = checkNotNull(config);

    this.searchPlugin = checkNotNull(searchPlugin);

    uidtable = toBytes(config.getString("tsd.storage.hbase.uid_table"));

    metrics = new UniqueId(tsdbStore, UniqueIdType.METRIC, metricsRegistry, idEventBus);
    tag_names = new UniqueId(tsdbStore, UniqueIdType.TAGK, metricsRegistry, idEventBus);
    tag_values = new UniqueId(tsdbStore, UniqueIdType.TAGV, metricsRegistry, idEventBus);

    if (config.getBoolean("tsd.core.preload_uid_cache.enabled")) {
      final Map<UniqueIdType, UniqueId> uid_cache_map = new EnumMap<UniqueIdType, UniqueId>(UniqueIdType.class);
      uid_cache_map.put(UniqueIdType.METRIC, metrics);
      uid_cache_map.put(UniqueIdType.TAGK, tag_names);
      uid_cache_map.put(UniqueIdType.TAGV, tag_values);
      UniqueId.preloadUidCache(config, tsdbStore, uid_cache_map);
    }

    tsuidQueryTimer = metricsRegistry.timer(name("tsuid.query-time"));
  }

  /**
   * Ensures that a given string is a valid metric name or tag name/value.
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
   * Resolves all the tags (name=value) into the a sorted byte arrays.
   * This function is the opposite of {@link #getTagNames(java.util.List)}.
   * @param tags The tags to resolve.
   * @return an array of sorted tags (tag id, tag name).
   * @throws net.opentsdb.uid.NoSuchUniqueName if one of the elements in the map contained an
   * unknown tag name or tag value.
   */
  public Deferred<ArrayList<byte[]>> getAllTags(final Map<String, String> tags) {
    IdLookupStrategy lookupStrategy = new SimpleIdLookupStrategy();
    return getTagIds(tags, lookupStrategy, lookupStrategy)
        .addCallback(new StripedTagIdsToList())
        .addCallback(SORT_CB);
  }

  /**
   * Resolves (and creates, if necessary) all the tags (name=value) into the a
   * sorted byte arrays.
   * @param tags The tags to resolve.  If a new tag name or tag value is
   * seen, it will be assigned an ID.
   * @return an array of sorted tags (tag id, tag name).
   * @since 2.0
   */
  Deferred<ArrayList<byte[]>> getOrCreateAllTags(final Map<String, String> tags) {
    final IdLookupStrategy tagkLookupStrategy = lookupStrategy(
        config.getBoolean("tsd.core.auto_create_tagks"));
    final IdLookupStrategy tagvLookupStrategy = lookupStrategy(
        config.getBoolean("tsd.core.auto_create_tagvs"));

    return getTagIds(tags, tagkLookupStrategy, tagvLookupStrategy)
        .addCallback(new StripedTagIdsToList())
        .addCallback(SORT_CB);
  }

  /**
   * Get a fitting {@link net.opentsdb.uid.IdLookupStrategy} based on whether
   * IDs should be created if they exist or not.
   *
   * @param shouldCreate Whether the returned lookup strategy should create
   *                     missing IDs or not
   * @return A fitting instantiated {@link net.opentsdb.uid.IdLookupStrategy}
   */
  private IdLookupStrategy lookupStrategy(final boolean shouldCreate) {
    if (shouldCreate) {
      return new CreatingIdLookupStrategy();
    }

    return new SimpleIdLookupStrategy();
  }

  /**
   * Get the IDs for all tag keys and tag values in the provided {@link
   * java.util.Map} using the provided tag key and tag value {@link
   * net.opentsdb.uid.IdLookupStrategy}. The returned value is a deferred that
   * contains a list of striped IDs with the tag key ID on odd indexes and tag
   * value IDs on even indexes.
   *
   * @param tags         The names for which to lookup the IDs for
   * @param tagkStrategy The strategy to use for looking up tag keys
   * @param tagvStrategy The strategy to use for looking up tag values
   * @return A Deferred that contains a striped list of all IDs
   */
  private Deferred<ArrayList<byte[]>> getTagIds(final Map<String, String> tags,
                                                final IdLookupStrategy tagkStrategy,
                                                final IdLookupStrategy tagvStrategy) {
    final ImmutableList.Builder<Deferred<byte[]>> tag_ids = ImmutableList.builder();

    // For each tag, start resolving the tag name and the tag value.
    for (final Map.Entry<String, String> entry : tags.entrySet()) {
      tag_ids.add(tagkStrategy.getId(tag_names, entry.getKey()));
      tag_ids.add(tagvStrategy.getId(tag_values, entry.getValue()));
    }

    return Deferred.groupInOrder(tag_ids.build());
  }

  /**
   * Resolves all the tags IDs asynchronously (name followed by value) into a map.
   * This function is the opposite of {@link #getAllTags(java.util.Map)}.
   * @param tags The tag IDs to resolve.
   * @return A map mapping tag names to tag values.
   * @throws net.opentsdb.uid.NoSuchUniqueId if one of the elements in the array contained an
   * invalid ID.
   * @throws IllegalArgumentException if one of the elements in the array had
   * the wrong number of bytes.
   * @since 2.0
   */
  public Deferred<HashMap<String, String>> getTagNames(final List<byte[]> tags)
    throws NoSuchUniqueId {
    final short name_width = UniqueIdType.TAGK.width;
    final short value_width = UniqueIdType.TAGV.width;
    final short tag_bytes = (short) (name_width + value_width);
    final HashMap<String, String> result
      = new HashMap<String, String>(tags.size());
    final ArrayList<Deferred<String>> deferreds
      = new ArrayList<Deferred<String>>(tags.size());

    for (final byte[] tag : tags) {
      final byte[] tmp_name = new byte[name_width];
      final byte[] tmp_value = new byte[value_width];
      if (tag.length != tag_bytes) {
        throw new IllegalArgumentException("invalid length: " + tag.length
            + " (expected " + tag_bytes + "): " + Arrays.toString(tag));
      }
      System.arraycopy(tag, 0, tmp_name, 0, name_width);
      deferreds.add(tag_names.getName(tmp_name));
      System.arraycopy(tag, name_width, tmp_value, 0, value_width);
      deferreds.add(tag_values.getName(tmp_value));
    }

    class GroupCB implements Callback<HashMap<String, String>, ArrayList<String>> {
      @Override
      public HashMap<String, String> call(final ArrayList<String> names)
          throws Exception {
        for (int i = 0; i < names.size(); i++) {
          if (i % 2 != 0) {
            result.put(names.get(i - 1), names.get(i));
          }
        }
        return result;
      }
    }

    return Deferred.groupInOrder(deferreds).addCallback(new GroupCB());
  }

  /**
   * Given an {@link net.opentsdb.uid.IdQuery} instance this method will perform
   * a search using the configured {@link net.opentsdb.search.SearchPlugin}.
   *
   * @param query The query specifying the search parameters.
   * @return A deferred that contains the result of the query.
   */
  public Deferred<List<IdentifierDecorator>> suggest(final IdQuery query) {
    return searchPlugin.executeIdQuery(query);
  }

  UniqueId uniqueIdInstanceForType(UniqueIdType type) {
    switch (type) {
      case METRIC:
        return metrics;
      case TAGK:
        return tag_names;
      case TAGV:
        return tag_values;
      default:
        throw new IllegalArgumentException(type + " is unknown");
    }
  }

  /**
   * Discards all in-memory caches.
   * @since 1.1
   */
  public void dropCaches() {
    metrics.dropCaches();
    tag_names.dropCaches();
    tag_values.dropCaches();
  }

  /**
   * Attempts to assign a UID to a name for the given type
   * Used by the UniqueIdRpc call to generate IDs for new metrics, tagks or
   * tagvs. The name must pass validation and if it's already assigned a UID,
   * this method will throw an error with the proper UID. Otherwise if it can
   * create the UID, it will be returned
   * @param type The type of uid to assign, metric, tagk or tagv
   * @param name The name of the uid object
   * @return A byte array with the UID if the assignment was successful
   * @throws IllegalArgumentException if the name is invalid or it already
   * exists
   * @since 2.0
   */
  public byte[] assignUid(final UniqueIdType type, final String name) {
    validateUidName(type.toString(), name);
    UniqueId instance = uniqueIdInstanceForType(type);

    try {
      try {
        final byte[] uid = instance.getId(name).joinUninterruptibly();
        throw new IllegalArgumentException("Name already exists with UID: " +
                IdUtils.uidToString(uid));
      } catch (NoSuchUniqueName nsue) {
        return instance.createId(name).joinUninterruptibly();
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Attempts to find the name for a unique identifier given a type
   * @param type The type of UID
   * @param uid The UID to search for
   * @return The name of the UID object if found
   * @throws IllegalArgumentException if the type is not valid
   * @throws net.opentsdb.uid.NoSuchUniqueId if the UID was not found
   * @since 2.0
   */
  public Deferred<String> getUidName(final UniqueIdType type, final byte[] uid) {
    checkArgument(uid != null, "Missing UID");

    UniqueId uniqueId = uniqueIdInstanceForType(type);
    return uniqueId.getName(uid);
  }

  /**
   * Attempts to find the UID matching a given name
   * @param type The type of UID
   * @param name The name to search for
   * @throws IllegalArgumentException if the type is not valid
   * @since 2.0
   */
  public Deferred<byte[]> getUID(final UniqueIdType type, final String name) {
    checkArgument(!Strings.isNullOrEmpty(name), "Missing UID name");
    UniqueId uniqueId = uniqueIdInstanceForType(type);
    return uniqueId.getId(name);
  }

  /**
   * Returns a initialized TSUID for this metric and these tags.
   * @since 2.0
   * @param metric The metric to use in the TSUID
   * @param tags The string tags to use in the TSUID
   */
  Deferred<TimeseriesId> getTSUID(final String metric,
                                  final Map<String, String> tags) {
    IdLookupStrategy metricLookupStrategy = lookupStrategy(
        config.getBoolean("tsd.core.auto_create_metrics"));

    // Lookup or create the metric ID.
    final Deferred<byte[]> metric_id = metricLookupStrategy.getId(metrics, metric);

    // Copy the metric ID at the beginning of the row key.
    class CopyMetricInRowKeyCB implements Callback<TimeseriesId, byte[]> {
      private final List<byte[]> tagIds;

      public CopyMetricInRowKeyCB(final List<byte[]> tagIds) {
        this.tagIds = tagIds;
      }

      @Override
      public TimeseriesId call(final byte[] metricid) {
        return new StaticTimeseriesId(metricid, tagIds);
      }
    }

    // Copy the tag IDs in the row key.
    class CopyTagsInRowKeyCB
      implements Callback<Deferred<TimeseriesId>, ArrayList<byte[]>> {
      @Override
      public Deferred<TimeseriesId> call(final ArrayList<byte[]> tags) {
        // Once we've resolved all the tags, schedule the copy of the metric
        // ID and return the row key we produced.
        return metric_id
                .addCallback(new CopyMetricInRowKeyCB(tags));
      }
    }

    // Kick off the resolution of all tags.
    return getOrCreateAllTags(tags)
      .addCallbackDeferring(new CopyTagsInRowKeyCB());
  }

  /**
   * Sorts a list of tags.
   * Each entry in the list expected to be a byte array that contains the tag
   * name UID followed by the tag value UID.
   */
  private static class SortResolvedTagsCB
    implements Callback<ArrayList<byte[]>, ArrayList<byte[]>> {
    @Override
    public ArrayList<byte[]> call(final ArrayList<byte[]> tags) {
      // Now sort the tags.
      Collections.sort(tags, Bytes.MEMCMP);
      return tags;
    }
  }

  /**
   * Lookup time series related to a metric, tagk, tagv or any combination
   * thereof.
   *
   * When dealing with tags, we can lookup on tagks, tagvs or pairs. Thus: tagk,
   * null  <- lookup all series with a tagk tagk, tagv  <- lookup all series
   * with a tag pair null, tagv  <- lookup all series with a tag value
   * somewhere
   *
   * The user can supply multiple tags in a query so the logic is a little goofy
   * but here it is: - Different tagks are AND'd, e.g. given "host=web01 dc=lga"
   * we will lookup series that contain both of those tag pairs. Also when given
   * "host= dc=" then we lookup series with both tag keys regardless of their
   * values. - Tagks without a tagv will override tag pairs. E.g. "host=web01
   * host=" will return all series with the "host" tagk. - Tagvs without a tagk
   * are OR'd. Given "=lga =phx" the lookup will fetch anything with either
   * "lga" or "phx" as the value for a pair. When combined with a tagk, e.g.
   * "host=web01 =lga" then it will return any series with the tag pair AND any
   * tag with the "lga" value.
   */
  public Deferred<List<byte[]>> executeTimeSeriesQuery(final SearchQuery query) {
    final Timer.Context timerContext = tsuidQueryTimer.time();

    Deferred<List<byte[]>> tsuids = resolve(query).addCallbackDeferring(
        new Callback<Deferred<List<byte[]>>, ResolvedSearchQuery>() {
          @Override
          public Deferred<List<byte[]>> call(final ResolvedSearchQuery arg) {
            return tsdbStore.executeTimeSeriesQuery(arg);
          }
        });

    return StopTimerCallback.stopOn(timerContext, tsuids);
  }

  /**
   * Resolve the string representation of a search query to an ID representation.
   */
  Deferred<ResolvedSearchQuery> resolve(final SearchQuery query) {
    final Deferred<byte[]> metric = metrics.resolveId(query.getMetric());
    final Deferred<SortedSet<ByteArrayPair>> tags = resolveTags(query.getTags());

    return metric.addBothDeferring(new Callback<Deferred<ResolvedSearchQuery>, byte[]>() {
      @Override
      public Deferred<ResolvedSearchQuery> call(final byte[] metric_id) {
        return tags.addCallback(new Callback<ResolvedSearchQuery, SortedSet<ByteArrayPair>>() {
          @Override
          public ResolvedSearchQuery call(final SortedSet<ByteArrayPair> tag_ids) {
            return new ResolvedSearchQuery(metric_id, tag_ids);
          }
        });
      }
    });
  }

  private Deferred<SortedSet<ByteArrayPair>> resolveTags(final List<Pair<String, String>> tags) {
    if (tags != null && !tags.isEmpty()) {

      final List<Deferred<ByteArrayPair>> pairs =
          Lists.newArrayListWithCapacity(tags.size());

      for (Pair<String, String> tag : tags) {
        final Deferred<byte[]> tagk = tag_names.resolveId(tag.getKey());
        final Deferred<byte[]> tagv = tag_values.resolveId(tag.getValue());

        pairs.add(tagk.addCallbackDeferring(new TagKeyResolvedCallback(tagv)));
      }

      return Deferred.group(pairs).addCallback(new TagSortCallback());
    }

    SortedSet<ByteArrayPair> of = ImmutableSortedSet.of();
    return Deferred.fromResult(of);
  }

  private static class TagSortCallback
      implements Callback<SortedSet<ByteArrayPair>, ArrayList<ByteArrayPair>> {
    @Override
    public SortedSet<ByteArrayPair> call(final ArrayList<ByteArrayPair> tags) {
      // remember, tagks are sorted in the row key so we need to supply a sorted
      // regex or matching will fail.
      return ImmutableSortedSet.copyOf(tags);
    }
  }

  private static class TagKeyResolvedCallback
      implements Callback<Deferred<ByteArrayPair>, byte[]> {
    private final Deferred<byte[]> tagv;

    public TagKeyResolvedCallback(final Deferred<byte[]> tagv) {
      this.tagv = tagv;
    }

    @Override
    public Deferred<ByteArrayPair> call(final byte[] tagk_id) {
      return tagv.addCallback(new TagValueResolvedCallback(tagk_id));
    }
  }

  private static class TagValueResolvedCallback
      implements Callback<ByteArrayPair, byte[]> {
    private final byte[] tagk_id;

    public TagValueResolvedCallback(final byte[] tagk_id) {
      this.tagk_id = tagk_id;
    }

    @Override
    public ByteArrayPair call(final byte[] tagv_id) {
      return new ByteArrayPair(tagk_id, tagv_id);
    }
  }
}
