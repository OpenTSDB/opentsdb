package net.opentsdb.storage.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.Const;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.StringCoder;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.cassandra.functions.FirstOrAbsentFunction;
import net.opentsdb.storage.cassandra.functions.IsEmptyFunction;
import net.opentsdb.time.JdkTimeProvider;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.uid.IdException;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdUtils;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.UniqueIdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.transform;
import static net.opentsdb.storage.cassandra.CassandraConst.CHARSET;
import static net.opentsdb.storage.cassandra.MoreFutures.wrap;

/**
 * The CassandraStore that implements the client interface required by TSDB.
 */
public class CassandraStore implements TsdbStore {
  /**
   * The logger used for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(CassandraStore.class);

  /**
   * The Cassandra cluster that we are connected to.
   */
  private final Cluster cluster;
  /**
   * The current Cassandra session.
   */
  private final Session session;

  /**
   * A time provider to tell the current time.
   */
  private final JdkTimeProvider timeProvider;

  /**
   * The statement used by the {@link #addPoint} method.
   */
  private PreparedStatement add_point_statement;
  private PreparedStatement insert_tags_statement;
  /**
   * The statement used by the {@link #allocateUID} method.
   */
  private PreparedStatement createIdStatement;
  /**
   * Used for {@link #allocateUID}, the one that does rename.
   */
  private PreparedStatement update_uid_name_statement;
  private PreparedStatement update_name_uid_statement;
  /**
   * The statement used when trying to get name or id.
   */
  private PreparedStatement get_name_statement;
  private PreparedStatement get_id_statement;


  /**
   * If you need a CassandraStore, try to change the config file and use the
   * {@link net.opentsdb.storage.StoreModule#get()}.
   *
   * @param cluster The configured Cassandra cluster.
   */
  public CassandraStore(final Cluster cluster) {

    this.cluster = cluster;
    this.session = cluster.connect("tsdb");

    this.timeProvider = new JdkTimeProvider();

    Metadata metadata = cluster.getMetadata();

    //Show what we connected to in the debug log
    LOG.info("Connected to cluster: {}", metadata.getClusterName());
    for (Host host : metadata.getAllHosts()) {
      LOG.info("Datacenter: {}; Host: {}; Rack: {}",
              host.getDatacenter(), host.getAddress(), host.getRack());
    }
    prepareStatements();
  }

  /**
   * In this method we prepare all the statements used for accessing Cassandra.
   */
  private void prepareStatements() {
    checkNotNull(session);
    checkNotNull(cluster);

    String CQL = "INSERT INTO \"tsdb\".\"" + Tables.DATAPOINTS + "\" (tsid, basetime, timestamp, flags, val) VALUES (?, ?, ?, ?, ?);";
    add_point_statement = session.prepare(CQL);

    CQL = "BEGIN BATCH USING TIMESTAMP ?" +
        "INSERT INTO tsdb." + Tables.ID_TO_NAME + " (id, type, ctim, name) VALUES (?, ?, ?, ?);" +
        "INSERT INTO tsdb." + Tables.NAME_TO_ID + " (name, type, ctim, id) VALUES (?, ?, ?, ?);" +
        "APPLY BATCH;";
    createIdStatement = session.prepare(CQL)
        .setConsistencyLevel(ConsistencyLevel.ALL);

    CQL = "UPDATE tsdb." + Tables.ID_TO_NAME + " SET name = ? WHERE id = ? AND type = ?;";
    update_uid_name_statement = session.prepare(CQL);

    CQL = "BEGIN BATCH " +
            "DELETE FROM tsdb." + Tables.NAME_TO_ID + " WHERE name = ? AND type= ? " +
            "INSERT INTO tsdb." + Tables.NAME_TO_ID + " (name, type, id) VALUES (?, ?, ?) " +
            "APPLY BATCH;";
    update_name_uid_statement = session.prepare(CQL);

    CQL = "SELECT * FROM tsdb." + Tables.ID_TO_NAME + " WHERE id = ? AND type = ? LIMIT 2;";
    get_name_statement = session.prepare(CQL);

    CQL = "SELECT * FROM tsdb." + Tables.NAME_TO_ID + " WHERE name = ? AND type = ? LIMIT 2;";
    get_id_statement = session.prepare(CQL);

    CQL = "INSERT INTO tsdb." + Tables.TS_INVERTED_INDEX + " (id, type, tsid) VALUES (?, ?, ?);";
    insert_tags_statement = session.prepare(CQL);
  }

  public Session getSession() {
    return session;
  }

  @Override
  public Deferred<Annotation> getAnnotation(byte[] tsuid, long start_time) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Object> flush() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public long getFlushInterval() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void setFlushInterval(short aShort) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Object> addPoint(final byte[] tsuid, final byte[] value,
                                   final long timestamp, final short flags) {
    final long base_time = buildBaseTime(timestamp);

    final ResultSetFuture future = session.executeAsync(
            add_point_statement.bind(
                    IdUtils.uidToString(tsuid),
                    base_time,
                    timestamp,
                    new Integer(flags),
                    StringCoder.fromBytes(value)));

    final byte[] metric_uid = IdUtils.getMetricFromTSUID(
        IdUtils.uidToString(tsuid));

    final List<byte[]> tags_uids = IdUtils.getTagsFromTSUID(
        IdUtils.uidToString(tsuid));

    final Deferred<Object> d = new Deferred<Object>();

    Futures.addCallback(future, new FutureCallback<ResultSet>() {
      @Override
      public void onSuccess(ResultSet rows) {
        d.callback(null);

        session.executeAsync(insert_tags_statement.bind(
                IdUtils.uidToLong(metric_uid, UniqueIdType.METRIC.width),
                UniqueIdType.METRIC.toValue(),
                IdUtils.uidToString(tsuid)));

        for (int i = 0; i < tags_uids.size(); i += 2) {
          session.executeAsync(insert_tags_statement.bind(
                  IdUtils.uidToLong(tags_uids.get(i), UniqueIdType.TAGK.width),
                  UniqueIdType.TAGK.toValue(),
                  IdUtils.uidToString(tsuid)));

          session.executeAsync(insert_tags_statement.bind(
                  IdUtils.uidToLong(tags_uids.get(i + 1), UniqueIdType.TAGV.width),
                  UniqueIdType.TAGV.toValue(),
                  IdUtils.uidToString(tsuid)));
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        d.callback(throwable);
      }
    });

    return d;
  }

  @Override
  public Deferred<Object> shutdown() {
    List<CloseFuture> close = new ArrayList<CloseFuture>();
    close.add(session.closeAsync());
    close.add(cluster.closeAsync());

    final Deferred<Object> d = new Deferred<Object>();

    Futures.addCallback(Futures.allAsList(close), new
        FutureCallback<List<Void>>() {
          @Override
          public void onSuccess(List<Void> voids) {
            d.callback(null);
          }

          @Override
          public void onFailure(Throwable throwable) {
            d.callback(throwable);
          }
        });
    return d;
  }

  @Override
  public Deferred<Optional<byte[]>> getId(final String name,
                                          final UniqueIdType type) {
    ListenableFuture<List<byte[]>> idsFuture = getIds(name, type);
    return wrap(transform(idsFuture, new FirstOrAbsentFunction<byte[]>()));
  }

  /**
   * Fetch the first two IDs that are associated with the provided name and
   * type.
   *
   * @param name The name to fetch IDs for
   * @param type The type of IDs to fetch
   * @return A future with a list of the first two found IDs
   */
  ListenableFuture<List<byte[]>> getIds(final String name,
                                        final UniqueIdType type) {
    ResultSetFuture idsFuture = session.executeAsync(
        get_id_statement.bind(name, type.toValue()));

    return transform(idsFuture, new Function<ResultSet, List<byte[]>>() {
      @Override
      public List<byte[]> apply(final ResultSet result) {
        ImmutableList.Builder<byte[]> builder = ImmutableList.builder();

        for (final Row row : result) {
          final long id = row.getLong("id");
          builder.add(Longs.toByteArray(id));
        }

        return builder.build();
      }
    });
  }

  @Override
  public Deferred<Optional<String>> getName(final byte[] id,
                                            final UniqueIdType type) {
    final long longId = Longs.fromByteArray(id);
    ListenableFuture<List<String>> namesFuture = getNames(longId, type);
    return wrap(transform(namesFuture, new FirstOrAbsentFunction<String>()));
  }

  /**
   * Fetch the first two names that are associated with the provided id and
   * type.
   *
   * @param id   The id to fetch names for
   * @param type The type of names to fetch
   * @return A future with a list of the first two found names
   */
  ListenableFuture<List<String>> getNames(final long id,
                                          final UniqueIdType type) {
    ResultSetFuture namesFuture = session.executeAsync(
        get_name_statement.bind(id, type.toValue()));

    return transform(namesFuture, new Function<ResultSet, List<String>>() {
      @Override
      public List<String> apply(final ResultSet result) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();

        for (final Row row : result) {
          final String name = row.getString("name");
          builder.add(name);
        }

        return builder.build();
      }
    });
  }

  @Override
  public Deferred<Object> add(UIDMeta meta) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Object> delete(UIDMeta meta) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<UIDMeta> getMeta(byte[] uid, String name, UniqueIdType type) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> updateMeta(UIDMeta meta, boolean overwrite) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Object> deleteUID(byte[] name, UniqueIdType type) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Check if (id, type) is available and return a future that contains a
   * boolean that will be true if the id is available or false if otherwise.
   *
   * @param id   The name to check
   * @param type The type to check
   * @return A future that contains a boolean that indicates if the id was
   * available
   */
  private ListenableFuture<Boolean> isIdAvailable(final long id,
                                                  final UniqueIdType type) {
    return transform(getNames(id, type), new IsEmptyFunction());
  }

  /**
   * Check if (name, type) is available and return a future that contains a
   * boolean that will be true if the name is available or false if otherwise.
   *
   * @param name The name to check
   * @param type The type to check
   * @return A future that contains a boolean that indicates if the name was
   * available
   */
  private ListenableFuture<Boolean> isNameAvailable(final String name,
                                                    final UniqueIdType type) {
    return transform(getIds(name, type), new IsEmptyFunction());
  }

  /**
   * Check if either of (id, type) and (name, type) are taken or if both are
   * available. If either of the combinations already are taken the returned
   * future will contain an {@link net.opentsdb.uid.IdException}.
   *
   * @param id   The id to check if it is available
   * @param name The name to check if it is available
   * @param type The type of id and name to check if it available
   * @return A future that contains an exception if either of the above
   * combinations were taken. Otherwise a future with meaningless contents will
   * be returned.
   */
  private ListenableFuture<Void> checkAvailable(final long id,
                                                final String name,
                                                final UniqueIdType type) {
    ImmutableList<ListenableFuture<Boolean>> availableList =
        ImmutableList.of(isIdAvailable(id, type), isNameAvailable(name, type));
    final ListenableFuture<List<Boolean>> availableFuture = Futures.allAsList(availableList);

    return transform(availableFuture, new AsyncFunction<List<Boolean>, Void>() {
      @Override
      public ListenableFuture<Void> apply(final List<Boolean> available) {
        // These are in the same order as they are provided in the call
        // to Futures#allAsList.
        final Boolean idAvailable = available.get(0);
        final Boolean nameAvailable = available.get(1);

        if (!idAvailable) {
          return Futures.immediateFailedFuture(
              new IdException(id, type, "Id was already taken"));
        }

        if (!nameAvailable) {
          return Futures.immediateFailedFuture(
              new IdException(name, type, "Name was already taken"));
        }

        return Futures.immediateFuture(null);
      }
    });
  }

  /**
   * Save a new identifier with the provided information in Cassandra. This will
   * not perform any checks to see if the id already exists, you are expected to
   * have done so already.
   *
   * @param id   The id to associate with the provided name
   * @param name The name to save
   * @param type The type of id to save
   * @return A future containing the newly saved identifier
   */
  private ListenableFuture<byte[]> createId(final long id,
                                            final String name,
                                            final UniqueIdType type) {
    final Date createTimestamp = timeProvider.now();
    final ResultSetFuture save = session.executeAsync(
        createIdStatement.bind(createTimestamp.getTime(),
            id, type.toValue(), createTimestamp, name,
            name, type.toValue(), createTimestamp, id));

    return transform(save, new AsyncFunction<ResultSet, byte[]>() {
      @Override
      public ListenableFuture<byte[]> apply(final ResultSet result) {
        // The Cassandra driver will have thrown an exception if the insertion
        // failed in which case we would not be here so just return the id we
        // sent to Cassandra.
        return Futures.immediateFuture(Longs.toByteArray(id));
      }
    });
  }

  /**
   * Allocate an ID for the provided (name, type). This will attempt to generate
   * an ID that is likely to be available. It will then check if this
   * information is available and finally save the information if it is. If the
   * information could be saved the ID will be returned in a future, otherwise
   * the future will contain an {@link net.opentsdb.uid.IdException}.
   *
   * @param name The name to allocate an ID for
   * @param type The type of name to allocate an ID for
   * @return A future that contains the newly allocated ID if successful,
   * otherwise the future will contain a {@link net.opentsdb.uid.IdException}.
   */
  @Override
  public Deferred<byte[]> allocateUID(final String name,
                                      final UniqueIdType type) {
    // This discards half the hash but it should still work ok with murmur3.
    final long id = Hashing.murmur3_128().hashString(name, CHARSET).asLong();

    // This does not protect us against someone trying to create the same
    // information in parallel but it is a convenience to the user so that we
    // do not even try to create if we can find an existing ID with the
    // information we are trying to allocate now.
    ListenableFuture<Void> availableFuture = checkAvailable(id, name, type);

    return wrap(transform(availableFuture, new AsyncFunction<Void, byte[]>() {
      @Override
      public ListenableFuture<byte[]> apply(final Void available) {
        // #checkAvailable will have thrown an exception if the id or name was
        // not available and if it did we would not be there. Thus we are now
        // free to create the id.
        return createId(id, name, type);
      }
    }));
  }

  /**
   * For all intents and purposes this function works as a rename. In the HBase
   * implementation the other method {@link #allocateUID} uses this method that
   * basically overwrites the value no matter what. This method is also used by
   * the function {@link net.opentsdb.uid.UniqueId#rename}.
   *
   * TODO #zeeck this method should be considered to be changed to rename and
   * the implementation changed in the HBaseStore. One of tre prerequisites of
   * this function is that the UID already exists.
   *
   * @param name The name to write.
   * @param uid  The uid to use.
   * @param type The type of UID
   * @return The uid that was used.
   */
  @Override
  public Deferred<byte[]> allocateUID(final String name, final byte[] uid,
                                      final UniqueIdType type) {

    // Get old name, we do this manually because the other method returns
    // a deferred and we want to avoid to mix deferreds between functions.
    ResultSetFuture f = session.executeAsync(get_name_statement.bind(
            IdUtils.uidToLong(uid), type.toValue()));

    final Deferred<byte[]> d = new Deferred<byte[]>();

    //CQL = "UPDATE tsdb." + Tables.ID_TO_NAME + " SET name = ? WHERE uid = ? AND type = ?;";
    final BoundStatement s1 = new BoundStatement(update_uid_name_statement)
            .bind(name, IdUtils.uidToLong(uid), type.toValue());

    Futures.addCallback(f, new FutureCallback<ResultSet>() {
      @Override
      public void onSuccess(ResultSet rows) {
        final String old_name = rows.one().getString("name");
        session.executeAsync(s1);
        BoundStatement s = new BoundStatement
                (update_name_uid_statement);
        // CQL =
        // BEGIN BATCH
        // DELETE FROM tsdb.name_to_id WHERE name = ? AND type = ?
        // INSERT INTO tsdb.name_to_id (name, type, uid) VALUES (?, ?, ?)
        // APPLY BATCH;
        session.executeAsync(s.bind(old_name, type.toValue(),
                name, type.toValue(), IdUtils.uidToLong(uid)));
        //TODO (zeeck) maybe check if this was ok
        d.callback(uid);
      }

      @Override
      public void onFailure(Throwable throwable) {
        d.callback(throwable);
      }
    });
    return d;
  }

  @Override
  public Deferred<Object> delete(Annotation annotation) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> updateAnnotation(Annotation original, Annotation annotation) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<List<Annotation>> getGlobalAnnotations(long start_time, long end_time) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Integer> deleteAnnotationRange(byte[] tsuid, long start_time, long end_time) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<ImmutableList<DataPoints>> executeQuery(Query query) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Map<byte[], Long>> getLastWriteTimes(final ResolvedSearchQuery query) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<List<IdentifierDecorator>> executeIdQuery(final IdQuery query) {
    throw new UnsupportedOperationException("Not implemented yet!");
  }

  @Override
  public Deferred<Tree> fetchTree(int tree_id) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> storeTree(Tree tree, boolean overwrite) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Integer> createNewTree(Tree tree) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<List<Tree>> fetchAllTrees() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> deleteTree(int tree_id, boolean delete_definition) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Map<String, String>> fetchCollisions(int tree_id, List<String> tsuids) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Map<String, String>> fetchNotMatched(int tree_id, List<String> tsuids) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> flushTreeCollisions(Tree tree) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> flushTreeNotMatched(Tree tree) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> storeLeaf(Leaf leaf, Branch branch, Tree tree) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<ArrayList<Boolean>> storeBranch(Tree tree, Branch branch, boolean store_leaves) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Branch> fetchBranchOnly(byte[] branch_id) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Branch> fetchBranch(byte[] branch_id, boolean load_leaf_uids, TSDB tsdb) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<TreeRule> fetchTreeRule(int tree_id, int level, int order) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Object> deleteTreeRule(int tree_id, int level, int order) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Object> deleteAllTreeRule(int tree_id) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> syncTreeRuleToStorage(TreeRule rule, boolean overwrite) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<List<byte[]>> executeTimeSeriesQuery(final ResolvedSearchQuery query) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Object> delete(TSMeta tsMeta) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Object> deleteTimeseriesCounter(TSMeta ts) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> create(TSMeta tsMeta) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<TSMeta> getTSMeta(byte[] tsuid) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> syncToStorage(TSMeta tsMeta, Deferred<ArrayList<Object>> uid_group, boolean overwrite) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<List<TSMeta>> executeTimeseriesMetaQuery(final ResolvedSearchQuery query) {
    throw new UnsupportedOperationException("Not implemented yet!");
  }

  @Override
  public Deferred<Boolean> TSMetaExists(String tsuid) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Boolean> TSMetaCounterExists(byte[] tsuid) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Long> incrementAndGetCounter(byte[] tsuid) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Deferred<Object> setTSMetaCounter(byte[] tsuid, long number) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Calculate the base time based on a timestamp to be used in a row key.
   */
  private static long buildBaseTime(final long timestamp) {
    if ((timestamp & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      return ((timestamp / 1000) - ((timestamp / 1000) % Const.MAX_TIMESPAN));
    } else {
      return (timestamp - (timestamp % Const.MAX_TIMESPAN));
    }
  }
}
