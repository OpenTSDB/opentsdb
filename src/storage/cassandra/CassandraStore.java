package net.opentsdb.storage.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.Const;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.StringCoder;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.Label;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdType;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

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
   * The statement used by the {@link #addPoint} method.
   */
  private PreparedStatement add_point_statement;
  private PreparedStatement insert_tags_statement;
  /**
   * The statement used by the {@link #allocateUID} method.
   */
  private PreparedStatement get_max_uid_statement;
  private PreparedStatement increment_uid_statement;
  private PreparedStatement create_new_uid_name_statement;
  private PreparedStatement create_new_name_uid_statement;
  private PreparedStatement lock_uid_name_combination;
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
   * {@link net.opentsdb.storage.StoreSupplier#get()}.
   *
   * @param cluster The configured Cassandra cluster.
   */
  public CassandraStore(final Cluster cluster) {

    this.cluster = cluster;
    this.session = cluster.connect("tsdb");

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
    String CQL = "INSERT INTO \"tsdb\".\"data\" (tsuid, basetime, " +
            "timestamp, flags, val) VALUES (?, ?, ?, ?, ?);";
    add_point_statement = session.prepare(CQL);

    CQL = "UPDATE \"tsdb\".\"max_uid_type\" SET max = max + 1 WHERE " +
            "type=?;";
    increment_uid_statement = session.prepare(CQL);

    CQL = "SELECT * FROM \"tsdb\".\"max_uid_type\" WHERE type=?;";
    get_max_uid_statement = session.prepare(CQL);

    CQL = "INSERT INTO tsdb.uid_id (uid, type, name) VALUES (?, ?, ?);";
    create_new_uid_name_statement = session.prepare(CQL);

    CQL = "INSERT INTO tsdb.name_id (name, type, uid) VALUES (?, ?, ?);";
    create_new_name_uid_statement = session.prepare(CQL);

    CQL = "INSERT INTO tsdbunique.uid_name_id (key, type) VALUES (?, ?) " +
            "IF NOT EXISTS;";
    lock_uid_name_combination = session.prepare(CQL);

    CQL = "UPDATE tsdb.uid_id SET name = ? WHERE uid = ? AND type = ?;";
    update_uid_name_statement = session.prepare(CQL);

    CQL = "BEGIN BATCH " +
            "DELETE FROM tsdb.name_id WHERE name = ? AND type= ? " +
            "INSERT INTO tsdb.name_id (name, type, uid) VALUES (?, ?, ?) " +
            "APPLY BATCH;";
    update_name_uid_statement = session.prepare(CQL);

    CQL = "SELECT * FROM tsdb.uid_id WHERE uid = ? AND type = ?;";
    get_name_statement = session.prepare(CQL);

    CQL = "SELECT * FROM tsdb.name_id WHERE name = ? AND type = ?;";
    get_id_statement = session.prepare(CQL);

    CQL = "INSERT INTO tsdb.tags_data (uid, type, tsuid) VALUES (?, ?, ?);";
    insert_tags_statement = session.prepare(CQL);
  }

  public Session getSession() {
    return session;
  }

  @Override
  public Deferred<Annotation> getAnnotation(byte[] tsuid, long start_time) {
    return null;
  }

  @Override
  public Deferred<Object> flush() {
    return null;
  }

  @Override
  public Deferred<ArrayList<KeyValue>> get(GetRequest request) {
    return null;
  }

  @Override
  public long getFlushInterval() {
    return 0;
  }

  @Override
  public Scanner newScanner(byte[] table) {
    return null;
  }

  @Override
  public void setFlushInterval(short aShort) {

  }

  @Override
  public Deferred<Object> addPoint(final byte[] tsuid, final byte[] value,
                                   final long timestamp, final short flags) {

    final long base_time = buildBaseTime(timestamp);


    final ResultSetFuture future = session.executeAsync(add_point_statement
            .bind(UniqueId.uidToString(tsuid), base_time, timestamp,
                    new Integer(flags),
                    StringCoder.fromBytes(value)));

    final byte[] metric_uid = UniqueId.getMetricFromTSUID(UniqueId
            .uidToString(tsuid));
    final List<byte[]> tags_uids = UniqueId.getTagsFromTSUID(UniqueId
            .uidToString(tsuid));

    final Deferred<Object> d = new Deferred<Object>();

    Futures.addCallback(future, new FutureCallback<ResultSet>() {
      @Override
      public void onSuccess(ResultSet rows) {
        d.callback(null);
        session.executeAsync(insert_tags_statement.bind(
                UniqueId.uidToLong(metric_uid, UniqueIdType.METRIC.width),
                UniqueIdType.METRIC.toValue(), UniqueId.uidToString
                        (tsuid)));
        for (int i = 0; i < tags_uids.size(); i += 2) {
          session.executeAsync(insert_tags_statement.bind(
                  UniqueId.uidToLong(tags_uids.get(i),
                          UniqueIdType.TAGK.width),
                  UniqueIdType.TAGK.toValue(), UniqueId.uidToString
                          (tsuid)));

          session.executeAsync(insert_tags_statement.bind(
                  UniqueId.uidToLong(tags_uids.get(i + 1),
                          UniqueIdType.TAGV.width),
                  UniqueIdType.TAGV.toValue(), UniqueId.uidToString
                          (tsuid)));
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
  public Deferred<Optional<byte[]>> getId(final String name, final
  UniqueIdType
          type) {

    ResultSetFuture f = session.executeAsync(get_id_statement.bind(
            name, type.toValue()));

    final Deferred<Optional<byte[]>> d = new Deferred<Optional<byte[]>>();

    Futures.addCallback(f, new FutureCallback<ResultSet>() {
      @Override
      public void onSuccess(ResultSet rows) {
        if (!rows.isExhausted()) {
          final long uid = rows.one().getLong("uid");
          d.callback(Optional.of(UniqueId.longToUID(uid, type.width)));
          return;
        }
        d.callback(Optional.absent());
      }

      @Override
      public void onFailure(Throwable throwable) {
        d.callback(throwable);
      }
    });
    return d;
  }

  @Override
  public Deferred<Optional<String>> getName(final byte[] id, final
  UniqueIdType type) {

    ResultSetFuture f = session.executeAsync(get_name_statement.bind(
            UniqueId.uidToLong(id), type.toValue()));

    final Deferred<Optional<String>> d = new Deferred<Optional<String>>();

    Futures.addCallback(f, new FutureCallback<ResultSet>() {
      @Override
      public void onSuccess(ResultSet rows) {
        if (!rows.isExhausted()) {
          final String name = rows.one().getString("name");
          d.callback(Optional.of(name));
          return;
        }
        d.callback(Optional.absent());
      }

      @Override
      public void onFailure(Throwable throwable) {
        d.callback(throwable);
      }
    });
    return d;
  }

  @Override
  public Deferred<Object> add(UIDMeta meta) {
    return null;
  }

  @Override
  public Deferred<Object> delete(UIDMeta meta) {
    return null;
  }

  @Override
  public Deferred<UIDMeta> getMeta(byte[] uid, String name, UniqueIdType type) {
    return null;
  }

  @Override
  public Deferred<Boolean> updateMeta(UIDMeta meta, boolean overwrite) {
    return null;
  }

  @Override
  public Deferred<Object> deleteUID(byte[] name, UniqueIdType type) {
    return null;
  }

  @Override
  public Deferred<byte[]> allocateUID(final String name, final UniqueIdType
          type) {

    final Deferred<byte[]> d = new Deferred<byte[]>();

    final ResultSetFuture future = session.executeAsync(get_max_uid_statement.bind
            (type.toValue()));

    Futures.addCallback(future, new FutureCallback<ResultSet>() {
      @Override
      public void onSuccess(ResultSet rows) {
        // we have received the value.
        final long next_uid = rows.one().getLong("max") + 1;
        //next step is to lock down this resource in a non blocking way.
        BatchStatement lock_batch = new BatchStatement();
        lock_batch.add(lock_uid_name_combination.bind(
                UniqueId.uidToString(UniqueId.longToUID(next_uid, type.width)),
                type.toValue()));
        lock_batch.add(lock_uid_name_combination.bind(
                name, type.toValue()));
        // We have a 'lock' table that we write to. We write two
        // rows one for each the name and the uid with the
        // UniqueIdType as an additional key.
        // If this write is successful we know there are no duplicate
        // values for the name and key for this UID type.
        // Thus onSuccess we will then go ahead and write this
        // resource to the original table.
        Futures.addCallback(
                session.executeAsync(lock_batch),
                new FutureCallback<ResultSet>() {
                  @Override
                  public void onSuccess(ResultSet rows) {
                    // We want this process to just write so no need to
                    // keep track of this callback at from this point.

                    if (rows.wasApplied()) {
                      session.executeAsync(increment_uid_statement.bind(type.toValue()));
                      session.executeAsync(create_new_name_uid_statement.bind(name, type.toValue(), next_uid));
                      session.executeAsync(create_new_uid_name_statement.bind(next_uid, type.toValue(), name));
                      //TODO (zeeck) maybe check callback?
                      // returned the used UID
                      // we will assume the write will be fine, Cassandra
                      // can do a lot of writes without issues so if we
                      // fail from this point there are some major issues!
                      d.callback(UniqueId.longToUID(next_uid, type.width));
                      return;
                    }
                    LOG.warn("Race condition creating mapping for uid " +
                            "{}. Another TSDB instance must have " +
                            "allocated this uid concurrently or " +
                            "name is already taken for " +
                            "this uid.", next_uid);
                    d.callback(new IllegalStateException("Could " +
                            "not allocate UID. Try again."));
                  }

                  @Override
                  public void onFailure(Throwable throwable) {
                    LOG.error("had a failure to communicate with " +
                            "Cassandra.", throwable);
                    d.callback(throwable);
                  }
                });

      }

      @Override
      public void onFailure(Throwable throwable) {
        // for some reason we could not get the max uid value.
        LOG.error("Could not fetch max UID value.", throwable);
        d.callback(throwable);
      }
    });

    return d;
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
            UniqueId.uidToLong(uid), type.toValue()));

    final Deferred<byte[]> d = new Deferred<byte[]>();

    //CQL = "UPDATE tsdb.uid_id SET name = ? WHERE uid = ? AND type = ?;";
    final BoundStatement s1 = new BoundStatement(update_uid_name_statement)
            .bind(name, UniqueId.uidToLong(uid), type.toValue());

    Futures.addCallback(f, new FutureCallback<ResultSet>() {
      @Override
      public void onSuccess(ResultSet rows) {
        final String old_name = rows.one().getString("name");
        session.executeAsync(s1);
        BoundStatement s = new BoundStatement
                (update_name_uid_statement);
        // CQL =
        // BEGIN BATCH
        // DELETE FROM tsdb.name_id WHERE name = ? AND type = ?
        // INSERT INTO tsdb.name_id (name, type, uid) VALUES (?, ?, ?)
        // APPLY BATCH;
        session.executeAsync(s.bind(old_name, type.toValue(),
                name, type.toValue(), UniqueId.uidToLong(uid)));
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
  public void scheduleForCompaction(byte[] row) {

  }

  @Override
  public Deferred<Object> delete(Annotation annotation) {
    return null;
  }

  @Override
  public Deferred<Boolean> updateAnnotation(Annotation original, Annotation annotation) {
    return null;
  }

  @Override
  public Deferred<List<Annotation>> getGlobalAnnotations(long start_time, long end_time) {
    return null;
  }

  @Override
  public Deferred<Integer> deleteAnnotationRange(byte[] tsuid, long start_time, long end_time) {
    return null;
  }

  @Override
  public Deferred<ImmutableList<DataPoints>> executeQuery(Query query) {
    return null;
  }

  @Override
  public Deferred<List<Label>> executeIdQuery(final IdQuery query) {
    throw new UnsupportedOperationException("Not implemented yet!");
  }

  @Override
  public Deferred<Tree> fetchTree(int tree_id) {
    return null;
  }

  @Override
  public Deferred<Boolean> storeTree(Tree tree, boolean overwrite) {
    return null;
  }

  @Override
  public Deferred<Integer> createNewTree(Tree tree) {
    return null;
  }

  @Override
  public Deferred<List<Tree>> fetchAllTrees() {
    return null;
  }

  @Override
  public Deferred<Boolean> deleteTree(int tree_id, boolean delete_definition) {
    return null;
  }

  @Override
  public Deferred<Map<String, String>> fetchCollisions(int tree_id, List<String> tsuids) {
    return null;
  }

  @Override
  public Deferred<Map<String, String>> fetchNotMatched(int tree_id, List<String> tsuids) {
    return null;
  }

  @Override
  public Deferred<Boolean> flushTreeCollisions(Tree tree) {
    return null;
  }

  @Override
  public Deferred<Boolean> flushTreeNotMatched(Tree tree) {
    return null;
  }

  @Override
  public Deferred<Boolean> storeLeaf(Leaf leaf, Branch branch, Tree tree) {
    return null;
  }

  @Override
  public Deferred<ArrayList<Boolean>> storeBranch(Tree tree, Branch branch, boolean store_leaves) {
    return null;
  }

  @Override
  public Deferred<Branch> fetchBranchOnly(byte[] branch_id) {
    return null;
  }

  @Override
  public Deferred<Branch> fetchBranch(byte[] branch_id, boolean load_leaf_uids, TSDB tsdb) {
    return null;
  }

  @Override
  public Deferred<TreeRule> fetchTreeRule(int tree_id, int level, int order) {
    return null;
  }

  @Override
  public Deferred<Object> deleteTreeRule(int tree_id, int level, int order) {
    return null;
  }

  @Override
  public Deferred<Object> deleteAllTreeRule(int tree_id) {
    return null;
  }

  @Override
  public Deferred<Boolean> syncTreeRuleToStorage(TreeRule rule, boolean overwrite) {
    return null;
  }

  @Override
  public Deferred<Object> delete(TSMeta tsMeta) {
    return null;
  }

  @Override
  public Deferred<Object> deleteTimeseriesCounter(TSMeta ts) {
    return null;
  }

  @Override
  public Deferred<Boolean> create(TSMeta tsMeta) {
    return null;
  }

  @Override
  public Deferred<TSMeta> getTSMeta(byte[] tsuid) {
    return null;
  }

  @Override
  public Deferred<Boolean> syncToStorage(TSMeta tsMeta, Deferred<ArrayList<Object>> uid_group, boolean overwrite) {
    return null;
  }

  @Override
  public Deferred<Boolean> TSMetaExists(String tsuid) {
    return null;
  }

  @Override
  public Deferred<Boolean> TSMetaCounterExists(byte[] tsuid) {
    return null;
  }

  @Override
  public Deferred<Long> incrementAndGetCounter(byte[] tsuid) {
    return null;
  }

  @Override
  public Deferred<Object> setTSMetaCounter(byte[] tsuid, long number) {
    return null;
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
