package net.opentsdb.storage.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    private Cluster cluster;
    /**
     * The current Cassandra session.
     */
    private Session session;

    public CassandraStore(final Cluster cluster) {

        this.cluster = cluster;
        this.session = cluster.connect("tsdb");

        Metadata metadata = cluster.getMetadata();

        //Show what we connected to in the debug log
        LOG.info("Connected to cluster: {}", metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            LOG.info("Datacenter: {}; Host: {}; Rack: {}",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
    }
    @Override
    public Deferred<Annotation> getAnnotation(byte[] tsuid, long start_time) {
        return null;
    }

    @Override
    public Deferred<ArrayList<Object>> checkNecessaryTablesExist() {
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
    public Deferred<Object> addPoint(byte[] tsuid, byte[] value, long timestamp, short flags) {
        return null;
    }

    @Override
    public Deferred<Object> shutdown() {
        return null;
    }

    @Override
    public void recordStats(StatsCollector collector) {

    }

    @Override
    public Deferred<Optional<byte[]>> getId(String name, UniqueIdType type) {
        return null;
    }

    @Override
    public Deferred<Optional<String>> getName(byte[] id, UniqueIdType type) {
        return null;
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
    public Deferred<byte[]> allocateUID(String name, UniqueIdType type) {
        return null;
    }

    @Override
    public Deferred<byte[]> allocateUID(String name, byte[] uid, UniqueIdType type) {
        return null;
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
}
