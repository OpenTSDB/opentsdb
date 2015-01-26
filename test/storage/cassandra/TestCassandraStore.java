package net.opentsdb.storage.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;
import net.opentsdb.core.StoreSupplier;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class TestCassandraStore {

    private CassandraStore store;
    private static Config config;
    private Session session;

    @BeforeClass
    public static void oneTimeSetUp() throws IOException {
        Map<String, String> overrides = new HashMap<String, String>();
        overrides.put("tsd.storage.adapter", "Cassandra");
        overrides.put("tsd.storage.cassandra.clusters", "127.0.0.1");

        config = new Config(false, overrides);
    }

    /**
     * Use this to connect to cassandra.
     */
    private void setUpCassandraConnection() {
        Cluster.Builder builder = Cluster.builder();
        Iterable<String> nodes = Splitter.on(',').trimResults()
                .omitEmptyStrings()
                .split(config.getString("tsd.storage.cassandra.clusters"));
        for (String node : nodes) {
            try {
                HostAndPort host = HostAndPort.fromString(node);

                builder.addContactPoint(host.getHostText()).withPort(host
                        .getPortOrDefault(CassandraConst
                                .DEFAULT_CASSANDRA_PORT));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("There was an error in the" +
                        " configuration file in the field 'tsd.storage" +
                        ".cassandra.clusters'.", e);
            }
        }
        Cluster cluster = builder.build();
        store = new CassandraStore(cluster);
        session = cluster.connect();
    }

    /**
     * Use this to clear the data in your localhost version of Cassandra
     */
    private void clearData() {
        session.execute("TRUNCATE tsdb.data");
        session.execute("TRUNCATE tsdb.tags_data");
        session.execute("TRUNCATE tsdb.uid_id");
        session.execute("TRUNCATE tsdb.name_id");
        session.execute("TRUNCATE tsdb.max_uid_type");
        session.execute("TRUNCATE tsdbunique.uid_name_id");
    }

    @Test
    public void constructor() throws IOException {
        Cluster.Builder builder = Cluster.builder();

        Iterable<String> nodes = Splitter.on(',').trimResults()
                .omitEmptyStrings()
                .split(config.getString("tsd.storage.cassandra.clusters"));

        for (String node : nodes) {
            try {
                HostAndPort host = HostAndPort.fromString(node);

                builder.addContactPoint(host.getHostText()).withPort(host
                        .getPortOrDefault(CassandraConst
                                .DEFAULT_CASSANDRA_PORT));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("There was an error in the" +
                        " configuration file in the field 'tsd.storage" +
                        ".cassandra.clusters'.", e);
            }
        }
        assertNotNull(new CassandraStore(builder.build()));
    }

    @Test (expected = NullPointerException.class)
    public void constructorNull() throws IOException {
        new CassandraStore(null);
    }

    @Test
    public void addPoint() {
        setUpCassandraConnection();
        clearData();


        //'001', 1356998400, 1356998400, 'a', 'data1'
    }


    @Test
    public void allocateUID() {
        store.allocateUID("new",UniqueIdType.METRIC);
    }

    @Test
    public void allocateUIDRename() {
        store.allocateUID("renamed",new byte[]{0,0,4},UniqueIdType.METRIC);
    }

    @Test
    public void getName() throws Exception {
        //TODO (zeeck) make timeout ot a const
        Optional<String> name;
        name = store.getName(new byte[] {0, 0, 1}, UniqueIdType.METRIC)
                .joinUninterruptibly(20);
        assertEquals("sys",name.get());
        name = store.getName(new byte[] {0, 0, 2}, UniqueIdType.METRIC)
                .joinUninterruptibly(20);
        assertEquals("cpu0",name.get());
        name = store.getName(new byte[] {0, 0, 3}, UniqueIdType.METRIC)
                .joinUninterruptibly(20);
        assertEquals("cpu1",name.get());
        name = store.getName(new byte[] {0, 0, 4}, UniqueIdType.METRIC)
                .joinUninterruptibly(20);
        assertEquals("renamed",name.get());
        name = store.getName(new byte[] {0, 0, 5}, UniqueIdType.METRIC)
                .joinUninterruptibly(20);
        assertFalse(name.isPresent());
    }
}