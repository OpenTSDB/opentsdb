package net.opentsdb.storage.cassandra;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;
import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.DatabaseTests;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(DatabaseTests.class)
public class TestCassandraStore {

    private CassandraStore store;
    private static Config config;

    private static final String METRIC_NAME_ONE  = "sys";
    private static final String METRIC_NAME_TWO  = "cpu0";
    private static final String METRIC_NAME_THREE = "cpu1";
    private static final String TAGK_NAME_ONE    = "host";
    private static byte[]       TAGK_UID_ONE;
    private static final String TAGV_NAME_ONE    = "127.0.0.1";
    private static byte[]       TAGV_UID_ONE;
    private static byte[]       TSUID_ONE;
    private static byte[]       TSUID_TWO;
    private static byte[]       TSUID_THREE;

    private Map<String, byte[]> name_uid = new HashMap<String, byte[]>();

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
        store = new CassandraStoreDescriptor().createStore(config, new Metrics(new MetricRegistry()));
    }

    /**
     * Use this to clear the data in your localhost version of Cassandra
     */
    private void clearData() {
        store.getSession().execute("TRUNCATE tsdb.data");
        store.getSession().execute("TRUNCATE tsdb.tags_data");
        store.getSession().execute("TRUNCATE tsdb.uid_id");
        store.getSession().execute("TRUNCATE tsdb.name_id");
        store.getSession().execute("TRUNCATE tsdb.max_uid_type");
        store.getSession().execute("TRUNCATE tsdbunique.uid_name_id");

        store.getSession().execute("UPDATE tsdb.max_uid_type SET max = max + " +
                "0 WHERE type='metrics';");
        store.getSession().execute("UPDATE tsdb.max_uid_type SET max = max + " +
                "0 WHERE type='tagk';");
        store.getSession().execute("UPDATE tsdb.max_uid_type SET max = max + " +
                "0 WHERE type='tagv';");
    }

    /**
     * Use this to set up some test data. This method will call for {@link
     * #setUpCassandraConnection} and {@link #clearData}. Then it will use
     * the {@link CassandraStore#addPoint} and {@link
     * CassandraStore#allocateUID} to get UID from the database.
     * WARNING!
     * If either of those tests fail in the original test one should consider
     * this whole thing to fail.
     */
    private void setUpData() throws Exception {
        setUpCassandraConnection();
        clearData();

        name_uid.put(METRIC_NAME_ONE, store.allocateUID(
                METRIC_NAME_ONE, UniqueIdType.METRIC).joinUninterruptibly());

        name_uid.put(METRIC_NAME_TWO, store.allocateUID(
                METRIC_NAME_TWO, UniqueIdType.METRIC).joinUninterruptibly());

        name_uid.put(METRIC_NAME_THREE, store.allocateUID(
                METRIC_NAME_THREE, UniqueIdType.METRIC).joinUninterruptibly());


        TAGK_UID_ONE = store.allocateUID(TAGK_NAME_ONE, UniqueIdType.TAGK)
                .joinUninterruptibly();

        TAGV_UID_ONE = store.allocateUID(TAGV_NAME_ONE, UniqueIdType.TAGV)
                .joinUninterruptibly();

        TSUID_ONE = createTSUIDFromTreeUID(name_uid.get
                (METRIC_NAME_ONE), TAGK_UID_ONE, TAGV_UID_ONE);
        TSUID_TWO = createTSUIDFromTreeUID(name_uid.get
                (METRIC_NAME_TWO), TAGK_UID_ONE, TAGV_UID_ONE);
        TSUID_THREE = createTSUIDFromTreeUID(name_uid.get
                (METRIC_NAME_THREE), TAGK_UID_ONE, TAGV_UID_ONE);


        store.addPoint(TSUID_ONE, new byte[]{'d','1'}, 1356998400, (short)'a');
        store.addPoint(TSUID_ONE, new byte[]{'d','2'}, 1356998401, (short)'b');
        store.addPoint(TSUID_ONE, new byte[]{'d','2'}, 1357002078, (short)'b');

        store.addPoint(TSUID_TWO, new byte[]{'d','3'}, 1356998400, (short)'b');

    }
    private byte[] createTSUIDFromTreeUID(final byte[] metric, final byte[]
            tagk, final byte[] tagv) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );

        outputStream.write( metric );
        outputStream.write(tagk);
        outputStream.write(tagv);
        return outputStream.toByteArray();
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
            } catch (IllegalArgumentException e) {
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
    public void addPoint() throws Exception {
        setUpCassandraConnection();
        clearData();
            //'001001001', 1356998400, 1356998400, 'a', 'data1'
            store.addPoint(
                    new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
                    new byte[]{'v', 'a', 'l', 'u', 'e', '1'},
                    (long) 1356998400,
                    (short) 47).joinUninterruptibly(CassandraConst
                    .CASSANDRA_TIMEOUT);
    }

    @Test (expected = NullPointerException.class)
    public void addPointNull() throws Exception {
        setUpCassandraConnection();
        clearData();
        store.addPoint(
                null,
                new byte[]{'v', 'a', 'l', 'u', 'e', '1'},
                (long) 1356998400,
                (short) 47).joinUninterruptibly(CassandraConst
                .CASSANDRA_TIMEOUT);

    }

    @Test (expected = IllegalArgumentException.class)
    public void addPointEmpty() throws Exception {
        setUpCassandraConnection();
        clearData();
        store.addPoint(
                new byte[]{},
                new byte[]{'v', 'a', 'l', 'u', 'e', '1'},
                (long) 1356998400,
                (short) 47).joinUninterruptibly(CassandraConst
                .CASSANDRA_TIMEOUT);

    }
    @Test (expected = IllegalArgumentException.class)
    public void addPointTooShort() throws Exception {
        setUpCassandraConnection();
        clearData();
        store.addPoint(
                new byte[]{0, 0, 1, 0, 0, 1, 0, 0},
                new byte[]{'v', 'a', 'l', 'u', 'e', '1'},
                (long) 1356998400,
                (short) 47).joinUninterruptibly(CassandraConst
                .CASSANDRA_TIMEOUT);
    }

    @Test
    public void allocateUID() throws Exception {
        setUpData();
        byte[] new_metric_uid = store.allocateUID("new", UniqueIdType.METRIC)
                .joinUninterruptibly
                (CassandraConst.CASSANDRA_TIMEOUT);
        long max_uid = 0 ;
        for(byte[] uid : name_uid.values()) {
            max_uid = Math.max(UniqueId.uidToLong(uid),max_uid);
        }
        assertEquals(max_uid + 1, UniqueId.uidToLong(new_metric_uid));
    }

    @Test
    public void renameUID() {
        fail();
        store.allocateUID("renamed",new byte[]{0,0,4},UniqueIdType.METRIC);
    }

    @Test
    public void getName() throws Exception {
        setUpData();
        validateValidName(METRIC_NAME_ONE, UniqueIdType.METRIC);
        validateValidName(METRIC_NAME_TWO, UniqueIdType.METRIC);
        validateValidName(METRIC_NAME_THREE, UniqueIdType.METRIC);
        validateInvalidName(new byte[] {0, 0, 10}, UniqueIdType.METRIC);
        validateValidName(TAGK_NAME_ONE, UniqueIdType.TAGK);
        validateValidName(TAGV_NAME_ONE, UniqueIdType.TAGK);
    }

    @Test
    public void getId() throws Exception {
        setUpData();
        validateValidId(METRIC_NAME_ONE, UniqueIdType.METRIC);
        validateValidId(METRIC_NAME_TWO, UniqueIdType.METRIC);
        validateValidId(METRIC_NAME_THREE, UniqueIdType.METRIC);
        validateInvalidId("Missing", UniqueIdType.METRIC);
        validateValidId(TAGK_NAME_ONE, UniqueIdType.TAGK);
        validateValidId(TAGV_NAME_ONE, UniqueIdType.TAGV);
    }

    private void validateValidId(final String name, final UniqueIdType type)
            throws Exception {
        Optional<byte[]> value = store.getId(name, type)
                .joinUninterruptibly(CassandraConst.CASSANDRA_TIMEOUT);

        assertTrue(value.isPresent());
        assertArrayEquals(name_uid.get(name), value.get());
    }

    private void validateInvalidId(final String name, final UniqueIdType type)
            throws Exception {
        Optional<byte[]> value = store.getId(name, type)
                .joinUninterruptibly(CassandraConst.CASSANDRA_TIMEOUT);

        assertFalse(value.isPresent());
    }
    private void validateValidName(final String name, final UniqueIdType type)
            throws Exception {
        Optional<String> value = store.getName(name_uid.get(name), type)
                .joinUninterruptibly(CassandraConst.CASSANDRA_TIMEOUT);

        assertTrue(value.isPresent());
        assertEquals(name,value.get());
    }

    private void validateInvalidName(final byte[] uid, final UniqueIdType type)
            throws Exception {
        Optional<String> value = store.getName(uid, type)
                .joinUninterruptibly(CassandraConst.CASSANDRA_TIMEOUT);
        assertFalse(value.isPresent());
    }
}
