package net.opentsdb.core;

import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.hbase.HBaseStore;
import net.opentsdb.utils.Config;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class StoreSupplierTest {

    Config config;
    StoreSupplier supplier;

    @Before
    public void setUp() throws Exception {
        config = new Config(false);
    }

    /*  Constructor */

    @Test
    public void storeSupplierDefaultConfig() {
        supplier = new StoreSupplier(config);
        assertTrue(supplier.toString().contains("HBase"));
    }

    @Test
    public void storeSupplierChangedConfig() {
        config.overrideConfig("tsd.storage.adapter", "FOO");
        supplier = new StoreSupplier(config);
        assertTrue(supplier.toString().contains("FOO"));
        assertFalse(supplier.toString().contains("HBase"));
    }

    @Test (expected = NullPointerException.class)
    public void storeSupplierNullConfig() {
        supplier = new StoreSupplier(null);
    }

    /*  get() */

    @Test
    public void testGetHBase() throws Exception {
        supplier = new StoreSupplier(config);

        TsdbStore store = supplier.get();

        assertTrue(store instanceof HBaseStore);
    }
    @Test
    public void testGetHBaseBadCase() throws Exception {
        config.overrideConfig("tsd.storage.adapter", "HbASE");
        supplier = new StoreSupplier(config);

        TsdbStore store = supplier.get();

        assertTrue(store instanceof HBaseStore);
    }

    @Test
    public void testGetCassandra() throws Exception {
        config.overrideConfig("tsd.storage.adapter", "Cassandra");
        supplier = new StoreSupplier(config);

        TsdbStore store = supplier.get();


        assertNull(store);
    }

    @Test
    public void testGetCassandraBadCase() throws Exception {
        config.overrideConfig("tsd.storage.adapter", "cAsSaNdRa");
        supplier = new StoreSupplier(config);

        TsdbStore store = supplier.get();
        assertNull(store);
    }

    @Test
    public void testGetInvalid() throws Exception {
        config.overrideConfig("tsd.storage.adapter", "FooBar4711");
        supplier = new StoreSupplier(config);
        try {
            supplier.get();
            fail("Should have thrown an IllegalArgumentException but did not!");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("FooBar4711"));
        }
    }

    @Test (expected = IllegalArgumentException.class)
    public void testGetEmpty() throws Exception {
        config.overrideConfig("tsd.storage.adapter", "");
        supplier = new StoreSupplier(config);

        TsdbStore store = supplier.get();
    }
}
