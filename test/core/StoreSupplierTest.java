package net.opentsdb.core;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.StorePlugin;
import net.opentsdb.storage.hbase.HBaseStore;
import net.opentsdb.storage.hbase.HBaseStorePlugin;
import net.opentsdb.utils.Config;
import org.junit.Before;
import org.junit.Test;

import java.util.ServiceLoader;

import static org.junit.Assert.*;

public class StoreSupplierTest {
    /**
     * A constant that describes the number of stores that the corev project
     * comes with and thus how many store plugins that the {@link java.util
     * .ServiceLoader} should be able to find.
     */
    private static final int NUM_STORES = 1;

    private Config config;
    private Iterable<StorePlugin> storePlugins;
    StoreSupplier supplier;

    @Before
    public void setUp() throws Exception {
        config = new Config(false);
        storePlugins = ImmutableSet.<StorePlugin>of(new HBaseStorePlugin());
    }

    /*  Constructor */

    @Test (expected = NullPointerException.class)
    public void storeSupplierNullConfig() {
        new StoreSupplier(null, storePlugins);
    }

    @Test (expected = NullPointerException.class)
    public void storeSupplierNullIterable() {
        new StoreSupplier(config, null);
    }

    /*  get() */

    @Test (expected = IllegalArgumentException.class)
    public void testGetEmptyConfig() throws Exception {
        config.overrideConfig("tsd.storage.adapter", "");
        supplier = new StoreSupplier(config, storePlugins);
        supplier.get();
    }

    @Test
    public void testGetMatchingPlugin() throws Exception {
        config.overrideConfig("tsd.storage.adapter",
            "net.opentsdb.storage.hbase.HBaseStorePlugin");
        supplier = new StoreSupplier(config, storePlugins);
        TsdbStore store = supplier.get();
        assertTrue(store instanceof HBaseStore);
    }

    @Test
    public void testGetNoMatchingPlugin() throws Exception {
        config.overrideConfig("tsd.storage.adapter", "FooBar4711");
        supplier = new StoreSupplier(config, storePlugins);
        try {
            supplier.get();
            fail("Should have thrown an IllegalArgumentException but did not!");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("FooBar4711"));
        }
    }

    /* ServiceLoader */

    @Test
    public void testNumberOfFoundStorePlugins() {
        ServiceLoader<StorePlugin> storePlugins = ServiceLoader.load(StorePlugin.class);
        assertEquals(NUM_STORES, Iterables.size(storePlugins));
    }
}
