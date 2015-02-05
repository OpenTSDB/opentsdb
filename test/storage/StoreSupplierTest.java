package net.opentsdb.storage;

import com.codahale.metrics.MetricRegistry;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.hbase.HBaseStore;
import net.opentsdb.storage.hbase.HBaseStoreDescriptor;
import net.opentsdb.utils.Config;
import org.junit.Before;
import org.junit.Test;

import java.util.ServiceLoader;

import static org.junit.Assert.*;

public class StoreSupplierTest {
    /**
     * A constant that describes the number of stores that the corev project
     * comes with and thus how many store descriptors that the {@link java.util
     * .ServiceLoader} should be able to find.
     *
     * Note that the {@link net.opentsdb.storage.MemoryStore} is not included
     * in this number. This is intentional. The memory store should only be
     * used in tests and leaving it out of this prevents anyone of
     * accidentally using it in production.
     */
    private static final int NUM_STORES = 1;

    private Config config;
    private Iterable<StoreDescriptor> storeDescriptors;
    private StoreSupplier supplier;
    private Metrics metrics;

    @Before
    public void setUp() throws Exception {
        config = new Config(false);
        storeDescriptors = ImmutableSet.<StoreDescriptor>of(new HBaseStoreDescriptor());
        metrics = new Metrics(new MetricRegistry());
    }

    /*  Constructor */

    @Test (expected = NullPointerException.class)
    public void storeSupplierNullConfig() {
        new StoreSupplier(null, storeDescriptors, metrics);
    }

    @Test (expected = NullPointerException.class)
    public void storeSupplierNullIterable() {
        new StoreSupplier(config, null, metrics);
    }

    @Test (expected = NullPointerException.class)
    public void storeSupplierNullMetrics() {
        new StoreSupplier(config, storeDescriptors, null);
    }

    /*  get() */

    @Test (expected = IllegalArgumentException.class)
    public void testGetEmptyConfig() throws Exception {
        config.overrideConfig("tsd.storage.adapter", "");
        supplier = new StoreSupplier(config, storeDescriptors, metrics);
        supplier.get();
    }

    @Test
    public void testGetMatchingStore() throws Exception {
        config.overrideConfig("tsd.storage.adapter",
            "net.opentsdb.storage.hbase.HBaseStoreDescriptor");
        supplier = new StoreSupplier(config, storeDescriptors, metrics);
        TsdbStore store = supplier.get();
        assertTrue(store instanceof HBaseStore);
    }

    @Test
    public void testGetNoMatchingStore() throws Exception {
        config.overrideConfig("tsd.storage.adapter", "FooBar4711");
        supplier = new StoreSupplier(config, storeDescriptors, metrics);
        try {
            supplier.get();
            fail("Should have thrown an IllegalArgumentException but did not!");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("FooBar4711"));
        }
    }

    /* ServiceLoader */

    @Test
    public void testNumberOfFoundStoreDescriptors() {
        ServiceLoader<StoreDescriptor> storeDescriptors = ServiceLoader.load(StoreDescriptor.class);
        assertEquals(NUM_STORES, Iterables.size(storeDescriptors));
    }

}
