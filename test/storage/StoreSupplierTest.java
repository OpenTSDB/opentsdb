package net.opentsdb.storage;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

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
    StoreSupplier supplier;

    @Before
    public void setUp() throws Exception {
        config = new Config(false);
        storeDescriptors = ImmutableSet.<StoreDescriptor>of(new HBaseStoreDescriptor());
    }

    /*  Constructor */

    @Test (expected = NullPointerException.class)
    public void storeSupplierNullConfig() {
        new StoreSupplier(null, storeDescriptors);
    }

    @Test (expected = NullPointerException.class)
    public void storeSupplierNullIterable() {
        new StoreSupplier(config, null);
    }

    /*  get() */

    @Test (expected = IllegalArgumentException.class)
    public void testGetEmptyConfig() throws Exception {
        config.overrideConfig("tsd.storage.adapter", "");
        supplier = new StoreSupplier(config, storeDescriptors);
        supplier.get();
    }

    @Test
    public void testGetMatchingStore() throws Exception {
        config.overrideConfig("tsd.storage.adapter",
            "net.opentsdb.storage.hbase.HBaseStoreDescriptor");
        supplier = new StoreSupplier(config, storeDescriptors);
        TsdbStore store = supplier.get();
        assertTrue(store instanceof HBaseStore);
    }

    @Test
    public void testGetNoMatchingStore() throws Exception {
        config.overrideConfig("tsd.storage.adapter", "FooBar4711");
        supplier = new StoreSupplier(config, storeDescriptors);
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
