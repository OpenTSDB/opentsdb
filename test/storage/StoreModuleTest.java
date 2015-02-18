package net.opentsdb.storage;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import dagger.ObjectGraph;
import net.opentsdb.TestModule;
import net.opentsdb.storage.hbase.HBaseStoreDescriptor;
import net.opentsdb.utils.Config;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;

import static org.junit.Assert.*;

public class StoreModuleTest {
  /**
   * A constant that describes the number of stores that the corev project comes
   * with and thus how many store descriptors that the {@link java.util
   * .ServiceLoader} should be able to find.
   *
   * Note that the {@link net.opentsdb.storage.MemoryStore} is not included in
   * this number. This is intentional. The memory store should only be used in
   * tests and leaving it out of this prevents anyone of accidentally using it
   * in production.
   */
  private static final int NUM_STORES = 2;

  @Inject Config config;

  private Iterable<StoreDescriptor> storeDescriptors;
  private StoreModule supplier;

  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new TestModule()).inject(this);

    storeDescriptors = ImmutableSet.<StoreDescriptor>of(new HBaseStoreDescriptor());
    supplier = new StoreModule();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetEmptyConfig() throws Exception {
    config.overrideConfig("tsd.storage.adapter", "");
    supplier.provideStoreDescriptor(config, storeDescriptors);
  }

  @Test
  public void testGetMatchingStore() throws Exception {
    config.overrideConfig("tsd.storage.adapter",
            "net.opentsdb.storage.hbase.HBaseStoreDescriptor");
    StoreDescriptor storeDescriptor =
            supplier.provideStoreDescriptor(config, storeDescriptors);
    assertTrue(storeDescriptor instanceof HBaseStoreDescriptor);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testGetNoMatchingStore() throws Exception {
    config.overrideConfig("tsd.storage.adapter", "FooBar4711");
    supplier.provideStoreDescriptor(config, storeDescriptors);
  }

  @Test
  public void testNumberOfFoundStoreDescriptors() {
    Iterable<StoreDescriptor> storeDescriptors = supplier.provideStoreDescriptors();
    assertEquals(NUM_STORES, Iterables.size(storeDescriptors));
  }
}
