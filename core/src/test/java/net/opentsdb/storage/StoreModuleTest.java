package net.opentsdb.storage;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import com.typesafe.config.ConfigValueFactory;
import dagger.ObjectGraph;
import net.opentsdb.TestModule;
import net.opentsdb.core.InvalidConfigException;
import net.opentsdb.stats.Metrics;
import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class StoreModuleTest {
  /**
   * A constant that describes the number of stores that the core project comes
   * with and thus how many store descriptors that the {@link java.util
   * .ServiceLoader} should be able to find.
   *
   * This is zero because the core project really should not provide any store
   * implementations on its own. There is one exception which is the {@link
   * net.opentsdb.storage.MemoryStore} however that is not exposed through the
   * {@link java.util.ServiceLoader} and nor should it be.
   */
  private static final int NUM_STORES = 0;

  @Inject Config config;

  private Iterable<StoreDescriptor> storeDescriptors;
  private StoreModule supplier;

  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new TestModule()).inject(this);

    storeDescriptors = ImmutableSet.<StoreDescriptor>of(new TestStoreDescriptor());
    supplier = new StoreModule();
  }

  @Test(expected = InvalidConfigException.class)
  public void testGetEmptyConfig() throws Exception {
    config = config.withValue("tsd.storage.adapter",
            ConfigValueFactory.fromAnyRef(""));
    supplier.provideStoreDescriptor(config, storeDescriptors);
  }

  @Test
  public void testGetMatchingStore() throws Exception {
    config = config.withValue("tsd.storage.adapter",
            ConfigValueFactory.fromAnyRef("net.opentsdb.storage.StoreModuleTest.TestStoreDescriptor"));
    StoreDescriptor storeDescriptor =
            supplier.provideStoreDescriptor(config, storeDescriptors);
    assertTrue(storeDescriptor instanceof TestStoreDescriptor);
  }

  @Test (expected = InvalidConfigException.class)
  public void testGetNoMatchingStore() throws Exception {
    config = config.withValue("tsd.storage.adapter",
            ConfigValueFactory.fromAnyRef("FooBar4711"));
    supplier.provideStoreDescriptor(config, storeDescriptors);
  }

  @Test
  public void testNumberOfFoundStoreDescriptors() {
    Iterable<StoreDescriptor> storeDescriptors = supplier.provideStoreDescriptors();
    assertEquals(NUM_STORES, Iterables.size(storeDescriptors));
  }

  private static class TestStoreDescriptor extends StoreDescriptor {
    @Override
    public TsdbStore createStore(final Config config, final MetricRegistry metrics) {
      return mock(TsdbStore.class);
    }
  }
}
