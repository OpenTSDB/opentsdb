package net.opentsdb;

import net.opentsdb.core.ConfigModule;
import net.opentsdb.core.CoreModule;
import net.opentsdb.core.IdClientTest;
import net.opentsdb.core.MetaClientAnnotationTest;
import net.opentsdb.core.MetaClientLabelMetaTest;
import net.opentsdb.plugins.PluginsModule;
import net.opentsdb.search.IdChangeIndexerListenerTest;
import net.opentsdb.storage.StoreModule;
import net.opentsdb.storage.StoreModuleTest;
import net.opentsdb.uid.TestUniqueId;
import net.opentsdb.uid.WildcardIdLookupStrategyTest;

import dagger.Component;

import javax.inject.Singleton;

/**
 * A dagger module that inherits from {@link net.opentsdb.core.TsdbModule} and both overrides it and
 * complements it.
 *
 * <p>This module complements the {@link net.opentsdb.core.TsdbModule} by providing a config.
 *
 * <p>The module will return an instance of {@link net.opentsdb.storage.MemoryStore} but it will not
 * expose this, it is instead exposed as a regular {@link net.opentsdb.storage.TsdbStore}. This
 * detail is important as we want to test a general {@link net.opentsdb.storage.TsdbStore}
 * implementation and not the behavior of the {@link net.opentsdb.storage.MemoryStore}. Because of
 * this tests should always strive to use this module as a base.
 */
@Component(
    modules = {
        ConfigModule.class,
        CoreModule.class,
        PluginsModule.class,
        StoreModule.class
    })
@Singleton
public interface TestComponent {
  void inject(MetaClientLabelMetaTest metaClientLabelMetaTest);

  void inject(StoreModuleTest storeModuleTest);

  void inject(TestUniqueId testUniqueId);

  void inject(IdClientTest idClientTest);

  void inject(MetaClientAnnotationTest metaClientAnnotationTest);

  void inject(IdChangeIndexerListenerTest idChangeIndexerListenerTest);

  void inject(WildcardIdLookupStrategyTest wildcardIdLookupStrategyTest);
}
