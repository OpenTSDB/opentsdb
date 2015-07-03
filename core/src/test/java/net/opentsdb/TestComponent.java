package net.opentsdb;

import net.opentsdb.core.ConfigModule;
import net.opentsdb.core.CoreModule;
import net.opentsdb.core.LabelClientTest;
import net.opentsdb.core.MetaClientAnnotationTest;
import net.opentsdb.core.MetaClientLabelMetaTest;
import net.opentsdb.plugins.PluginsModule;
import net.opentsdb.plugins.RealTimePublisher;
import net.opentsdb.search.IdChangeIndexerListenerTest;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.storage.StoreModule;
import net.opentsdb.storage.StoreModuleTest;
import net.opentsdb.uid.LabelClientTypeContextTest;
import net.opentsdb.uid.WildcardIdLookupStrategyTest;

import dagger.Component;

import javax.inject.Singleton;

/**
 * A dagger component that is configured to be able to inject into test classes.
 *
 * <p>The module will return an instance of {@link net.opentsdb.storage.MemoryStore} but it will not
 * expose this, it is instead exposed as a general {@link net.opentsdb.storage.TsdbStore}. This
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

  void inject(LabelClientTypeContextTest labelClientTypeContextTest);

  void inject(LabelClientTest idClientTest);

  void inject(MetaClientAnnotationTest metaClientAnnotationTest);

  void inject(IdChangeIndexerListenerTest idChangeIndexerListenerTest);

  void inject(WildcardIdLookupStrategyTest wildcardIdLookupStrategyTest);

  SearchPlugin searchPlugin();

  RealTimePublisher realTimePublisher();
}
