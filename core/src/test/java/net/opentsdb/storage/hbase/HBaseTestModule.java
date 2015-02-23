package net.opentsdb.storage.hbase;

import com.typesafe.config.ConfigFactory;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.TsdbModule;
import net.opentsdb.storage.StoreDescriptor;
import com.typesafe.config.Config;

/**
 * This is the dagger module that should be used by all HBase tests. It provides
 * a live HBaseStore as its TsdbStore.
 *
 * @see net.opentsdb.core.TsdbModule
 * @see net.opentsdb.TestModule
 */
@Module(includes = TsdbModule.class,
        overrides = true,
        injects = {
                IdQueryRunnerTest.class
        })
class HBaseTestModule {
  @Provides
  Config provideConfig() {
    return ConfigFactory.load();
  }

  @Provides
  StoreDescriptor provideStoreDescriptor() {
    return new HBaseStoreDescriptor();
  }
}
