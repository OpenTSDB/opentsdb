package net.opentsdb.storage.hbase;

import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.TsdbModule;
import net.opentsdb.storage.StoreDescriptor;
import net.opentsdb.utils.Config;

import java.io.IOException;

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
    try {
      return new Config(false);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to load config", e);
    }
  }

  @Provides
  StoreDescriptor provideStoreDescriptor() {
    return new HBaseStoreDescriptor();
  }
}
