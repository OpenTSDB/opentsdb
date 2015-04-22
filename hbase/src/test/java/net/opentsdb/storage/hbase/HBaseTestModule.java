package net.opentsdb.storage.hbase;

import com.typesafe.config.ConfigFactory;
import dagger.Module;
import dagger.Provides;
import net.opentsdb.core.CoreModule;
import net.opentsdb.core.PluginsModule;
import com.typesafe.config.Config;
import net.opentsdb.storage.StoreModule;

/**
 * This is the dagger module that should be used by all HBase tests. It provides
 * a live HBaseStore as its TsdbStore.
 *
 * @see net.opentsdb.core.TsdbModule
 * @see net.opentsdb.TestModule
 */
@Module(includes = {
            CoreModule.class,
            PluginsModule.class,
            StoreModule.class
        },
        injects = {
                IdQueryRunnerTest.class
        })
class HBaseTestModule {
  @Provides
  Config provideConfig() {
    return ConfigFactory.load("hbase");
  }
}
