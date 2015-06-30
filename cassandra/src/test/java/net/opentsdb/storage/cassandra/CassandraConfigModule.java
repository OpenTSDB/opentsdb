package net.opentsdb.storage.cassandra;

import net.opentsdb.core.ConfigModule;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module
public class CassandraConfigModule extends ConfigModule {
  public CassandraConfigModule() {
    super(ConfigFactory.load("cassandra"));
  }

  @Provides
  @Singleton
  Config provideConfig() {
    return config;
  }
}
