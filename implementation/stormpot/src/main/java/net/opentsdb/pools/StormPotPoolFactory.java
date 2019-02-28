package net.opentsdb.pools;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

public class StormPotPoolFactory extends BaseTSDBPlugin implements ObjectPoolFactory {
  public static final String PREFIX = "objectpool.";
  public static final String ALLOCATOR_KEY = "allocator";
  public static final String INITIAL_COUNT_KEY = "count.initial";
  
  @Override
  public ObjectPool newPool(final ObjectPoolConfig config) {
    try {
      final StormPotPool pool = new StormPotPool(tsdb, config);
      return pool;
    } catch (Throwable t) {
      throw new ObjectPoolException(t);
    }
  }

  @Override
  public String type() {
    return "StormPotPoolFactory";
  }
  
  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = id;
    this.tsdb = tsdb;
    registerConfigs(tsdb.getConfig());
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
  String myConfig(final String key) {
    return PREFIX + (Strings.isNullOrEmpty(id) ? "" : id + ".") + key;
  }
  
  void registerConfigs(final Configuration config) {
    if (!config.hasProperty(myConfig(INITIAL_COUNT_KEY))) {
      config.register(myConfig(INITIAL_COUNT_KEY), 4096, false, 
          "The initial count of items to store in the pool.");
    }
  }
  
}