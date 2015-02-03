package net.opentsdb.storage;


import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;

import net.opentsdb.stats.Metrics;
import net.opentsdb.utils.Config;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Use this class to create a TsdbStore instance. Given a config file and an
 * iterable with store plugin this class will set up an instance of the
 * configured store.
 */
public class StoreSupplier implements Supplier<TsdbStore> {
  /**
   * The config object to be used when we want to create a
   * {@link TsdbStore} object.
   */
  private final Config config;

  /**
   * The store plugins to look among.
   */
  private final Iterable<StoreDescriptor> storePlugins;

  /**
   * The metrics instance to use with the store supplied by this supplier.
   */
  private final Metrics metrics;

  /**
   * Instantiates a supplier for further use.
   *
   * @param config The configuration object used when generating a
   *               TsdbStore object.
   * @param storePlugins The store plugins to look among for a matching one.
   * @param metrics The metrics instance to use with the store supplied by
   *                this supplier.
   */
  public StoreSupplier(final Config config,
                       final Iterable<StoreDescriptor> storePlugins,
                       final Metrics metrics) {
    this.config = checkNotNull(config);
    this.storePlugins = checkNotNull(storePlugins);
    this.metrics = checkNotNull(metrics);
  }

  /**
   * Get the {@link TsdbStore} that the configuration specifies. This method
   * will create a new instance on each call.
   *
   * @return This method will return a ready to use {@link TsdbStore} object.
   * No guarantee is made that it will connect properly to the database but
   * it will be configured according to the config class sent in when this
   * object was created.
   */
  @Override
  public TsdbStore get() {
    String adapter_type = config.getString("tsd.storage.adapter");
    if (Strings.isNullOrEmpty(adapter_type)) {
      throw new IllegalArgumentException("The config could not find the" +
          " field 'tsd.storage.adapter', please make sure it was " +
          "configured correctly.");
    }

    for (final StoreDescriptor storeDescriptor : storePlugins) {
      String pluginName = storeDescriptor.getClass().getCanonicalName();

      if (pluginName.equals(adapter_type))
        return storeDescriptor.createStore(config, metrics);
    }

    throw new IllegalArgumentException("The config could not find a valid" +
        " value for the field 'tsd.storage.adapter', please make sure" +
        " it was configured correctly. It was '" + adapter_type + "'.");
  }

  /**
   * Only for debug at the moment.
   *
   * @return Returns a string showing what kind of TsdbStore this supplier
   * will return when one call {@link StoreSupplier#get()}.
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("config.adapter", config.getString("tsd.storage.adapter"))
        .toString();
  }
}
