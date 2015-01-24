package net.opentsdb.core;

import net.opentsdb.search.SearchPlugin;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.StorePlugin;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A builder helper class to create TSDB instances.
 */
public class TsdbBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TsdbBuilder.class);

  private Supplier<TsdbStore> storeSupplier;
  private Config config;
  private SearchPlugin searchPlugin;
  private RTPublisher realtimePublisher;

  /**
   * Create a TsdbBuilder instance based on the provided config.
   * @param config The config the instance is to be based off
   * @return A newly created TsdbBuilder
   */
  public static TsdbBuilder createFromConfig(final Config config) {
    checkNotNull(config);

    addPluginPath(config);

    TsdbBuilder builder = new TsdbBuilder();

    StoreSupplier storeSupplier = new StoreSupplier(config,
        ServiceLoader.load(StorePlugin.class));

    builder.withConfig(config)
            .withStoreSupplier(storeSupplier)
            .withSearchPlugin(loadSearchPlugin(config))
            .withRealtimePublisher(loadRealtimePublisher(config));

    return builder;
  }

  /**
   * Add the plugin path that is read from the config to the class path.
   * @param config The config object to read from
   */
  private static void addPluginPath(final Config config) {
    final String plugin_path = config.getString("tsd.core.plugin_path");

    if (!Strings.isNullOrEmpty(plugin_path)) {
      try {
        PluginLoader.loadJARs(plugin_path);
      } catch (Exception e) {
        LOG.error("Error loading plugins from plugin path: {}", plugin_path, e);
        throw new RuntimeException("Error loading plugins from plugin path: " +
                plugin_path, e);
      }
    }
  }

  /**
   * Load the search plugin that the config describes and return it.
   * @param config The config object to read from
   * @return An optional that contains a search plugin or is absent if search
   * plugins are disabled
   * @see com.google.common.base.Optional
   */
  private static Optional<SearchPlugin> loadSearchPlugin(final Config config) {
    // load the search plugin if enabled
    if (config.getBoolean("tsd.search.enable")) {
      final SearchPlugin search = PluginLoader.loadSpecificPlugin(
              config.getString("tsd.search.plugin"), SearchPlugin.class);

      return Optional.of(search);
    }

    return Optional.absent();
  }

  /**
   * Load the realtime publisher that the config describes and return it.
   * @param config The config object to read from
   * @return An optional that contains a realtime publisher or is absent if
   * realtime publishers are disabled
   * @see com.google.common.base.Optional
   */
  private static Optional<RTPublisher> loadRealtimePublisher(final Config config) {
    // load the realtime publisher plugin if enabled
    if (config.getBoolean("tsd.rtpublisher.enable")) {
      RTPublisher rt_publisher = PluginLoader.loadSpecificPlugin(
              config.getString("tsd.rtpublisher.plugin"), RTPublisher.class);

      return Optional.of(rt_publisher);
    }

    return Optional.absent();
  }

  /**
   * Set the config to use with the TSDB instance that will be created.
   * @param config The config object to use
   * @return This instance
   */
  public TsdbBuilder withConfig(final Config config) {
    this.config = checkNotNull(config);
    return this;
  }

  /**
   * Set the store supplier that will be used by the TSDB instance created by
   * this builder.
   * @param supplier The {@link net.opentsdb.core.StoreSupplier} to use
   * @return This instance
   */
  public TsdbBuilder withStoreSupplier(final Supplier<TsdbStore> supplier) {
    this.storeSupplier = checkNotNull(supplier);
    return this;
  }

  /**
   * Set the store that will be used by the TSDB instance created by
   * this builder.
   * @param store The {@link net.opentsdb.storage.TsdbStore} to use
   * @return This instance
   */
  public TsdbBuilder withStore(final TsdbStore store) {
    withStoreSupplier(Suppliers.ofInstance(store));
    return this;
  }

  /**
   * Set the search plugin that will be used by the TSDB instance created by
   * this builder.
   * @param searchPlugin The {@link net.opentsdb.search.SearchPlugin} to use
   * @return This instance
   */
  public TsdbBuilder withSearchPlugin(final SearchPlugin searchPlugin) {
    this.searchPlugin = searchPlugin;
    return this;
  }

  /**
   * Set the search plugin that will be used by the TSDB instance created by
   * this builder.
   * @param searchPlugin The {@link net.opentsdb.search.SearchPlugin} to use
   * @return This instance
   * @see com.google.common.base.Optional
   */
  public TsdbBuilder withSearchPlugin(final Optional<SearchPlugin> searchPlugin) {
    withSearchPlugin(searchPlugin.or(new DefaultSearchPlugin()));
    return this;
  }

  /**
   * Set the realtime publisher that will be used by the TSDB instance
   * created by this builder.
   * @param realtimePublisher The {@link net.opentsdb.tsd.RTPublisher} to use
   * @return This instance
   */
  public TsdbBuilder withRealtimePublisher(final RTPublisher realtimePublisher) {
    this.realtimePublisher = realtimePublisher;
    return this;
  }

  /**
   * Set the realtime publisher that will be used by the TSDB instance
   * created by this builder.
   * @param realtimePublisher The {@link net.opentsdb.tsd.RTPublisher} to use
   * @return This instance
   * @see com.google.common.base.Optional
   */
  public TsdbBuilder withRealtimePublisher(final Optional<RTPublisher> realtimePublisher) {
    withRealtimePublisher(realtimePublisher.or(new DefaultRealtimePublisher()));
    return this;
  }

  /**
   * Build the TSDB instance based on the previously given parameters.
   * @return A newly created {@link net.opentsdb.core.TSDB} instance
   */
  public TSDB build() {
    return new TSDB(storeSupplier.get(), config, searchPlugin, realtimePublisher);
  }
}