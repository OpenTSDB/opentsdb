package net.opentsdb.core;

import net.opentsdb.search.DefaultSearchPlugin;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.StoreSupplier;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.StoreDescriptor;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A builder helper class to create TSDB instances.
 */
public class TsdbBuilder {
  /**
   * The config instance used by this builder and any objects it creates.
   */
  private final Config config;

  /**
   * The metrics instance used by this builder and any objects it creates.
   */
  private final Metrics metrics;

  /**
   * The provided supplier that will provide a {@link net.opentsdb.storage.TsdbStore}
   * instance to the {@link net.opentsdb.core.TSDB} that this builder will create.
   */
  private Supplier<TsdbStore> storeSupplier;

  /**
   * The search plugin that the {@link net.opentsdb.core.TSDB} instance built by
   * this builder will use.
   */
  private SearchPlugin searchPlugin;

  /**
   * The realtime publisher that the {@link net.opentsdb.core.TSDB} instance
   * built by this builder will use.
   */
  private RTPublisher realtimePublisher;

  /**
   * Create a TsdbBuilder instance based on the provided config.
   * @param config The config the instance is to be based off
   * @return A newly created TsdbBuilder
   */
  public static TsdbBuilder createFromConfig(final Config config) {
    checkNotNull(config);

    final Metrics metrics = new Metrics(new MetricRegistry());
    final TsdbBuilder builder = new TsdbBuilder(config, metrics);

    StoreSupplier storeSupplier = new StoreSupplier(config,
        ServiceLoader.load(StoreDescriptor.class), metrics);

    builder.withStoreSupplier(storeSupplier)
            .withSearchPlugin(loadSearchPlugin(config))
            .withRealtimePublisher(loadRealtimePublisher(config));

    return builder;
  }

  /**
   * Load the realtime publisher that the config describes and return it.
   * @param config The config object to read from
   * @return The configured realtime publisher or the default discarding one.
   */
  private static RTPublisher loadRealtimePublisher(final Config config) {
    // load the realtime publisher plugin if enabled
    if (config.getBoolean("tsd.rtpublisher.enable")) {
      return PluginLoader.loadSpecificPlugin(
              config.getString("tsd.rtpublisher.plugin"), RTPublisher.class);
    }

    return defaultRealtimePublisher();
  }

  /**
   * Load the search plugin that the config describes and return it.
   * @param config The config object to read from
   * @return The configured search plugin or the default discarding one.
   */
  private static SearchPlugin loadSearchPlugin(final Config config) {
    // load the search plugin if enabled
    if (config.getBoolean("tsd.search.enable")) {
      return PluginLoader.loadSpecificPlugin(
              config.getString("tsd.search.plugin"), SearchPlugin.class);
    }

    return defaultSearchPlugin();
  }

  private static RTPublisher defaultRealtimePublisher() {
    return new DefaultRealtimePublisher();
  }

  private static SearchPlugin defaultSearchPlugin() {
    return new DefaultSearchPlugin();
  }

  /***
   * Create a new builder with the provided config and metrics instance.
   */
  public TsdbBuilder(final Config config,
                     final Metrics metrics) {
    this.config = checkNotNull(config);
    this.metrics = checkNotNull(metrics);

    searchPlugin = defaultSearchPlugin();
    realtimePublisher = defaultRealtimePublisher();
  }

  /**
   * Set the store supplier that will be used by the TSDB instance created by
   * this builder.
   * @param supplier The {@link net.opentsdb.storage.StoreSupplier} to use
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
    this.searchPlugin = checkNotNull(searchPlugin);
    return this;
  }

  /**
   * Set the realtime publisher that will be used by the TSDB instance
   * created by this builder.
   * @param realtimePublisher The {@link net.opentsdb.tsd.RTPublisher} to use
   * @return This instance
   */
  public TsdbBuilder withRealtimePublisher(final RTPublisher realtimePublisher) {
    this.realtimePublisher = checkNotNull(realtimePublisher);
    return this;
  }

  /**
   * Build the TSDB instance based on the previously given parameters.
   * @return A newly created {@link net.opentsdb.core.TSDB} instance
   */
  public TSDB build() {
    metrics.getRegistry().registerAll(realtimePublisher.metrics());
    metrics.getRegistry().registerAll(searchPlugin.metrics());

    return new TSDB(storeSupplier.get(), config, searchPlugin,
            realtimePublisher, metrics);
  }
}