package net.opentsdb.core;

import net.opentsdb.search.SearchPlugin;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class TsdbBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TsdbBuilder.class);

  private TsdbStore client;
  private Config config;
  private SearchPlugin searchPlugin;
  private RTPublisher realTimePublisher;

  public static TsdbBuilder createFromConfig(final Config config) {
    addPluginPath(config);

    TsdbBuilder builder = new TsdbBuilder();

    builder.withConfig(config)
            .withStoreSupplier(new StoreSupplier(config))
            .withSearchPlugin(loadSearchPlugin(config))
            .withRealTimePublisher(loadRealTimePublisher(config));

    return builder;
  }

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

  private static Optional<SearchPlugin> loadSearchPlugin(final Config config) {
    // load the search plugin if enabled
    if (config.getBoolean("tsd.search.enable")) {
      final SearchPlugin search = PluginLoader.loadSpecificPlugin(
              config.getString("tsd.search.plugin"), SearchPlugin.class);

      return Optional.of(search);
    }

    return Optional.absent();
  }

  private static Optional<RTPublisher> loadRealTimePublisher(final Config config) {
    // load the real time publisher plugin if enabled
    if (config.getBoolean("tsd.rtpublisher.enable")) {
      RTPublisher rt_publisher = PluginLoader.loadSpecificPlugin(
              config.getString("tsd.rtpublisher.plugin"), RTPublisher.class);

      return Optional.of(rt_publisher);
    }

    return Optional.absent();
  }

  public TsdbBuilder withConfig(final Config config) {
    this.config = checkNotNull(config);
    return this;
  }

  public TsdbBuilder withStoreSupplier(final StoreSupplier supplier) {
    withStore(supplier.get());
    return this;
  }

  public TsdbBuilder withStore(final TsdbStore store) {
    this.client = checkNotNull(store);
    return this;
  }

  public TsdbBuilder withSearchPlugin(final SearchPlugin searchPlugin) {
    this.searchPlugin = searchPlugin;
    return this;
  }

  public TsdbBuilder withSearchPlugin(final Optional<SearchPlugin> searchPlugin) {
    withSearchPlugin(searchPlugin.get());
    return this;
  }

  public TsdbBuilder withRealTimePublisher(final RTPublisher realTimePublisher) {
    this.realTimePublisher = realTimePublisher;
    return this;
  }

  public TsdbBuilder withRealTimePublisher(final Optional<RTPublisher> realTimePublisher) {
    withRealTimePublisher(realTimePublisher.get());
    return this;
  }

  public TSDB build() {
    return new TSDB(client, config, searchPlugin, realTimePublisher);
  }
}