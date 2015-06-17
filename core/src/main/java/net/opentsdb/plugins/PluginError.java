package net.opentsdb.plugins;

import com.stumbleupon.async.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simply logs plugin errors when they're thrown by attaching as an errorback. Without this,
 * exceptions will just disappear (unless logged by the plugin) since we don't wait for a result.
 */
public final class PluginError implements Callback<Object, Exception> {
  private static final Logger LOG = LoggerFactory.getLogger(PluginError.class);

  private final Plugin plugin;

  public PluginError(Plugin plugin) {
    this.plugin = plugin;
  }

  @Override
  public Object call(final Exception e) throws Exception {
    LOG.error("Received an exception from {}", plugin, e);
    return null;
  }
}
