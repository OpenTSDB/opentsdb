package net.opentsdb.plugins;

import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Simply logs plugin errors when they're thrown by attaching as an errorback. Without this,
 * exceptions will just disappear (unless logged by the plugin) since we don't wait for a result.
 */
public final class PluginError implements FutureCallback<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(PluginError.class);

  private final Plugin plugin;

  public PluginError(Plugin plugin) {
    this.plugin = plugin;
  }

  @Override
  public void onSuccess(@Nullable final Object result) {
    // We only care about errors so ignore successful ones.
  }

  @Override
  public void onFailure(final Throwable t) {
    LOG.error("Received an exception from {}", plugin, t);
  }
}
