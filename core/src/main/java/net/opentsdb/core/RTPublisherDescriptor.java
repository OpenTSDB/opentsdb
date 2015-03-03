package net.opentsdb.core;

import com.typesafe.config.Config;

public abstract class RTPublisherDescriptor {
  /**
   * Called to initialize the plugin. Implementations are responsible for
   * setting up any IO they need as well as starting any required background
   * threads. <b>Note:</b> Implementations should throw exceptions if they can't
   * start up properly. The TSD will then shutdown so the operator can fix the
   * problem. Please use IllegalArgumentException for configuration issues.
   *
   * @param config The global config for the entire instance
   * @throws IllegalArgumentException if required configuration parameters are
   *                                  missing
   * @throws Exception                if something else goes wrong
   */
  public abstract RTPublisher create(Config config) throws Exception;
}
