package net.opentsdb.core;

import com.stumbleupon.async.Deferred;

public interface TSDBPlugin {
  /**
   * An ID for the plugin in case multiple plugins of the same class are 
   * loaded.
   * 
   * @return A non-null string ID.
   */
  public String id();
  
  /**
   * Called by TSDB to initialize the plugin asynchronously. Almost every 
   * implementation will have to override this to load settings from the TSDB's
   * config file.
   * 
   * <b>WARNING:</b> Initialization order is not guaranteed. If you depend on
   * another plugin to be loaded by the TSDB, load on the first call to a method.
   * TODO - ^^ that sucks.
   * 
   * Implementations are responsible for setting up any IO they need as well
   * as starting any required background threads.
   * 
   * <b>Note:</b> Implementations should throw exceptions if they can't start
   * up properly. The TSD will then shutdown so the operator can fix the
   * problem. Please use IllegalArgumentException for configuration issues.
   * If it can't startup for another reason, return an Exception in the deferred.
   * 
   * @param tsdb The parent TSDB object.
   * @return A non-null deferred for the TSDB to wait on to confirm 
   * initialization. The deferred should resolve to a {@code null} on successful 
   * init or an exception on failure.
   * 
   * @throws IllegalArgumentException if required configuration parameters are
   * missing.
   */
  public Deferred<Object> initialize(final TSDB tsdb);
  
  /**
   * Called to gracefully shutdown the plugin. Implementations should close
   * any IO they have open and release resources.
   * 
   * @return A non-null deferred for the TSDB to wait on to confirm shutdown.
   * The deferred should resolve to a {@code null} on successful shutdown or an 
   * exception on failure.
   */
  public Deferred<Object> shutdown();
  
  /**
   * Should return the version of this plugin in the format:
   * MAJOR.MINOR.MAINT, e.g. 2.0.1. The MAJOR version should match the major
   * version of OpenTSDB the plugin is meant to work with.
   * @return A version string used to log the loaded version
   */
  public String version();
}
