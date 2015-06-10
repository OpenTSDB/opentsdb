package net.opentsdb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;

/**
 * A default exception handler for threads. Will tell the JVM to exit when an exception bubbles all
 * the way up to a thread without being handled.
 *
 * <p>See {@link UncaughtExceptionHandler} for the exact order the handlers are executed in.
 */
public class KillingUncaughtHandler implements UncaughtExceptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(KillingUncaughtHandler.class);

  public static void install() {
    final UncaughtExceptionHandler currentHandler = Thread.getDefaultUncaughtExceptionHandler();
    if (currentHandler instanceof KillingUncaughtHandler) {
      return;
    }
    final KillingUncaughtHandler oomHandler = new KillingUncaughtHandler();
    Thread.setDefaultUncaughtExceptionHandler(oomHandler);
  }

  @Override
  public void uncaughtException(final Thread thread, final Throwable ex) {
    LOG.error("Exiting after uncaught exception", ex);
    System.exit(42);
  }
}
