package net.opentsdb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default exception handler for threads. Will tell the JVM to exit when an exception bubbles all
 * the way up to a thread without being handled.
 * <p/>
 * See {@link java.lang.Thread.UncaughtExceptionHandler} for the exact order the handlers are
 * executed in.
 */
public class KillingUncaughtHandler implements Thread.UncaughtExceptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(KillingUncaughtHandler.class);

  public static void install() {
    final Thread.UncaughtExceptionHandler previousHandler = Thread.getDefaultUncaughtExceptionHandler();
    if (previousHandler instanceof KillingUncaughtHandler) {
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
