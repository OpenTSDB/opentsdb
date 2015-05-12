
package net.opentsdb;

import net.opentsdb.idmanager.Assign;
import net.opentsdb.utils.KillingUncaughtHandler;

import java.util.Arrays;

public class TSDB {
  public static void main(final String[] args) {
    KillingUncaughtHandler.install();

    if (args.length < 1) {
      usage(args);
      return;
    }

    if ("id".equals(args[0])) {
      handleIdManager(args);
    }
  }

  private static void usage(final String[] args) {
  }

  private static void handleIdManager(final String[] args) {
    if ("assign".equals(args[1])) {
      Assign.main(Arrays.copyOfRange(args, 2, args.length));
    }
  }
}
