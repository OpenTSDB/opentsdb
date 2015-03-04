
package net.opentsdb.idmanager;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.typesafe.config.ConfigException;
import dagger.ObjectGraph;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import net.opentsdb.core.InvalidConfigException;
import net.opentsdb.core.UniqueIdClient;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdException;
import net.opentsdb.uid.IdUtils;
import net.opentsdb.uid.UniqueIdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

/**
 * Command line tool to assign new IDs
 */
public final class Assign {
  private static final Logger LOG = LoggerFactory.getLogger(Assign.class);

  private final UniqueIdClient idClient;

  /** Prints printHelp. */
  private static void printHelp(final OptionParser parser) {
    System.err.println("Usage: tsdb id assign [OPTIONS] <TYPE> [NAME]...");
    System.err.println("Create IDs for NAME(s), or names read from standard" +
        " input of type TYPE.");
    System.err.println();

    try {
      parser.printHelpOn(System.err);
    } catch (IOException e) {
      throw new AssertionError("PrintStream (System.err) never throws");
    }

    System.err.println();
    System.err.println("With no NAME, or when NAME is -, read standard input.");
    System.err.println();

    System.exit(2);
  }

  private static void printError(final String errorMessage) {
    System.err.println("assign: " + errorMessage);
    System.err.println("Try 'tsdb id assign --help' for more information");
  }

  @Inject
  Assign(final UniqueIdClient idClient) {
    this.idClient = checkNotNull(idClient);
  }

  public static void main(final String[] args) {
    OptionParser parser = new OptionParser();

    parser.acceptsAll(asList("help", "h"),
        "display this help and exit").forHelp();
    parser.acceptsAll(asList("verbose", "v"),
        "Print more logging messages and not just errors.");
    ArgumentAcceptingOptionSpec<File> configSpec = parser.acceptsAll(asList("config", "c"),
        "Path to a configuration file (default: Searches for file see docs).")
        .withRequiredArg()
        .ofType(File.class)
        .defaultsTo(new File(appHome(), "config/opentsdb.conf"));

    try {
      final OptionSet options = parser.parse(args);

      if (options.has("help")) {
        printHelp(parser);
      }

      final File configFile = options.valueOf(configSpec);

      final List<?> nonOptionArguments = options.nonOptionArguments();

      final UniqueIdType type = type(nonOptionArguments);
      final ImmutableSet<String> names = ImmutableSet.copyOf(
          Arrays.copyOfRange(args, 1, args.length));

      final ObjectGraph objectGraph = ObjectGraph.create(new AssignModule(configFile));
      final TsdbStore store = objectGraph.get(TsdbStore.class);
      final Assign assign = objectGraph.get(Assign.class);

      final List<Deferred<Void>> assignments =
          Lists.newArrayListWithCapacity(names.size());

      for (final String name : names) {
        assignments.add(assign.assign(name, type));
      }

      Deferred.group(assignments).joinUninterruptibly();
      store.shutdown().joinUninterruptibly();
    } catch (IllegalArgumentException e) {
      printError(e.getMessage());
      System.exit(42);
    } catch (InvalidConfigException e) {
      System.err.println(e.getMessage());
      System.exit(42);
    } catch (ConfigException e) {
      System.err.println(e.getMessage());
      System.exit(42);
    } catch (OptionException e) {
      printError(e.getMessage());
      System.exit(42);
    } catch (Exception e) {
      LOG.error("Fatal error while assigning id", e);
      System.exit(42);
    }
  }

  private static String appHome() {
    return System.getProperty("app.home");
  }

  private static UniqueIdType type(final List<?> nonOptionArguments) {
    try {
      String stringType = nonOptionArguments.get(0).toString();
      return UniqueIdType.fromValue(stringType);
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException("Missing identifier type to assign");
    }
  }

  private Deferred<Void> assign(final String name, final UniqueIdType type) {
    return idClient.createId(type, name)
        .addCallbacks(new LogNewIdCB(name, type), new LogErrorCB(name, type));
  }

  private static class LogNewIdCB implements Callback<Void, byte[]> {
    private final String name;
    private final UniqueIdType type;

    public LogNewIdCB(final String name, final UniqueIdType type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public Void call(final byte[] id) {
      LOG.info("{} {}: {}", type, name, IdUtils.uidToLong(id));
      return null;
    }
  }

  private static class LogErrorCB implements Callback<Object, Exception> {
    private final String name;
    private final UniqueIdType type;

    public LogErrorCB(final String name, final UniqueIdType type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public Object call(final Exception e) throws Exception {
      if (e instanceof IdException) {
        System.err.println(e.getMessage());
      } else {
        LOG.error("{} {}: {}", name, type, e.getMessage(), e);
      }

      return null;
    }
  }
}
