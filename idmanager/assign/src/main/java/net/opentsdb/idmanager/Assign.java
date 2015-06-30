package net.opentsdb.idmanager;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

import net.opentsdb.core.ConfigModule;
import net.opentsdb.core.IdClient;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdException;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.InvalidConfigException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.typesafe.config.ConfigException;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Command line tool to assign new IDs.
 */
public final class Assign {
  private static final Logger LOG = LoggerFactory.getLogger(Assign.class);

  private final IdClient idClient;

  @Inject
  Assign(final IdClient idClient) {
    this.idClient = checkNotNull(idClient);
  }

  /** Prints printHelp. */
  private static void printHelp(final OptionParser parser) {
    System.err.println("Usage: tsdb id assign [OPTIONS] <TYPE> [NAME]...");
    System.err.println("Create IDs for NAME(s), or names read from standard"
                       + " input of type TYPE.");
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

  /**
   * Entry-point for the assign application. The assign program is normally not executed directly
   * but rather through the main project.
   *
   * @param args The command-line arguments
   */
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
        .defaultsTo(new File(appHome(), "config/opentsdb"));

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

      final AssignComponent assignComponent = DaggerAssignComponent.builder()
          .configModule(new ConfigModule(configFile))
          .build();

      final TsdbStore store = assignComponent.store();
      final Assign assign = assignComponent.assign();

      final List<ListenableFuture<LabelId>> assignments =
          Lists.newArrayListWithCapacity(names.size());

      for (final String name : names) {
        assignments.add(assign.assign(name, type));
      }

      Futures.allAsList(assignments).get();
      store.close();
    } catch (IllegalArgumentException | OptionException e) {
      printError(e.getMessage());
      System.exit(42);
    } catch (InvalidConfigException | ConfigException e) {
      System.err.println(e.getMessage());
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

  private ListenableFuture<LabelId> assign(final String name, final UniqueIdType type) {
    final ListenableFuture<LabelId> id = idClient.createId(type, name);
    Futures.addCallback(id, new LogNewIdCallback(name, type));
    return id;
  }

  private static class LogNewIdCallback implements FutureCallback<LabelId> {
    private final String name;
    private final UniqueIdType type;

    public LogNewIdCallback(final String name, final UniqueIdType type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public void onSuccess(@Nullable final LabelId id) {
      LOG.info("{} {}: {}", type, name, id);
    }

    @Override
    public void onFailure(final Throwable throwable) {
      if (throwable instanceof IdException) {
        System.err.println(throwable.getMessage());
      } else {
        LOG.error("{} {}: {}", name, type, throwable.getMessage(), throwable);
      }
    }
  }
}
