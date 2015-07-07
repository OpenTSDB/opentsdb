package net.opentsdb.idmanager;

import static com.google.common.base.Preconditions.checkNotNull;

import net.opentsdb.application.CommandLineApplication;
import net.opentsdb.application.CommandLineOptions;
import net.opentsdb.core.LabelClient;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelException;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.LabelType;
import net.opentsdb.utils.InvalidConfigException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.typesafe.config.ConfigException;
import joptsimple.OptionException;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Command line tool to assign new IDs.
 */
public final class Assign {
  private static final Logger LOG = LoggerFactory.getLogger(Assign.class);

  private final LabelClient labelClient;

  @Inject
  Assign(final LabelClient labelClient) {
    this.labelClient = checkNotNull(labelClient);
  }

  /**
   * Entry-point for the assign application. The assign program is normally not executed directly
   * but rather through the main project.
   *
   * @param args The command-line arguments
   */
  public static void main(final String[] args) {
    final CommandLineApplication application = CommandLineApplication.builder()
        .command("id assign")
        .usage("[OPTIONS] <TYPE> [NAME]...")
        .description("Create IDs for NAME(s), or names read from standard input of type TYPE.")
        .helpText("With no NAME, or when NAME is -, read standard input.")
        .build();

    final CommandLineOptions cmdOptions = new CommandLineOptions();

    try {
      final OptionSet options = cmdOptions.parseOptions(args);

      if (cmdOptions.shouldPrintHelp()) {
        application.printHelpAndExit(cmdOptions);
      }

      final AssignComponent assignComponent = DaggerAssignComponent.builder()
          .configModule(cmdOptions.configModule())
          .build();

      final List<?> nonOptionArguments = options.nonOptionArguments();

      final LabelType type = type(nonOptionArguments);
      final ImmutableSet<String> names = ImmutableSet.copyOf(
          Arrays.copyOfRange(args, 1, args.length));

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
      application.printError(e.getMessage());
      System.exit(42);
    } catch (InvalidConfigException | ConfigException e) {
      System.err.println(e.getMessage());
      System.exit(42);
    } catch (Exception e) {
      LOG.error("Fatal error while assigning id", e);
      System.exit(42);
    }
  }

  private static LabelType type(final List<?> nonOptionArguments) {
    try {
      String stringType = nonOptionArguments.get(0).toString();
      return LabelType.fromValue(stringType);
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException("Missing identifier type to assign");
    }
  }

  private ListenableFuture<LabelId> assign(final String name, final LabelType type) {
    final ListenableFuture<LabelId> id = labelClient.createId(type, name);
    Futures.addCallback(id, new LogNewIdCallback(name, type));
    return id;
  }

  private static class LogNewIdCallback implements FutureCallback<LabelId> {
    private final String name;
    private final LabelType type;

    public LogNewIdCallback(final String name, final LabelType type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public void onSuccess(@Nullable final LabelId id) {
      LOG.info("{} {}: {}", type, name, id);
    }

    @Override
    public void onFailure(final Throwable throwable) {
      if (throwable instanceof LabelException) {
        System.err.println(throwable.getMessage());
      } else {
        LOG.error("{} {}: {}", name, type, throwable.getMessage(), throwable);
      }
    }
  }
}
