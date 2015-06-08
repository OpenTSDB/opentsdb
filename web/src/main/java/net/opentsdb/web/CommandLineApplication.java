package net.opentsdb.web;

import static java.util.Arrays.asList;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import com.google.common.io.Closeables;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

public class CommandLineApplication {
  private final PrintStream outStream;
  private final ArgumentAcceptingOptionSpec<File> configSpec;
  private final ArgumentAcceptingOptionSpec<File> loggerConfigSpec;

  private final String command;
  private final String commandFormat;
  private final String shortDescription;

  protected CommandLineApplication(final String command,
                                   final String commandFormat,
                                   final String shortDescription,
                                   final OptionParser optionParser) {
    // Release an FD that we don't need or want
    Closeables.closeQuietly(System.in);

    this.command = command;
    this.commandFormat = commandFormat;
    this.shortDescription = shortDescription;

    this.outStream = System.err;

    optionParser.acceptsAll(asList("help", "h"),
        "Display this help and exit").forHelp();
    optionParser.acceptsAll(asList("verbose", "v"),
        "Print more logging messages and not just errors.");
    configSpec = optionParser.acceptsAll(asList("config", "c"),
        "Path to a configuration file.")
        .withRequiredArg()
        .ofType(File.class)
        .defaultsTo(new File(appHome(), "config/opentsdb"));
    loggerConfigSpec = optionParser.accepts("logger-config",
        "Path to a logback configuration file.")
        .withRequiredArg()
        .ofType(File.class)
        .defaultsTo(new File(appHome(), "config/logback.xml"));
  }

  protected void printError(final String errorMessage) {
    outStream.println(command + ": " + errorMessage);
    outStream.println("Try 'tsdb " + command + " --help' for more information");
  }

  /** Prints printHelp. */
  protected void printHelp(final OptionParser parser) {
    outStream.println("Usage: tsdb " + command + " " + commandFormat);
    outStream.println(shortDescription);
    outStream.println();

    try {
      parser.printHelpOn(outStream);
    } catch (IOException e) {
      throw new AssertionError("PrintStream (System.err) never throws");
    }

    outStream.println();

    System.exit(2);
  }

  private String appHome() {
    return System.getProperty("app.home");
  }

  protected ArgumentAcceptingOptionSpec<File> getConfigSpec() {
    return configSpec;
  }

  protected ArgumentAcceptingOptionSpec<File> getLoggerConfigSpec() {
    return loggerConfigSpec;
  }

  protected static void configureLogger(final File logConfigFile) {
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

    try {
      JoranConfigurator configurator = new JoranConfigurator();
      configurator.setContext(context);

      // Reset any defaults
      context.reset();

      configurator.doConfigure(logConfigFile);
    } catch (JoranException je) {
      // StatusPrinter will handle this
    }
    StatusPrinter.printInCaseOfErrorsOrWarnings(context);
  }
}
