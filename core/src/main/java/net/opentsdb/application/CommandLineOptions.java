package net.opentsdb.application;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

import net.opentsdb.core.ConfigModule;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * An instance of this class knows has to parse command line arguments and defines some common
 * behavior such as which config files to load by default and how they can be overridden.
 */
public class CommandLineOptions {
  private final OptionParser optionParser;

  private final ArgumentAcceptingOptionSpec<File> configSpec;
  private final ArgumentAcceptingOptionSpec<File> loggerConfigSpec;

  private OptionSet options;

  public CommandLineOptions() {
    this(new OptionParser());
  }

  /**
   * Create a new instance that uses the provided option parser to parse arguments read from the
   * command line.
   */
  protected CommandLineOptions(final OptionParser optionParser) {
    this.optionParser = checkNotNull(optionParser);

    optionParser.acceptsAll(asList("help", "h"),
        "display this help and exit").forHelp();
    optionParser.acceptsAll(asList("verbose", "v"),
        "Print more logging messages and not just errors.");
    configSpec = optionParser.acceptsAll(asList("config", "c"),
        "Path to a configuration file (default: Searches for file see docs).")
        .withRequiredArg()
        .ofType(File.class)
        .defaultsTo(new File(appHome(), "config/opentsdb"));
    loggerConfigSpec = optionParser.accepts("logger-config",
        "Path to a logback configuration file.")
        .withRequiredArg()
        .ofType(File.class)
        .defaultsTo(new File(appHome(), "config/logback.xml"));
  }

  private String appHome() {
    return System.getProperty("app.home");
  }

  /**
   * Reads the configuration file specified in the command line arguments.
   *
   * @return The file from which the configuration should be read from
   * @throws IllegalStateException if the command line arguments have not been parsed yet
   * @see #parseOptions(String[]) for how to parse the arguments
   */
  public File configFile() {
    checkState(options != null, "Arguments have not been parsed yet. Call #parseOptions first.");
    return options.valueOf(configSpec);
  }

  /**
   * Create a {@link ConfigModule} that provides a configuration the is based on the configuration
   * file returned by {@link #configFile()}.
   *
   * @see #configFile()
   */
  public ConfigModule configModule() {
    return ConfigModule.fromFile(configFile());
  }

  /**
   * Reads the logger configuration file specified in the command line arguments.
   *
   * @return The file from which the logback logger should read its configuration from
   * @throws IllegalStateException if the command line arguments have not been parsed yet
   * @see #parseOptions(String[]) for how to parse the arguments
   */
  public File loggerConfigFile() {
    checkState(options != null, "Arguments have not been parsed yet. Call #parseOptions first.");
    return options.valueOf(loggerConfigSpec);
  }

  /**
   * Configure the logback logging infrastructure based on the logback config file specified in the
   * command line arguments or the default configuration file if none was specified.
   *
   * @see #loggerConfigFile()
   */
  public void configureLogger() {
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

    try {
      JoranConfigurator configurator = new JoranConfigurator();
      configurator.setContext(context);

      // Reset any defaults
      context.reset();

      configurator.doConfigure(loggerConfigFile());
    } catch (JoranException je) {
      // StatusPrinter will handle this
    }
    StatusPrinter.printInCaseOfErrorsOrWarnings(context);
  }

  /**
   * Parse the provided string array as command line arguments. The {@code args} are usually the
   * parameter to the {@code public static void main(String[] args)} method.
   */
  public OptionSet parseOptions(final String[] args) {
    checkState(options == null, "Options have already been parsed");
    this.options = optionParser.parse(args);
    return options;
  }

  /**
   * Check if the command line arguments contained the help argument.
   *
   * @return {@code true} if the help should be printed, {@code false} if not
   */
  public boolean shouldPrintHelp() {
    checkState(options != null, "Arguments have not been parsed yet. Call #parseOptions first.");
    return options.has("help");
  }

  protected OptionParser optionParser() {
    return optionParser;
  }
}
