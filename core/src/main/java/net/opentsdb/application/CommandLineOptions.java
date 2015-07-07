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

public class CommandLineOptions {
  private final OptionParser optionParser;

  private final ArgumentAcceptingOptionSpec<File> configSpec;
  private final ArgumentAcceptingOptionSpec<File> loggerConfigSpec;

  private OptionSet options;

  public CommandLineOptions() {
    this(new OptionParser());
  }

  public CommandLineOptions(final OptionParser optionParser) {
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

  public File configFile() {
    checkState(options != null, "Arguments have not been parsed yet. Call #parseOptions first.");
    return options.valueOf(configSpec);
  }

  public ConfigModule configModule() {
    return ConfigModule.fromFile(configFile());
  }

  public File loggerConfigFile() {
    checkState(options != null, "Arguments have not been parsed yet. Call #parseOptions first.");
    return options.valueOf(loggerConfigSpec);
  }

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

  public OptionSet parseOptions(final String[] args) {
    checkState(options == null, "Options have already been parsed");
    this.options = optionParser.parse(args);
    return options;
  }

  public boolean shouldPrintHelp() {
    checkState(options != null, "Arguments have not been parsed yet. Call #parseOptions first.");
    return options.has("help");
  }

  protected OptionParser optionParser() {
    return optionParser;
  }
}
