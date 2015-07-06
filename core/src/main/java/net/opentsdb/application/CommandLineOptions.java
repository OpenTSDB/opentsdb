package net.opentsdb.application;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

import net.opentsdb.core.ConfigModule;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.File;

public class CommandLineOptions {
  private final OptionParser optionParser;
  private final ArgumentAcceptingOptionSpec<File> configSpec;

  private OptionSet options;

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

  public OptionSet parseOptions(final String[] args) {
    checkState(options == null, "Options have already been parsed");
    this.options = optionParser.parse(args);
    return options;
  }

  public boolean shouldPrintHelp() {
    checkState(options != null, "Arguments have not been parsed yet. Call #parseOptions first.");
    return options.has("help");
  }
}
