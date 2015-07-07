package net.opentsdb.application;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.auto.value.AutoValue;

import java.io.IOException;
import java.io.PrintStream;

@AutoValue
public abstract class CommandLineApplication {
  CommandLineApplication() {
  }

  public static Builder builder() {
    return new AutoValue_CommandLineApplication.Builder()
        .outputStream(System.err);
  }

  abstract String command();

  abstract String description();

  abstract String helpText();

  abstract PrintStream outputStream();

  public void printError(final String errorMessage) {
    outputStream().println(command() + ": " + errorMessage);
    outputStream().println("Try 'tsdb " + command() + " --help' for more information");
  }

  public void printHelpAndExit(final CommandLineOptions options) {
    outputStream().println("Usage: tsdb " + command() + ' ' + usage());
    outputStream().println(description());
    outputStream().println();

    try {
      options.optionParser().printHelpOn(outputStream());
    } catch (IOException e) {
      throw new AssertionError("PrintStream (System.err) never throws");
    }

    outputStream().println();
    outputStream().println(helpText());
    outputStream().println();

    System.exit(2);
  }

  abstract String usage();

  @AutoValue.Builder
  public abstract static class Builder {
    abstract CommandLineApplication autoBuild();

    public CommandLineApplication build() {
      final CommandLineApplication application = autoBuild();
      checkState(!isNullOrEmpty(application.command()));
      checkState(!isNullOrEmpty(application.description()));
      checkState(!isNullOrEmpty(application.helpText()));
      checkState(!isNullOrEmpty(application.usage()));
      return application;
    }

    public abstract Builder command(final String command);

    public abstract Builder description(final String description);

    public abstract Builder helpText(final String helpText);

    public abstract Builder outputStream(final PrintStream outputStream);

    public abstract Builder usage(final String usage);
  }
}
