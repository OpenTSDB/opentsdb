package net.opentsdb.core;

import com.typesafe.config.ConfigValue;

import static com.google.common.base.Preconditions.checkNotNull;

public class InvalidConfigException extends RuntimeException {
  public InvalidConfigException(final ConfigValue value,
                                final String message) {
    super(value.origin().description() + ": " + checkNotNull(message));
  }
}
