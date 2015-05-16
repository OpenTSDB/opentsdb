package net.opentsdb.utils;

import com.typesafe.config.ConfigValue;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Exception thrown when something about a specific {@link
 * com.typesafe.config.ConfigValue} is wrong.
 */
public class InvalidConfigException extends RuntimeException {
  public InvalidConfigException(final ConfigValue value,
                                final String message) {
    super(value.origin().description() + ": " + checkNotNull(message));
  }
}
