package net.opentsdb.uid;

import net.opentsdb.core.Const;

import com.google.common.base.Strings;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Enumerator for different types of UIDs
 */
public enum UniqueIdType {
  // Think long and hard before changing the identifier arguments bellow.
  // Changing any of them without updating UniqueIdType#fromValue bellow will
  // wreck all deployments.
  METRIC("metrics", Const.METRICS_WIDTH),
  TAGK("tagk", Const.TAG_NAME_WIDTH),
  TAGV("tagv", Const.TAG_VALUE_WIDTH);

  private final String identifier;

  public final short width;

  UniqueIdType(String identifier, short width) {
    checkArgument(!Strings.isNullOrEmpty(identifier), "Empty string as 'identifier' argument!");
    checkArgument(width > 0 && width <= 8, "Invalid width: %s", width);

    this.identifier = identifier;
    this.width = width;
  }

  /**
   * Attempts to convert the given string to an instance of this enum. This is
   * the reverse of {@link #toValue}.
   *
   * @param type The string to convert
   * @return a valid UniqueIdType if matched
   * @throws IllegalArgumentException if the string did not match a type
   */
  public static UniqueIdType fromValue(final String type) {
    if ("metric".equals(type.toLowerCase()) ||
        "metrics".equals(type.toLowerCase())) {
      return METRIC;
    } else if ("tagk".equals(type.toLowerCase())) {
      return TAGK;
    } else if ("tagv".equals(type.toLowerCase())) {
      return TAGV;
    } else {
      throw new IllegalArgumentException("Invalid type: " + type);
    }
  }

  /**
   * Returns a string that uniquely identifies the enum value and is safe to use
   * with external systems (databases). This is the reverse of {@link #fromValue}.
   */
  public String toValue() {
    return identifier;
  }
}
