package net.opentsdb.uid;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;

/**
 * Enumerator for different types of IDs.
 */
public enum IdType {
  // Think long and hard before changing the identifier arguments bellow.
  // Changing any of them without updating IdType#fromValue bellow will
  // wreck all deployments.
  METRIC("metrics"),
  TAGK("tagk"),
  TAGV("tagv");

  private final String identifier;

  IdType(String identifier) {
    checkArgument(!Strings.isNullOrEmpty(identifier), "Empty string as 'identifier' argument!");

    this.identifier = identifier;
  }

  /**
   * Attempts to convert the given string to an instance of this enum. This is the reverse of {@link
   * #toValue}.
   *
   * @param type The string to convert
   * @return a valid IdType if matched
   * @throws IllegalArgumentException if the string did not match a type
   */
  public static IdType fromValue(final String type) {
    if ("metric".equals(type.toLowerCase())
        || "metrics".equals(type.toLowerCase())) {
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
   * Returns a string that uniquely identifies the enum value and is safe to use with external
   * systems (databases). This is the reverse of {@link #fromValue}.
   */
  public String toValue() {
    return identifier;
  }
}
