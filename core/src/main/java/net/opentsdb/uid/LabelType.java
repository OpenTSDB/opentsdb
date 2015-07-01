package net.opentsdb.uid;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;

/**
 * Enumerator for different types of IDs.
 */
public enum LabelType {
  // Think long and hard before changing the identifier arguments bellow.
  // Changing any of them without updating LabelType#fromValue bellow will
  // wreck all deployments.
  METRIC("metric"),
  TAGK("tagk"),
  TAGV("tagv");

  private final String identifier;

  LabelType(String identifier) {
    checkArgument(!Strings.isNullOrEmpty(identifier), "Empty string as 'identifier' argument!");

    this.identifier = identifier;
  }

  /**
   * Attempts to convert the given string to an instance of this enum. This is the reverse of {@link
   * #toValue}.
   *
   * @param type The string to convert
   * @return a valid LabelType if matched
   * @throws IllegalArgumentException if the string did not match a type
   */
  public static LabelType fromValue(final String type) {
    final String lowerCaseType = type.toLowerCase();

    switch (lowerCaseType) {
      case "metric":
        return METRIC;
      case "tagk":
        return TAGK;
      case "tagv":
        return TAGV;
      default:
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
