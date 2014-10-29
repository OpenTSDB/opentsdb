package net.opentsdb.uid;

import net.opentsdb.core.Const;

import com.google.common.base.Strings;

import static com.google.common.base.Preconditions.checkArgument;

/** Enumerator for different types of UIDS @since 2.0 */
public enum UniqueIdType {
  METRIC(Const.METRICS_QUAL, Const.METRICS_WIDTH),
  TAGK(Const.TAG_NAME_QUAL, Const.TAG_NAME_WIDTH),
  TAGV(Const.TAG_VALUE_QUAL, Const.TAG_VALUE_WIDTH);

  public final String qualifier;
  public final short width;

  UniqueIdType(String qualifier, short width) {
    checkArgument(!Strings.isNullOrEmpty(qualifier), "Empty string as 'qualifier' argument!");
    checkArgument(width > 0 && width <= 8, "Invalid width: %s", width);

    this.qualifier = qualifier;
    this.width = width;
  }
}
