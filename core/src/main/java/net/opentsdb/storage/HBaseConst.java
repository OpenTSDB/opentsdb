package net.opentsdb.storage;

import com.google.common.base.Charsets;

import java.nio.charset.Charset;

/**
 * A dump for all HBase constants that have not been fully migrated to the HBase
 * project.
 *
 * All members of this class should be migrated to the HBase subproject once the
 * places that uses them have abstractions that does not need them.
 */
@Deprecated
public class HBaseConst {
  /**
   * Charset used to convert Strings to byte arrays and back.
   */
  public static final Charset CHARSET = Charsets.ISO_8859_1;

  public static class TSMeta {
    /**
     * The counter column qualifier
     */
    public static final byte[] COUNTER_QUALIFIER = "ts_ctr".getBytes(CHARSET);
    /**
     * The meta data family
     */
    public static final byte[] FAMILY = "name".getBytes(CHARSET);
    /**
     * The meta data column qualifier
     */
    public static final byte[] META_QUALIFIER = "ts_meta".getBytes(CHARSET);
  }
}
