package net.opentsdb.storage.hbase;

import java.nio.charset.Charset;

import com.google.common.base.Charsets;

public class HBaseConst {
  /**
   * Charset used to convert Strings to byte arrays and back.
   */
  public static final Charset CHARSET = Charsets.ISO_8859_1;

  static class UniqueId {
    /**
     * The column family that maps names to IDs
     */
    static final byte[] ID_FAMILY = "id".getBytes(CHARSET);

    /**
     * Row key of the special row used to track the max ID already assigned.
     */
    static final byte[] MAXID_ROW = {0};
  }
}
