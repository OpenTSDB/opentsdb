package net.opentsdb.uid;

import static com.google.common.base.Preconditions.checkArgument;

import net.opentsdb.core.Const;

import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.xml.bind.DatatypeConverter;

public class IdUtils {
  private static final Logger LOG = LoggerFactory.getLogger(IdUtils.class);

  private IdUtils() {
  }

  /**
   * Appends the given UID to the given string buffer, followed by "\\E".
   *
   * @param buf The buffer to append
   * @param id The UID to add as a binary regex pattern
   * @since 2.1
   */
  public static void addIdToRegexp(final StringBuilder buf, final byte[] id) {
    boolean backslash = false;
    for (final byte b : id) {
      buf.append((char) (b & 0xFF));
      if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
        // So we just terminated the quoted section because we just added \E
        // to `buf'.  So let's put a literal \E now and start quoting again.
        buf.append("\\\\E\\Q");
      } else {
        backslash = b == '\\';
      }
    }
    buf.append("\\E");
  }

  /**
   * Extracts a list of tagk/tagv pairs from a tsuid
   *
   * @param tsuid The tsuid to parse
   * @return A list of tagk/tagv UID pairs
   * @throws IllegalArgumentException if the TSUID is malformed
   * @since 2.0
   */
  public static List<byte[]> getTagPairsFromTSUID(final byte[] tsuid) {
    if (tsuid == null) {
      throw new IllegalArgumentException("Missing TSUID");
    }
    if (tsuid.length <= Const.METRICS_WIDTH) {
      throw new IllegalArgumentException(
          "TSUID is too short, may be missing tags");
    }

    final List<byte[]> tags = new ArrayList<byte[]>();
    final int pair_width = Const.TAG_NAME_WIDTH + Const.TAG_VALUE_WIDTH;

    // start after the metric then iterate over each tagk/tagv pair
    for (int i = Const.METRICS_WIDTH; i < tsuid.length; i += pair_width) {
      if (i + pair_width > tsuid.length) {
        throw new IllegalArgumentException(
            "The TSUID appears to be malformed, improper tag width");
      }
      tags.add(Arrays.copyOfRange(tsuid, i, i + pair_width));
    }
    return tags;
  }

  /**
   * Extracts a list of tagks and tagvs as individual values in a list
   *
   * @param tsuid The tsuid to parse
   * @return A list of tagk/tagv UIDs alternating with tagk, tagv, tagk, tagv
   * @throws IllegalArgumentException if the TSUID is malformed
   * @since 2.1
   */
  public static List<byte[]> getTagsFromTSUID(final String tsuid) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Missing TSUID");
    }
    if (tsuid.length() <= Const.METRICS_WIDTH * 2) {
      throw new IllegalArgumentException(
          "TSUID is too short, may be missing tags");
    }

    final List<byte[]> tags = new ArrayList<byte[]>();
    final int pair_width = (Const.TAG_NAME_WIDTH * 2) + (Const.TAG_VALUE_WIDTH * 2);

    // start after the metric then iterate over each tagk/tagv pair
    for (int i = Const.METRICS_WIDTH * 2; i < tsuid.length(); i += pair_width) {
      if (i + pair_width > tsuid.length()) {
        throw new IllegalArgumentException(
            "The TSUID appears to be malformed, improper tag width");
      }
      String tag = tsuid.substring(i, i + (Const.TAG_NAME_WIDTH * 2));
      tags.add(stringToUid(tag));
      tag = tsuid.substring(i + (Const.TAG_NAME_WIDTH * 2), i + pair_width);
      tags.add(stringToUid(tag));
    }
    return tags;
  }

  /**
   * Converts a Long to a byte array with the proper UID width
   *
   * @param uid The UID to convert
   * @param width The width of the UID in bytes
   * @return The UID as a byte array
   * @throws IllegalStateException if the UID is larger than the width would
   * @throws IllegalArgumentException if width <= 0. allow
   * @since 2.1
   */
  public static byte[] longToUID(final long uid, final short width) {
    checkArgument(width > 0, "width can't be negative");

    final byte[] padded = Longs.toByteArray(uid);

    // Verify that we're going to drop bytes that are 0.
    for (int i = 0; i < padded.length - width; i++) {
      if (padded[i] != 0) {
        final String message = "UID " + Long.toString(uid) + " was too large for " + width
                               + " bytes";
        LOG.error("OMG {}", message);
        throw new IllegalStateException(message);
      }
    }

    // Shrink the ID on the requested number of bytes.
    return Arrays.copyOfRange(padded, padded.length - width, padded.length);
  }

  /**
   * Converts a hex string to a byte array If the {@code uid} is less than {@code uid_length * 2}
   * characters wide, it will be padded with 0s to conform to the spec. E.g. if the tagk width is 3
   * and the given {@code uid} string is "1", the string will be padded to "000001" and then
   * converted to a byte array to reach 3 bytes. All {@code uid}s are padded to 1 byte. If given
   * "1", and {@code uid_length} is 0, the uid will be padded to "01" then converted.
   *
   * @param uid The UID to convert
   * @return The UID as a byte array
   * @throws NullPointerException if the ID was null
   * @throws IllegalArgumentException if the string is not valid hex
   * @since 2.0
   */
  public static byte[] stringToUid(final String uid) {
    return stringToUid(uid, (short) 0);
  }

  /**
   * Converts a hex string to a byte array If the {@code uid} is less than {@code uidLength * 2}
   * characters wide, it will be padded with 0s to conform to the spec. E.g. if the tagk width is 3
   * and the given {@code uid} string is "1", the string will be padded to "000001" and then
   * converted to a byte array to reach 3 bytes. All {@code uid}s are padded to 1 byte. If given
   * "1", and {@code uidLength} is 0, the uid will be padded to "01" then converted.
   *
   * @param uid The UID to convert
   * @param uidLength An optional length, in bytes, that the UID must conform to. Set to 0 if not
   * used.
   * @return The UID as a byte array
   * @throws NullPointerException if the ID was null
   * @throws IllegalArgumentException if the string is not valid hex
   * @since 2.0
   */
  public static byte[] stringToUid(final String uid, final short uidLength) {
    if (uid == null || uid.isEmpty()) {
      throw new IllegalArgumentException("UID was empty");
    }
    String id = uid;
    if (uidLength > 0) {
      while (id.length() < uidLength * 2) {
        id = "0" + id;
      }
    } else {
      if (id.length() % 2 > 0) {
        id = "0" + id;
      }
    }
    return DatatypeConverter.parseHexBinary(id);
  }

  /**
   * Converts a UID to an integer value. The array must be the same length as uidLength or an
   * exception will be thrown.
   *
   * @param uid The byte array to convert
   * @param uidLength Length the array SHOULD be according to the UID config
   * @return The UID converted to an integer
   * @throws IllegalArgumentException if the length of the byte array does not match the uidLength
   * value
   * @since 2.1
   */
  public static long uidToLong(final byte[] uid, final short uidLength) {
    if (uid.length != uidLength) {
      throw new IllegalArgumentException("UID was " + uid.length
                                         + " bytes long but expected to be " + uidLength);
    }

    final byte[] uid_raw = new byte[8];
    System.arraycopy(uid, 0, uid_raw, 8 - uidLength, uidLength);
    return Longs.fromByteArray(uid_raw);
  }

  /**
   * Converts a UID to an integer value. The array must be the same length as uid_length or an
   * exception will be thrown.
   *
   * @param uid The byte array to convert
   * @return The UID converted to an integer
   * @throws IllegalArgumentException if the length of the byte array does not match the uid_length
   * value
   * @since 2.1
   */
  public static long uidToLong(final byte[] uid) {
    final byte[] uid_raw = new byte[8];
    System.arraycopy(uid, 0, uid_raw, 8 - uid.length, uid.length);
    return Longs.fromByteArray(uid_raw);
  }

  /**
   * Converts a byte array to a hex encoded, upper case string with padding
   *
   * @param uid The ID to convert
   * @return the UID as a hex string
   * @throws NullPointerException if the ID was null
   * @since 2.0
   */
  public static String uidToString(final byte[] uid) {
    return DatatypeConverter.printHexBinary(uid);
  }
}
