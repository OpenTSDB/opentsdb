// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import net.opentsdb.HBaseException;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;

/**
 * Command line tool to manipulate UIDs.
 * Can be used to find or assign UIDs.
 */
final class UidManager {

  private static final Logger LOG = LoggerFactory.getLogger(UidManager.class);

  /** Function used to convert a String to a byte[]. */
  private static final Method toBytes;
  /** Function used to convert a byte[] to a String. */
  private static final Method fromBytes;
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET;
  /** The single column family used by this class. */
  private static final byte[] ID_FAMILY;
  /** The single column family used by this class. */
  private static final byte[] NAME_FAMILY;
  static {
    final Class<UniqueId> uidclass = UniqueId.class;
    try {
      // Those are all implementation details so they're not part of the
      // interface.  We access them anyway using reflection.  I think this
      // is better than marking those public and adding a javadoc comment
      // "THIS IS INTERNAL DO NOT USE".  If only Java had C++'s "friend" or
      // a less stupid notion of a package.
      Field f;
      f = uidclass.getDeclaredField("CHARSET");
      f.setAccessible(true);
      CHARSET = (Charset) f.get(null);
      f = uidclass.getDeclaredField("ID_FAMILY");
      f.setAccessible(true);
      ID_FAMILY = (byte[]) f.get(null);
      f = uidclass.getDeclaredField("NAME_FAMILY");
      f.setAccessible(true);
      NAME_FAMILY = (byte[]) f.get(null);
      toBytes = uidclass.getDeclaredMethod("toBytes", String.class);
      toBytes.setAccessible(true);
      fromBytes = uidclass.getDeclaredMethod("fromBytes", byte[].class);
      fromBytes.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("static initializer failed", e);
    }
  }

  /** Prints usage. */
  static void usage(final String errmsg) {
    usage(null, errmsg);
  }

  /** Prints usage. */
  static void usage(final ArgP argp, final String errmsg) {
    System.err.println(errmsg);
    System.err.println("Usage: uid <subcommand> args\n"
        + "Sub commands:\n"
        + "  grep [kind] <RE>: Finds matching IDs.\n"
        + "  assign <kind> <name> [names]:"
        + " Assign an ID for the given name(s).\n"
        + "  [kind] <name>: Lookup the ID of this name.\n"
        + "  [kind] <ID>: Lookup the name of this ID.\n\n"
        + "Example values for [kind]:"
        + " metric, tagk (tag name), tagv (tag value).");
    if (argp != null) {
      System.err.print(argp.usage());
    }
  }

  public static void main(String[] args) {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    CliOptions.addVerbose(argp);
    argp.addOption("--idwidth", "N",
                   "Number of bytes on which the UniqueId is encoded.");
    argp.addOption("--ignore-case",
                   "Ignore case distinctions when matching a regexp.");
    argp.addOption("-i", "Short for --ignore-case.");
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, "Invalid usage");
      System.exit(2);
    } else if (args.length < 1) {
      usage(argp, "Not enough arguments");
      System.exit(2);
    }
    final HTable htable = createHTable(argp);
    final short idwidth = (argp.has("--idwidth")
                           ? Short.parseShort(argp.get("--idwidth"))
                           : 3);
    if (idwidth <= 0) {
      usage(argp, "Negative or 0 --idwidth");
      System.exit(3);
    }
    final boolean ignorecase = argp.has("--ignore-case") || argp.has("-i");
    argp = null;
    System.exit(runCommand(htable, idwidth, ignorecase, args));
  }

  private static int runCommand(final HTable htable,
                                final short idwidth,
                                final boolean ignorecase,
                                final String[] args) {
    final int nargs = args.length;
    if (args[0].equals("grep")) {
      if (2 <= nargs && nargs <= 3) {
        try {
          return grep(htable, ignorecase, args);
        } catch (IOException e) {
          return 3;
        }
      } else {
        usage("Wrong number of arguments");
        return 2;
      }
    } else if (args[0].equals("assign")) {
      if (nargs < 3) {
        usage("Wrong number of arguments");
        return 2;
      }
      return assign(htable, idwidth, args);
    } else {
      if (1 <= nargs && nargs <= 2) {
        final String kind = nargs == 2 ? args[0] : null;
        try {
          final long id = Long.parseLong(args[nargs - 1]);
          return lookupId(htable, idwidth, id, kind);
        } catch (NumberFormatException e) {
          return lookupName(htable, idwidth, args[nargs - 1], kind);
        }
      } else {
        usage("Wrong number of arguments");
        return 2;
      }
    }
  }

  /**
   * Using the command line options, creates the right {@link HTable}.
   * @param argp The parsed arguments from the command line.
   * @return A {@link HTable} for the given {@code --uidtable}.
   */
  private static HTable createHTable(final ArgP argp) {
    final Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.client.scanner.caching", 1024);
    final String uidtable = argp.get("--uidtable", "tsdb-uid");
    try {
      return new HTable(conf, uidtable);
    } catch (IOException e) {
      LOG.error("Failed to create an HTable for " + uidtable, e);
      System.exit(3);
      return null;  // Unreachable, stupid javac.
    }
  }

  /**
   * Implements the {@code grep} subcommand.
   * @param htable The table where the UIDs are.
   * @param ignorecase Whether or not to ignore the case while grepping.
   * @param args Command line arguments ({@code [kind] RE}).
   * @return The exit status of the command (0 means at least 1 match).
   */
  private static int grep(final HTable htable,
                          final boolean ignorecase,
                          final String[] args) throws IOException {
    final Scan scan = new Scan();
    String regexp;
    final byte[] id_fam = ID_FAMILY;
    if (args.length == 3) {
      scan.addColumn(id_fam, toBytes(args[1]));
      regexp = args[2];
    } else {
      scan.addFamily(id_fam);
      regexp = args[1];
    }
    if (ignorecase) {
      regexp = "(?i)" + regexp;
    }
    {
      final RegexStringComparator r = new RegexStringComparator(regexp);
      r.setCharset(CHARSET);
      final RowFilter filter = new RowFilter(RowFilter.CompareOp.EQUAL, r);
      scan.setFilter(filter);
    }
    final ResultScanner scanner = htable.getScanner(scan);
    boolean found = false;
    try {
      Result result;
      while ((result = scanner.next()) != null) {
        found |= printResult(result, id_fam, true);
      }
    } catch (IOException e) {
      LOG.error("Error while scanning HBase, scanner=" + scanner, e);
      throw e;
    } finally {
      scanner.close();
    }
    return found ? 0 : 1;
  }

  /**
   * Helper to print the cells in a given family for a given row, if any.
   * @param result The row to print.
   * @param family Only cells in this family (if any) will be printed.
   * @param formard If true, this row contains a forward mapping (name to ID).
   * Otherwise the row is assumed to contain a reverse mapping (ID to name).
   * @return {@code true} if at least one cell was printed.
   */
  private static boolean printResult(final Result result,
                                     final byte[] family,
                                     final boolean formard) {
    final byte[] row = result.getRow();
    if (row == null) {
      return false;
    }
    String name = formard ? fromBytes(row) : null;
    String id = formard ? null : Arrays.toString(row);
    boolean printed = false;
    for (final Map.Entry<byte[], byte[]> entry
         : result.getFamilyMap(family).entrySet()) {
      printed = true;
      if (formard) {
        id = Arrays.toString(entry.getValue());
      } else {
        name = fromBytes(entry.getValue());
      }
      System.out.println(fromBytes(entry.getKey())
                         + ' ' + name + ": " + id);
    }
    return printed;
  }

  /**
   * Implements the {@code assign} subcommand.
   * @param htable The table where the UIDs are.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param args Command line arguments ({@code assign name [names]}).
   * @return The exit status of the command (0 means success).
   */
  private static int assign(final HTable htable,
                            final short idwidth,
                            final String[] args) {
    final UniqueId uid = new UniqueId(htable, args[1], (int) idwidth);
    for (int i = 2; i < args.length; i++) {
      try {
        uid.getOrCreateId(args[i]);
        // Lookup again the ID we've just created and print it.
        extactLookupName(htable, idwidth, args[1], args[i]);
      } catch (HBaseException e) {
        LOG.error("error while processing " + args[i], e);
        return 3;
      }
    }
    return 0;
  }

  /**
   * Looks up an ID and finds the corresponding name(s), if any.
   * @param htable The table where the UIDs are.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param lid The ID to look for.
   * @param kind The 'kind' of the ID (can be {@code null}).
   * @return The exit status of the command (0 means at least 1 found).
   */
  private static int lookupId(final HTable htable,
                              final short idwidth,
                              final long lid,
                              final String kind) {
    final byte[] id = idInBytes(idwidth, lid);
    if (id == null) {
      return 1;
    } else if (kind != null) {
      return extactLookupId(htable, idwidth, kind, id);
    }
    return findAndPrintRow(htable, id, NAME_FAMILY, false);
  }

  /**
   * Gets a given row in HBase and prints it on standard output.
   * @param htable The table to use.
   * @param row The row to attempt to get from HBase.
   * @param family The family in which we're interested.
   * @return 0 if at least one cell was found and printed, 1 otherwise.
   */
  private static int findAndPrintRow(final HTable htable,
                                     final byte[] row,
                                     final byte[] family,
                                     boolean formard) {
    final Get get = new Get(row);
    get.addFamily(family);
    Result result;
    try {
      result = htable.get(get);
    } catch (IOException e) {
      LOG.error("Get failed: " + get, e);
      return 1;
    }
    return printResult(result, family, formard) ? 0 : 1;
  }

  /**
   * Looks up an ID for a given kind, and prints it if found.
   * @param htable The table where the UIDs are.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param kind The 'kind' of the ID (must not be {@code null}).
   * @param id The ID to look for.
   * @return 0 if the ID for this kind was found, 1 otherwise.
   */
  private static int extactLookupId(final HTable htable,
                                    final short idwidth,
                                    final String kind,
                                    final byte[] id) {
    final UniqueId uid = new UniqueId(htable, kind, (int) idwidth);
    try {
      final String name = uid.getName(id);
      System.out.println(kind + ' ' + name + ": " + Arrays.toString(id));
      return 0;
    } catch (NoSuchUniqueId e) {
      LOG.error(e.getMessage());
      return 1;
    }
  }

  /**
   * Transforms an ID into the corresponding byte array.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param lid The ID to transform.
   * @return The ID represented in {@code idwidth} bytes, or
   * {@code null} if {@code lid} couldn't fit in {@code idwidth} bytes.
   */
  private static byte[] idInBytes(final short idwidth, final long lid) {
    if (idwidth <= 0) {
      throw new AssertionError("negative idwidth: " + idwidth);
    }
    final byte[] id = Bytes.toBytes(lid);
    for (int i = 0; i < id.length - idwidth; i++) {
      if (id[i] != 0) {
        System.err.println(lid + " is too large to fit on " + idwidth
            + " bytes.  Maybe you forgot to adjust --idwidth?");
        return null;
      }
    }
    return Arrays.copyOfRange(id, id.length - idwidth, id.length);
  }

  /**
   * Looks up a name and finds the corresponding UID(s), if any.
   * @param htable The table where the UIDs are.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param name The name to look for.
   * @param kind The 'kind' of the ID (can be {@code null}).
   * @return The exit status of the command (0 means at least 1 found).
   */
  private static int lookupName(final HTable htable,
                                final short idwidth,
                                final String name,
                                final String kind) {
    if (kind != null) {
      return extactLookupName(htable, idwidth, kind, name);
    }
    return findAndPrintRow(htable, toBytes(name), ID_FAMILY, true);
  }

  /**
   * Looks up a name for a given kind, and prints it if found.
   * @param htable The table where the UIDs are.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param kind The 'kind' of the ID (must not be {@code null}).
   * @param name The name to look for.
   * @return 0 if the name for this kind was found, 1 otherwise.
   */
  private static int extactLookupName(final HTable htable,
                                      final short idwidth,
                                      final String kind,
                                      final String name) {
    final UniqueId uid = new UniqueId(htable, kind, (int) idwidth);
    try {
      final byte[] id = uid.getId(name);
      System.out.println(kind + ' ' + name + ": " + Arrays.toString(id));
      return 0;
    } catch (NoSuchUniqueName e) {
      LOG.error(e.getMessage());
      return 1;
    }
  }

  private static byte[] toBytes(final String s) {
    try {
      return (byte[]) toBytes.invoke(null, s);
    } catch (Exception e) {
      throw new RuntimeException("toBytes=" + toBytes, e);
    }
  }

  private static String fromBytes(final byte[] b) {
    try {
      return (String) fromBytes.invoke(null, b);
    } catch (Exception e) {
      throw new RuntimeException("fromBytes=" + fromBytes, e);
    }
  }

}
