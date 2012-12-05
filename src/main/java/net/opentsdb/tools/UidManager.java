// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

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
  /** Row key of the special row used to track the max ID already assigned. */
  private static final byte[] MAXID_ROW;
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
      f = uidclass.getDeclaredField("MAXID_ROW");
      f.setAccessible(true);
      MAXID_ROW = (byte[]) f.get(null);
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
        + "  rename <kind> <name> <newname>: Renames this UID.\n"
        + "  fsck: Checks the consistency of UIDs.\n"
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
    final byte[] table = argp.get("--uidtable", "tsdb-uid").getBytes();
    final short idwidth = (argp.has("--idwidth")
                           ? Short.parseShort(argp.get("--idwidth"))
                           : 3);
    if (idwidth <= 0) {
      usage(argp, "Negative or 0 --idwidth");
      System.exit(3);
    }
    final boolean ignorecase = argp.has("--ignore-case") || argp.has("-i");
    final HBaseClient client = CliOptions.clientFromOptions(argp);
    argp = null;
    int rc;
    try {
      rc = runCommand(client, table, idwidth, ignorecase, args);
    } finally {
      try {
        client.shutdown().joinUninterruptibly();
      } catch (Exception e) {
        LOG.error("Unexpected exception while shutting down", e);
        rc = 42;
      }
    }
    System.exit(rc);
  }

  private static int runCommand(final HBaseClient client,
                                final byte[] table,
                                final short idwidth,
                                final boolean ignorecase,
                                final String[] args) {
    final int nargs = args.length;
    if (args[0].equals("grep")) {
      if (2 <= nargs && nargs <= 3) {
        try {
          return grep(client, table, ignorecase, args);
        } catch (HBaseException e) {
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
      return assign(client, table, idwidth, args);
    } else if (args[0].equals("rename")) {
      if (nargs != 4) {
        usage("Wrong number of arguments");
        return 2;
      }
      return rename(client, table, idwidth, args);
    } else if (args[0].equals("fsck")) {
      return fsck(client, table);
    } else {
      if (1 <= nargs && nargs <= 2) {
        final String kind = nargs == 2 ? args[0] : null;
        try {
          final long id = Long.parseLong(args[nargs - 1]);
          return lookupId(client, table, idwidth, id, kind);
        } catch (NumberFormatException e) {
          return lookupName(client, table, idwidth, args[nargs - 1], kind);
        }
      } else {
        usage("Wrong number of arguments");
        return 2;
      }
    }
  }

  /**
   * Implements the {@code grep} subcommand.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param ignorecase Whether or not to ignore the case while grepping.
   * @param args Command line arguments ({@code [kind] RE}).
   * @return The exit status of the command (0 means at least 1 match).
   */
  private static int grep(final HBaseClient client,
                          final byte[] table,
                          final boolean ignorecase,
                          final String[] args) {
    final Scanner scanner = client.newScanner(table);
    scanner.setMaxNumRows(1024);
    String regexp;
    scanner.setFamily(ID_FAMILY);
    if (args.length == 3) {
      scanner.setQualifier(toBytes(args[1]));
      regexp = args[2];
    } else {
      regexp = args[1];
    }
    if (ignorecase) {
      regexp = "(?i)" + regexp;
    }
    scanner.setKeyRegexp(regexp, CHARSET);
    boolean found = false;
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          found |= printResult(row, ID_FAMILY, true);
        }
      }
    } catch (HBaseException e) {
      LOG.error("Error while scanning HBase, scanner=" + scanner, e);
      throw e;
    } catch (Exception e) {
      LOG.error("WTF?  Unexpected exception type, scanner=" + scanner, e);
      throw new AssertionError("Should never happen");
    }
    return found ? 0 : 1;
  }

  /**
   * Helper to print the cells in a given family for a given row, if any.
   * @param row The row to print.
   * @param family Only cells in this family (if any) will be printed.
   * @param formard If true, this row contains a forward mapping (name to ID).
   * Otherwise the row is assumed to contain a reverse mapping (ID to name).
   * @return {@code true} if at least one cell was printed.
   */
  private static boolean printResult(final ArrayList<KeyValue> row,
                                     final byte[] family,
                                     final boolean formard) {
    final byte[] key = row.get(0).key();
    String name = formard ? fromBytes(key) : null;
    String id = formard ? null : Arrays.toString(key);
    boolean printed = false;
    for (final KeyValue kv : row) {
      if (!Bytes.equals(kv.family(), family)) {
        continue;
      }
      printed = true;
      if (formard) {
        id = Arrays.toString(kv.value());
      } else {
        name = fromBytes(kv.value());
      }
      System.out.println(fromBytes(kv.qualifier()) + ' ' + name + ": " + id);
    }
    return printed;
  }

  /**
   * Implements the {@code assign} subcommand.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param args Command line arguments ({@code assign name [names]}).
   * @return The exit status of the command (0 means success).
   */
  private static int assign(final HBaseClient client,
                            final byte[] table,
                            final short idwidth,
                            final String[] args) {
    final UniqueId uid = new UniqueId(client, table, args[1], (int) idwidth);
    for (int i = 2; i < args.length; i++) {
      try {
        uid.getOrCreateId(args[i]);
        // Lookup again the ID we've just created and print it.
        extactLookupName(client, table, idwidth, args[1], args[i]);
      } catch (HBaseException e) {
        LOG.error("error while processing " + args[i], e);
        return 3;
      }
    }
    return 0;
  }

  /**
   * Implements the {@code rename} subcommand.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param args Command line arguments ({@code assign name [names]}).
   * @return The exit status of the command (0 means success).
   */
  private static int rename(final HBaseClient client,
                            final byte[] table,
                            final short idwidth,
                            final String[] args) {
    final String kind = args[1];
    final String oldname = args[2];
    final String newname = args[3];
    final UniqueId uid = new UniqueId(client, table, kind, (int) idwidth);
    try {
      uid.rename(oldname, newname);
    } catch (HBaseException e) {
      LOG.error("error while processing renaming " + oldname
                + " to " + newname, e);
      return 3;
    } catch (NoSuchUniqueName e) {
      LOG.error(e.getMessage());
      return 1;
    }
    System.out.println(kind + ' ' + oldname + " -> " + newname);
    return 0;
  }

  /**
   * Implements the {@code fsck} subcommand.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @return The exit status of the command (0 means success).
   */
  private static int fsck(final HBaseClient client, final byte[] table) {

    final class Uids {
      int errors;
      long maxid;
      short width;
      final HashMap<String, String> id2name = new HashMap<String, String>();
      final HashMap<String, String> name2id = new HashMap<String, String>();

      void error(final KeyValue kv, final String msg) {
        error(msg + ".  kv=" + kv);
      }

      void error(final String msg) {
        LOG.error(msg);
        errors++;
      }
    }

    final long start_time = System.nanoTime();
    final HashMap<String, Uids> name2uids = new HashMap<String, Uids>();
    final Scanner scanner = client.newScanner(table);
    scanner.setMaxNumRows(1024);
    int kvcount = 0;
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          for (final KeyValue kv : row) {
            kvcount++;
            final String kind = fromBytes(kv.qualifier());
            Uids uids = name2uids.get(kind);
            if (uids == null) {
              uids = new Uids();
              name2uids.put(kind, uids);
            }
            final byte[] key = kv.key();
            final byte[] family = kv.family();
            final byte[] value = kv.value();
            if (Bytes.equals(key, MAXID_ROW)) {
              if (value.length != 8) {
                uids.error(kv, "Invalid maximum ID for " + kind
                           + ": should be on 8 bytes: ");
              } else {
                uids.maxid = Bytes.getLong(value);
                LOG.info("Maximum ID for " + kind + ": " + uids.maxid);
              }
            } else {
              short idwidth = 0;
              if (Bytes.equals(family, ID_FAMILY)) {
                idwidth = (short) value.length;
                final String skey = fromBytes(key);
                final String svalue = Arrays.toString(value);
                final String id = uids.name2id.put(skey, svalue);
                if (id != null) {
                  uids.error(kv, "Duplicate forward " + kind + " mapping: "
                             + skey + " -> " + id
                             + " and " + skey + " -> " + svalue);
                }
              } else if (Bytes.equals(family, NAME_FAMILY)) {
                final String skey = Arrays.toString(key);
                final String svalue = fromBytes(value);
                idwidth = (short) key.length;
                final String name = uids.id2name.put(skey, svalue);
                if (name != null) {
                  uids.error(kv, "Duplicate reverse " + kind + "  mapping: "
                             + svalue + " -> " + name
                             + " and " + svalue + " -> " + skey);
                }
              }
              if (uids.width == 0) {
                uids.width = idwidth;
              } else if (uids.width != idwidth) {
                uids.error(kv, "Invalid " + kind + " ID of length " + idwidth
                           + " (expected: " + uids.width + ')');
              }
            }
          }
        }
      }
    } catch (HBaseException e) {
      LOG.error("Error while scanning HBase, scanner=" + scanner, e);
      throw e;
    } catch (Exception e) {
      LOG.error("WTF?  Unexpected exception type, scanner=" + scanner, e);
      throw new AssertionError("Should never happen");
    }

    // Match up all forward mappings with their reverse mappings and vice
    // versa and make sure they agree.
    int errors = 0;
    for (final Map.Entry<String, Uids> entry : name2uids.entrySet()) {
      final String kind = entry.getKey();
      final Uids uids = entry.getValue();

      // Look for forward mappings without the corresponding reverse mappings.
      // These are harmful and shouldn't exist.
      for (final Map.Entry<String, String> nameid : uids.name2id.entrySet()) {
        final String name = nameid.getKey();
        final String id = nameid.getValue();
        final String found = uids.id2name.get(id);
        if (found == null) {
          uids.error("Forward " + kind + " mapping is missing reverse"
                     + " mapping: " + name + " -> " + id);
        } else if (!found.equals(name)) {
          uids.error("Forward " + kind + " mapping " + name + " -> " + id
                     + " is different than reverse mapping: "
                     + id + " -> " + found);
          final String id2 = uids.name2id.get(found);
          if (id2 != null) {
            uids.error("Inconsistent forward " + kind + " mapping "
                       + name + " -> " + id
                       + " vs " + name + " -> " + found
                       + " / " + found + " -> " + id2);
          } else {
            uids.error("Duplicate forward " + kind + " mapping "
                       + name + " -> " + id
                       + " and " + id2 + " -> " + found);
          }
        }
      }

      // Look for reverse mappings without the corresponding forward mappings.
      // These are harmless but shouldn't frequently occur.
      for (final Map.Entry<String, String> idname : uids.id2name.entrySet()) {
        final String name = idname.getValue();
        final String id = idname.getKey();
        final String found = uids.name2id.get(name);
        if (found == null) {
          LOG.warn("Reverse " + kind + " mapping is missing forward"
                   + " mapping: " + name + " -> " + id);
        } else if (!found.equals(id)) {
          final String name2 = uids.id2name.get(found);
          if (name2 != null) {
            uids.error("Inconsistent reverse " + kind + " mapping "
                       + id + " -> " + name
                       + " vs " + found + " -> " + name
                       + " / " + name2 + " -> " + found);
          } else {
            uids.error("Duplicate reverse " + kind + " mapping "
                       + id + " -> " + name
                       + " and " + found + " -> " + name2);
          }
        }
      }

      final int maxsize = Math.max(uids.id2name.size(), uids.name2id.size());
      if (uids.maxid > maxsize) {
        LOG.warn("Max ID for " + kind + " is " + uids.maxid + " but only "
                 + maxsize + " entries were found.  Maybe "
                 + (uids.maxid - maxsize) + " IDs were deleted?");
      } else if (uids.maxid < maxsize) {
        uids.error("We found " + maxsize + ' ' + kind + " but the max ID is"
                   + " only " + uids.maxid + "!  Future IDs may be"
                   + " double-assigned!");
      }

      if (uids.errors > 0) {
        LOG.error(kind + ": Found " + uids.errors + " errors.");
        errors += uids.errors;
      }
    }
    final long timing = (System.nanoTime() - start_time) / 1000000;
    System.out.println(kvcount + " KVs analyzed in " + timing + "ms (~"
                       + (kvcount * 1000 / timing) + " KV/s)");
    if (errors == 0) {
      System.out.println("No errors found.");
      return 0;
    }
    System.err.println(errors + " errors found.");
    return 1;
  }

  /**
   * Looks up an ID and finds the corresponding name(s), if any.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param lid The ID to look for.
   * @param kind The 'kind' of the ID (can be {@code null}).
   * @return The exit status of the command (0 means at least 1 found).
   */
  private static int lookupId(final HBaseClient client,
                              final byte[] table,
                              final short idwidth,
                              final long lid,
                              final String kind) {
    final byte[] id = idInBytes(idwidth, lid);
    if (id == null) {
      return 1;
    } else if (kind != null) {
      return extactLookupId(client, table, idwidth, kind, id);
    }
    return findAndPrintRow(client, table, id, NAME_FAMILY, false);
  }

  /**
   * Gets a given row in HBase and prints it on standard output.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param key The row key to attempt to get from HBase.
   * @param family The family in which we're interested.
   * @return 0 if at least one cell was found and printed, 1 otherwise.
   */
  private static int findAndPrintRow(final HBaseClient client,
                                     final byte[] table,
                                     final byte[] key,
                                     final byte[] family,
                                     boolean formard) {
    final GetRequest get = new GetRequest(table, key);
    get.family(family);
    ArrayList<KeyValue> row;
    try {
      row = client.get(get).joinUninterruptibly();
    } catch (HBaseException e) {
      LOG.error("Get failed: " + get, e);
      return 1;
    } catch (Exception e) {
      LOG.error("WTF?  Unexpected exception type, get=" + get, e);
      return 42;
    }
    return printResult(row, family, formard) ? 0 : 1;
  }

  /**
   * Looks up an ID for a given kind, and prints it if found.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param kind The 'kind' of the ID (must not be {@code null}).
   * @param id The ID to look for.
   * @return 0 if the ID for this kind was found, 1 otherwise.
   */
  private static int extactLookupId(final HBaseClient client,
                                    final byte[] table,
                                    final short idwidth,
                                    final String kind,
                                    final byte[] id) {
    final UniqueId uid = new UniqueId(client, table, kind, (int) idwidth);
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
    final byte[] id = Bytes.fromLong(lid);
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
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param name The name to look for.
   * @param kind The 'kind' of the ID (can be {@code null}).
   * @return The exit status of the command (0 means at least 1 found).
   */
  private static int lookupName(final HBaseClient client,
                                final byte[] table,
                                final short idwidth,
                                final String name,
                                final String kind) {
    if (kind != null) {
      return extactLookupName(client, table, idwidth, kind, name);
    }
    return findAndPrintRow(client, table, toBytes(name), ID_FAMILY, true);
  }

  /**
   * Looks up a name for a given kind, and prints it if found.
   * @param client The HBase client to use.
   * @param idwidth Number of bytes on which the UIDs should be.
   * @param kind The 'kind' of the ID (must not be {@code null}).
   * @param name The name to look for.
   * @return 0 if the name for this kind was found, 1 otherwise.
   */
  private static int extactLookupName(final HBaseClient client,
                                      final byte[] table,
                                      final short idwidth,
                                      final String kind,
                                      final String name) {
    final UniqueId uid = new UniqueId(client, table, kind, (int) idwidth);
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
