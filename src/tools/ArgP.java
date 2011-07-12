// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * A dead simple command-line argument parser.
 * Because I couldn't find any one in Java that wasn't horribly bloated.
 * <p>
 * Example:
 * <pre>{@literal
 *   public static void main(String[] args) {
 *     final ArgP argp = new ArgP();
 *     argp.addOption("--verbose", "Whether or not to be verbose.");
 *     argp.addOption("--path", "PATH", "The input path to read.");
 *     try {
 *       args = argp.parse(args);
 *     } catch (IllegalArgumentException e) {
 *       System.err.println(e.getMessage());
 *       System.err.print(argp.usage());  // Note: usage already ends with \n.
 *       System.exit(1);
 *     }
 *     final boolean verbose = argp.has("--verbose");
 *     final String path = argp.get("--path");  // Check that it's non-null.
 *     ...
 *   }
 * }</pre>
 * This parser honors the convention that argument {@code --} means
 * "stop parsing options".
 * <p>
 * This class is not thread-safe.
 */
public final class ArgP {

  /**
   * Maps an option name (e.g, {@code "--foo"}) to a 2-element array
   * {@code ["META", "Help string"]}
   */
  private final HashMap<String, String[]> options
    = new HashMap<String, String[]>();

  /**
   * Maps an option name to the value parsed for this option.
   * The value can be {@code null}.
   */
  private HashMap<String, String> parsed;

  /** Constructor.  */
  public ArgP() {
  }

  /**
   * Registers an option in this argument parser.
   * @param name The name of the option to recognize (e.g. {@code --foo}).
   * @param meta The meta-variable to associate with the value of the option.
   * @param help A short description of this option.
   * @throws IllegalArgumentException if the given name was already used.
   * @throws IllegalArgumentException if the name doesn't start with a dash.
   * @throws IllegalArgumentException if any of the given strings is empty.
   */
  public void addOption(final String name,
                        final String meta,
                        final String help) {
    if (name.isEmpty()) {
      throw new IllegalArgumentException("empty name");
    } else if (name.charAt(0) != '-') {
      throw new IllegalArgumentException("name must start with a `-': " + name);
    } else if (meta != null && meta.isEmpty()) {
      throw new IllegalArgumentException("empty meta");
    } else if (help.isEmpty()) {
      throw new IllegalArgumentException("empty help");
    }
    final String[] prev = options.put(name, new String[] { meta, help });
    if (prev != null) {
      options.put(name, prev);  // Undo the `put' above.
      throw new IllegalArgumentException("Option " + name + " already defined"
                                         + " in " + this);
    }
  }

  /**
   * Registers an option that doesn't take a value in this argument parser.
   * @param name The name of the option to recognize (e.g. {@code --foo}).
   * @param help A short description of this option.
   * @throws IllegalArgumentException if the given name was already used.
   * @throws IllegalArgumentException if the name doesn't start with a dash.
   * @throws IllegalArgumentException if any of the given strings is empty.
   */
  public void addOption(final String name, final String help) {
    addOption(name, null, help);
  }

  /**
   * Returns whether or not the given option name exists.
   * <p>Calling
   * {@link #addOption(String, String, String) addOption}{@code (foo, ...)}
   * entails that {@code optionExists(foo)} returns {@code true}.
   * @param name The name of the option to recognize (e.g. {@code --foo}).
   */
  public boolean optionExists(final String name) {
    return options.containsKey(name);
  }

  /**
   * Parses the command line given in argument.
   * @return The remaining words that weren't options (i.e. that didn't start
   * with a dash).
   * @throws IllegalArgumentException if the given command line wasn't valid.
   */
  public String[] parse(final String[] args) {
    parsed = new HashMap<String, String>(options.size());
    ArrayList<String> unparsed = null;
    for (int i = 0; i < args.length; i++) {
      final String arg = args[i];
      String[] opt = options.get(arg);
      if (opt != null) {  // Perfect match: got --foo
        if (opt[0] != null) {  // This option requires an argument.
          if (++i < args.length) {
            parsed.put(arg, args[i]);
          } else {
            throw new IllegalArgumentException("Missing argument for " + arg);
          }
        } else {
          parsed.put(arg, null);
        }
        continue;
      }
      // Is it a --foo=blah?
      final int equal = arg.indexOf('=', 1);
      if (equal > 0) {  // Looks like so.
        final String name = arg.substring(0, equal);
        opt = options.get(name);
        if (opt != null) {
          parsed.put(name, arg.substring(equal + 1, arg.length()));
          continue;
        }
      }
      // Not a flag.
      if (unparsed == null) {
        unparsed = new ArrayList<String>(args.length - i);
      }
      if (!arg.isEmpty() && arg.charAt(0) == '-') {
        if (arg.length() == 2 && arg.charAt(1) == '-') {  // `--'
          for (i++; i < args.length; i++) {
            unparsed.add(args[i]);
          }
          break;
        }
        throw new IllegalArgumentException("Unrecognized option " + arg);
      }
      unparsed.add(arg);
    }
    if (unparsed != null) {
      return unparsed.toArray(new String[unparsed.size()]);
    } else {
      return new String[0];
    }
  }

  /**
   * Returns the value of the given option, if it was given.
   * Returns {@code null} if the option wasn't given, or if the option doesn't
   * take a value (in which case you should use {@link #has} instead).
   * @param name The name of the option to recognize (e.g. {@code --foo}).
   * @throws IllegalArgumentException if this option wasn't registered with
   * {@link #addOption}.
   * @throws IllegalStateException if {@link #parse} wasn't called.
   */
  public String get(final String name) {
    if (!options.containsKey(name)) {
      throw new IllegalArgumentException("Unknown option " + name);
    } else if (parsed == null) {
      throw new IllegalStateException("parse() wasn't called");
    }
    return parsed.get(name);
  }

  /**
   * Returns the value of the given option, or a default value.
   * @param name The name of the option to recognize (e.g. {@code --foo}).
   * @param defaultv The default value to return if the option wasn't given.
   * @throws IllegalArgumentException if this option wasn't registered with
   * {@link #addOption}.
   * @throws IllegalStateException if {@link #parse} wasn't called.
   */
  public String get(final String name, final String defaultv) {
    final String value = get(name);
    return value == null ? defaultv : value;
  }

  /**
   * Returns whether or not the given option was given.
   * @param name The name of the option to recognize (e.g. {@code --foo}).
   * @throws IllegalArgumentException if this option wasn't registered with
   * {@link #addOption}.
   * @throws IllegalStateException if {@link #parse} wasn't called.
   */
  public boolean has(final String name) {
    if (!options.containsKey(name)) {
      throw new IllegalArgumentException("Unknown option " + name);
    } else if (parsed == null) {
      throw new IllegalStateException("parse() wasn't called");
    }
    return parsed.containsKey(name);
  }

  /**
   * Appends the usage to the given buffer.
   * @param buf The buffer to write to.
   */
  public void addUsageTo(final StringBuilder buf) {
    final ArrayList<String> names = new ArrayList<String>(options.keySet());
    Collections.sort(names);
    int max_length = 0;
    for (final String name : names) {
      final String[] opt = options.get(name);
      final int length = name.length()
        + (opt[0] == null ? 0 : opt[0].length() + 1);
      if (length > max_length) {
        max_length = length;
      }
    }
    for (final String name : names) {
      final String[] opt = options.get(name);
      int length = name.length();
      buf.append("  ").append(name);
      if (opt[0] != null) {
        length += opt[0].length() + 1;
        buf.append('=').append(opt[0]);
      }
      for (int i = length; i <= max_length; i++) {
        buf.append(' ');
      }
      buf.append(opt[1]).append('\n');
    }
  }

  /**
   * Returns a usage string.
   */
  public String usage() {
    final StringBuilder buf = new StringBuilder(16 * options.size());
    addUsageTo(buf);
    return buf.toString();
  }

  public String toString() {
    final StringBuilder buf = new StringBuilder(16 * options.size());
    buf.append("ArgP(");
    for (final String name : options.keySet()) {
      final String[] opt = options.get(name);
      buf.append(name)
        .append("=(").append(opt[0]).append(", ").append(opt[1]).append(')')
        .append(", ");
    }
    buf.setLength(buf.length() - 2);
    buf.append(')');
    return buf.toString();
  }

}
