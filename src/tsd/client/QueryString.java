// This file is part of OpenTSDB.
// Copyright (C) 2012  The OpenTSDB Authors.
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
package tsd.client;

// I (tsuna) originally wrote this code for Netty.  Surprisingly, GWT has
// nothing to manually parse query string parameters...

import java.util.ArrayList;
import java.util.HashMap;

import com.google.gwt.http.client.URL;

/**
 * Splits an HTTP query string into a path string and key-value parameter pairs.
 */
public final class QueryString extends HashMap<String, ArrayList<String>> {

  /**
   * Returns the decoded key-value parameter pairs of the URI.
   */
  public static QueryString decode(final String s) {
    final QueryString params = new QueryString();
    String name = null;
    int pos = 0; // Beginning of the unprocessed region
    int i;       // End of the unprocessed region
    for (i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (c == '=' && name == null) {
        if (pos != i) {
          name = URL.decodeComponent(s.substring(pos, i));
        }
        pos = i + 1;
      } else if (c == '&') {
        if (name == null && pos != i) {
          // We haven't seen an `=' so far but moved forward.
          // Must be a param of the form '&a&' so add it with
          // an empty value.
          params.add(URL.decodeComponent(s.substring(pos, i)), "");
        } else if (name != null) {
          params.add(name, URL.decodeComponent(s.substring(pos, i)));
          name = null;
        }
        pos = i + 1;
      }
    }

    if (pos != i) {  // Are there characters we haven't dealt with?
      if (name == null) {     // Yes and we haven't seen any `='.
        params.add(URL.decodeComponent(s.substring(pos, i)), "");
      } else {                // Yes and this must be the last value.
        params.add(name, URL.decodeComponent(s.substring(pos, i)));
      }
    } else if (name != null) {  // Have we seen a name without value?
      params.add(name, "");
    }

    return params;
  }

  /**
   * Adds a query string element.
   * @param name The name of the element.
   * @param value The value of the element.
   */
  public void add(final String name, final String value) {
    ArrayList<String> values = super.get(name);
    if (values == null) {
      values = new ArrayList<String>(1);  // Often there's only 1 value.
      super.put(name, values);
    }
    values.add(value);
  }

  /**
   * Returns the first value for the given key, or {@code null}.
   */
  public String getFirst(final String key) {
    final ArrayList<String> values = super.get(key);
    return values == null ? null : values.get(0);
  }

}
