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
package net.opentsdb.tsd;

import java.util.HashMap;

/**
 * Provides for a JSON-RPC style error response for use in the API calls
 */
final class JsonRpcError {
  /** User provided error string */
  private String message;
  
  /** User provided error code */
  private int code;

  /**
   * Constructor that requires the user to supply a message and a numeric error
   * code
   * @param msg A descriptive error message
   * @param c A positive or negative integer error code
   */
  public JsonRpcError(final String msg, final int c) {
    message = msg;
    code = c;
  }

  /**
   * Attempts to build a JSON-RPC style error message from the local object
   * @return A JSON formatted string conforming to the JSON-RPC error message
   *         style with "code" and "message" fields. If there was an error
   *         serializing it will return the default "{"error":"unable to build
   *         error message"}" string
   */
  public final String getJSON() {
    HashMap<String, Object> main_node = new HashMap<String, Object>(1);
    HashMap<String, String> nodes = new HashMap<String, String>(2);
    nodes.put("code", Integer.toString(code));
    nodes.put("message", message);
    main_node.put("error", nodes);

    JsonHelper json = new JsonHelper(main_node);
    String response = json.getJsonString();
    if (json.getError().isEmpty())
      return response;
    else
      return "{\"error\":\"Unable to build error message\"}";
  }

  /**
   * Attempts to build a JSON-RPC style error message from the local object but
   * wraps the string in a function name provided by the user
   * @param function The name of a javascript function. If this is an empty
   *          string, the default "parseTSDResponse" will be used
   * @return A JSON formatted string conforming to the JSON-RPC error message
   *         style with "code" and "message" fields. If there was an error
   *         serializing it will return the default "{"error":"unable to build
   *         error message"}" string
   */
  public final String getJSONP(final String function) {
    return (function.length() > 0 ? function : "parseTSDResponse") + 
      "(" + getJSON() + ");";
  }
}
