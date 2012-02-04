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
package net.opentsdb.tsd;

import java.io.IOException;

import net.opentsdb.core.TSDB;

/** Implements the "/s" endpoint to serve static files. */
final class StaticFileRpc implements HttpRpc {

  /**
   * The path to the directory where to find static files
   * (for the {@code /s} URLs).
   */
  private final String staticroot;

  /**
   * Constructor.
   */
  public StaticFileRpc() {
    staticroot = RpcHandler.getDirectoryFromSystemProp("tsd.http.staticroot");
  }

  public void execute(final TSDB tsdb, final HttpQuery query)
    throws IOException {
    final String uri = query.request().getUri();
    if ("/favicon.ico".equals(uri)) {
      query.sendFile(staticroot + "/favicon.ico", 31536000 /*=1yr*/);
      return;
    }
    if (uri.length() < 3) {  // Must be at least 3 because of the "/s/".
      throw new BadRequestException("URI too short <code>" + uri + "</code>");
    }
    // Cheap security check to avoid directory traversal attacks.
    // TODO(tsuna): This is certainly not sufficient.
    if (uri.indexOf("..", 3) > 0) {
      throw new BadRequestException("Malformed URI <code>" + uri + "</code>");
    }
    final int questionmark = uri.indexOf('?', 3);
    final int pathend = questionmark > 0 ? questionmark : uri.length();
    query.sendFile(staticroot + uri.substring(3, pathend),
                   uri.contains("nocache") ? 0 : 31536000 /*=1yr*/);
  }
}
