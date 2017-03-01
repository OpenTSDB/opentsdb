// This file is part of OpenTSDB.
// Copyright (C) 2010-2015  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import java.io.File;

public class FileSystem {
  /**
   * Verifies a directory and checks to see if it's writeable or not if
   * configured
   * @param dir The path to check on
   * @param need_write Set to true if the path needs write access
   * @param create Set to true if the directory should be created if it does not
   *          exist
   * @throws IllegalArgumentException if the path is empty, if it's not there
   *           and told not to create it or if it needs write access and can't
   *           be written to
   */
  public static void checkDirectory(final String dir,
      final boolean need_write, final boolean create) {
    if (dir.isEmpty())
      throw new IllegalArgumentException("Directory path is empty");
    final File f = new File(dir);
    if (!f.exists() && !(create && f.mkdirs())) {
      throw new IllegalArgumentException("No such directory [" + dir + "]");
    } else if (!f.isDirectory()) {
      throw new IllegalArgumentException("Not a directory [" + dir + "]");
    } else if (need_write && !f.canWrite()) {
      throw new IllegalArgumentException("Cannot write to directory [" + dir
          + "]");
    }
  }
}