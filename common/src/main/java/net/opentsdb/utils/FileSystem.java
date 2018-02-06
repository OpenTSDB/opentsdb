// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
