// This file is part of OpenTSDB.
// Copyright (C) 2013-2017  The OpenTSDB Authors.
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import net.opentsdb.utils.FileSystem;

public final class TestFileSystem {

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test (expected = IllegalArgumentException.class)
  public void checkDirectoryEmptyString() throws Exception {
    FileSystem.checkDirectory("", true /* need_write */, false /* create */);
  }

  @Test (expected = IllegalArgumentException.class)
  public void checkDirectoryNotWritable() throws Exception {
    final File confFile = tempDir.newFolder("opentsdb");
    confFile.setWritable(false);

    FileSystem.checkDirectory("", true /* need_write */, false /* create */);
  }
}
