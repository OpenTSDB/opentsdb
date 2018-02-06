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

import net.opentsdb.utils.FileSystem;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({File.class, FileSystem.class})
public final class TestFileSystem {

  File mockFile;
  
  @Before
  public void setUp() throws Exception {  
    mockFile = PowerMockito.mock(File.class);
    PowerMockito.whenNew(File.class).withAnyArguments().thenReturn(mockFile);
    PowerMockito.when(mockFile, "getParent").thenReturn("/temp/opentsdb");
    PowerMockito.when(mockFile, "exists").thenReturn(true);
    PowerMockito.when(mockFile, "isDirectory").thenReturn(true);
    PowerMockito.when(mockFile, "canWrite").thenReturn(true);
  }

  @Test (expected = IllegalArgumentException.class)
  public void checkDirectoryEmptyString() throws Exception {
    FileSystem.checkDirectory("", true, false);
  }
}
