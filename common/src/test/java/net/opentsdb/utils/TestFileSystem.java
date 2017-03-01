// This file is part of OpenTSDB.
// Copyright (C) 2013-2014  The OpenTSDB Authors.
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