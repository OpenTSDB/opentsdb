// This file is part of OpenTSDB.
// Copyright (C) 2011-2015  The OpenTSDB Authors.
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
package net.opentsdb.graph;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.utils.FileSystem;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
@RunWith(PowerMockRunner.class)
@PrepareForTest({PrintWriter.class, File.class, FileSystem.class, Plot.class})
public final class TestPlot {

  Plot plot;
  File mockFile;
  @Before
  public void setUp() throws Exception {
    plot = new Plot(0, 1234567890, null);
    Map<String, String> params = new HashMap<String, String>();
    plot.setParams(params);
    
    PrintWriter mockWriter = PowerMockito.mock(PrintWriter.class);
    PowerMockito.whenNew(PrintWriter.class)
      .withAnyArguments()
      .thenReturn(mockWriter);
    
    // Mock the builder pattern for PrintWriter instances.
    PowerMockito.when(mockWriter, "append", Mockito.anyString()).thenReturn(mockWriter);
    PowerMockito.when(mockWriter, "append", Mockito.anyChar()).thenReturn(mockWriter);
  
    mockFile = PowerMockito.mock(File.class);
    PowerMockito.whenNew(File.class).withAnyArguments().thenReturn(mockFile);
    PowerMockito.when(mockFile, "getParent").thenReturn("/temp/opentsdb");
    PowerMockito.when(mockFile, "exists").thenReturn(true);
    PowerMockito.when(mockFile, "isDirectory").thenReturn(true);
    PowerMockito.when(mockFile, "canWrite").thenReturn(true);
  }

  @Test
  public void dumpToFilesDirectoryExistsIsWritable() throws Exception {
    plot.dumpToFiles("/temp/opentsdb/s0M3haSh");
  }

  @Test(expected = IllegalArgumentException.class)
  public void dumpToFilesDirectoryExistsNotWritable() throws Exception {
    PowerMockito.when(mockFile, "canWrite").thenReturn(false);
    plot.dumpToFiles("/temp/opentsdb/s0M3haSh");
  }

  @Test
  public void dumpToFilesCreateNonexistentDirectory() throws Exception {
    PowerMockito.when(mockFile, "exists").thenReturn(false);
    PowerMockito.when(mockFile, "mkdirs").thenReturn(true);
    plot.dumpToFiles("/temp/opentsdb/s0M3haSh");
    verify(mockFile, times(1)).mkdirs();
  }

  @Test(expected = IllegalArgumentException.class)
  public void dumpToFilesCreateNonexistentDirectoryFail() throws Exception {
    PowerMockito.when(mockFile, "exists").thenReturn(false);
    PowerMockito.when(mockFile, "mkdirs").thenReturn(false);
    plot.dumpToFiles("/temp/opentsdb/s0M3haSh");
  }

  @Test(expected = IllegalArgumentException.class)
  public void dumpToFilesNotADirectory() throws Exception {
    PowerMockito.when(mockFile, "exists").thenReturn(true);
    PowerMockito.when(mockFile, "isDirectory").thenReturn(false);
    plot.dumpToFiles("/temp/opentsdb/s0M3haSh");
  }
}
