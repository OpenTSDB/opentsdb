// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
package net.opentsdb.query;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.FilterList;
import org.hbase.async.KeyRegexpFilter;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Scanner.class })
public class TestQueryUtil {
  private Scanner scanner;
  
  @Before
  public void before() throws Exception {
    scanner = mock(Scanner.class);
  }
  
  @Test
  public void setDataTableScanFilterNoOp() throws Exception {
    QueryUtil.setDataTableScanFilter(
        scanner,
        Lists.<byte[]>newArrayList(), 
        new ByteMap<byte[][]>(),
        false,
        false,
        0);
    verify(scanner, never()).getCurrentKey();
    verify(scanner, never()).setFilter(any(ScanFilter.class));
    verify(scanner, never()).setStartKey(any(byte[].class));
    verify(scanner, never()).setStopKey(any(byte[].class));
  }
  
  @Test
  public void setDataTableScanFilterGroupBy() throws Exception {
    QueryUtil.setDataTableScanFilter(
        scanner,
        Lists.<byte[]>newArrayList(new byte[] { 0, 0, 1 }), 
        new ByteMap<byte[][]>(),
        false,
        false,
        0);
    verify(scanner, never()).getCurrentKey();
    // TODO - validate the regex
    verify(scanner, times(1)).setFilter(any(KeyRegexpFilter.class));
    verify(scanner, never()).setStartKey(any(byte[].class));
    verify(scanner, never()).setStopKey(any(byte[].class));
  }
  
  @Test
  public void setDataTableScanFilterTags() throws Exception {
    final ByteMap<byte[][]> tags = new ByteMap<byte[][]>();
    tags.put(new byte[] { 0, 0, 1 }, new byte[][] { new byte[] {0, 0, 1} });
    QueryUtil.setDataTableScanFilter(
        scanner,
        Lists.<byte[]>newArrayList(), 
        tags,
        false,
        false,
        0);
    verify(scanner, never()).getCurrentKey();
    // TODO - validate the regex
    verify(scanner, times(1)).setFilter(any(KeyRegexpFilter.class));
    verify(scanner, never()).setStartKey(any(byte[].class));
    verify(scanner, never()).setStopKey(any(byte[].class));
  }
  
  @Test
  public void setDataTableScanFilterEnableFuzzy() throws Exception {
    final ByteMap<byte[][]> tags = new ByteMap<byte[][]>();
    tags.put(new byte[] { 0, 0, 1 }, new byte[][] { new byte[] {0, 0, 1} });
    QueryUtil.setDataTableScanFilter(
        scanner,
        Lists.<byte[]>newArrayList(), 
        tags,
        false,
        true,
        0);
    verify(scanner, never()).getCurrentKey();
    // TODO - validate the regex
    verify(scanner, times(1)).setFilter(any(KeyRegexpFilter.class));
    verify(scanner, never()).setStartKey(any(byte[].class));
    verify(scanner, never()).setStopKey(any(byte[].class));
  }
  
  @Test
  public void setDataTableScanFilterEnableExplicit() throws Exception {
    final ByteMap<byte[][]> tags = new ByteMap<byte[][]>();
    tags.put(new byte[] { 0, 0, 1 }, new byte[][] { new byte[] {0, 0, 1} });
    QueryUtil.setDataTableScanFilter(
        scanner,
        Lists.<byte[]>newArrayList(), 
        tags,
        true,
        false,
        0);
    verify(scanner, never()).getCurrentKey();
    // TODO - validate the regex
    verify(scanner, times(1)).setFilter(any(KeyRegexpFilter.class));
    verify(scanner, never()).setStartKey(any(byte[].class));
    verify(scanner, never()).setStopKey(any(byte[].class));
  }
  
  @Test
  public void setDataTableScanFilterEnableBoth() throws Exception {
    when(scanner.getCurrentKey()).thenReturn(new byte[] { 0, 0, 0, 1 });
    final ByteMap<byte[][]> tags = new ByteMap<byte[][]>();
    tags.put(new byte[] { 0, 0, 1 }, new byte[][] { new byte[] {0, 0, 1} });
    QueryUtil.setDataTableScanFilter(
        scanner,
        Lists.<byte[]>newArrayList(), 
        tags,
        true,
        true,
        0);
    verify(scanner, times(2)).getCurrentKey();
    // TODO - validate the regex and fuzzy filter
    verify(scanner, times(1)).setFilter(any(FilterList.class));
    verify(scanner, times(1)).setStartKey(any(byte[].class));
    verify(scanner, times(1)).setStopKey(any(byte[].class));
  }
}
