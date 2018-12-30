// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.TreeMap;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class, Scanner.class, SaltScanner.class, Span.class, 
  Const.class, UniqueId.class })
public class TestSaltScannerSalted extends TestSaltScanner {
  
  @Before
  public void beforeLocal() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
    
    KEY_A = getRowKey(METRIC_STRING, 1356998400, TAGK_STRING, TAGV_STRING);
    KEY_B = getRowKey(METRIC_STRING, 1356998400, TAGK_STRING, TAGV_B_STRING);
    KEY_C = getRowKey(METRIC_STRING, 1359680400, TAGK_STRING, TAGV_STRING);
    
    filters = new ArrayList<TagVFilter>();
    
    spans = new TreeMap<byte[], Span>(new RowKey.SaltCmp());
    setupMockScanners(true);
  }

}
