// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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

import org.junit.Before;
import org.powermock.api.mockito.PowerMockito;

public class TestSaltScannerHistogramSalted extends TestSaltScannerHistogram {
  
  @Before
  public void beforeLocal() throws Exception {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
    
    key_a = getRowKey(METRIC_STRING, 1356998400, TAGK_STRING, TAGV_STRING, 
        TAGK_B_STRING, TAGV_STRING);
    key_b = getRowKey(METRIC_STRING, 1356998400, TAGK_STRING, TAGV_B_STRING, 
        TAGK_B_STRING, TAGV_STRING);
    key_c = getRowKey(METRIC_STRING, 1359680400, TAGK_STRING, TAGV_STRING, 
        TAGK_B_STRING, TAGV_STRING);
  }

}
