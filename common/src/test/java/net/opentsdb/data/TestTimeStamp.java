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
package net.opentsdb.data;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import net.opentsdb.data.TimeStamp.TimeStampComparator;

public class TestTimeStamp {
  
  @Test
  public void comparator() throws Exception {
    TimeStampComparator comparator = TimeStamp.COMPARATOR;
    
    TimeStamp ts1 = new MillisecondTimeStamp(1000L);
    TimeStamp ts2 = new MillisecondTimeStamp(2000L);
    TimeStamp ts3 = new MillisecondTimeStamp(1000L);
    
    assertEquals(0, comparator.compare(null, null));
    assertEquals(-1, comparator.compare(ts1, null));
    assertEquals(1, comparator.compare(null, ts2));
    assertEquals(0, comparator.compare(ts1, ts1));
    assertEquals(0, comparator.compare(ts1, ts3));
    assertEquals(-1, comparator.compare(ts1, ts2));
    assertEquals(1, comparator.compare(ts2, ts1));
  }
  
}
