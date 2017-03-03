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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import net.opentsdb.data.TimeStamp.TimeStampComparator;

public class TestMillisecondTimeStamp {

  @Test
  public void ctors() throws Exception {
    TimeStamp ts = new MillisecondTimeStamp(1000);
    assertEquals(1000, ts.msEpoch());
    assertEquals(1, ts.epoch());
    
    ts = new MillisecondTimeStamp(-1000);
    assertEquals(-1000, ts.msEpoch());
    assertEquals(-1, ts.epoch());
  }
  
  @Test
  public void getCopy() throws Exception {
    TimeStamp ts = new MillisecondTimeStamp(1000);
    assertEquals(1000, ts.msEpoch());
    assertEquals(1, ts.epoch());
    
    TimeStamp copy = ts.getCopy();
    assertNotSame(ts, copy);
    assertEquals(1000, ts.msEpoch());
    assertEquals(1, ts.epoch());
  }
  
  @Test
  public void update() throws Exception {
    TimeStamp ts = new MillisecondTimeStamp(1000);
    ts.updateMsEpoch(2000);
    assertEquals(2000, ts.msEpoch());
    assertEquals(2, ts.epoch());
    
    ts.updateEpoch(3);
    assertEquals(3000, ts.msEpoch());
    assertEquals(3, ts.epoch());
    
    TimeStamp copy_into = new MillisecondTimeStamp(1000);
    assertEquals(1000, copy_into.msEpoch());
    assertEquals(1, copy_into.epoch());
    
    copy_into.update(ts);
    assertEquals(3000, copy_into.msEpoch());
    assertEquals(3, copy_into.epoch());
  }
  
  @Test
  public void compare() throws Exception {
    final TimeStamp ts1 = new MillisecondTimeStamp(1000);
    final TimeStamp ts2 = new MillisecondTimeStamp(2000);
    
    assertTrue(ts1.compare(TimeStampComparator.LT, ts2));
    assertTrue(ts1.compare(TimeStampComparator.LTE, ts2));
    assertFalse(ts1.compare(TimeStampComparator.GT, ts2));
    assertFalse(ts1.compare(TimeStampComparator.GTE, ts2));
    assertFalse(ts1.compare(TimeStampComparator.EQ, ts2));
    assertTrue(ts1.compare(TimeStampComparator.NE, ts2));
    
    ts2.updateMsEpoch(1000);
    assertTrue(ts1.compare(TimeStampComparator.EQ, ts2));
    
    try {
      ts1.compare(TimeStampComparator.LT, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      ts1.compare(null, ts2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void setMax() throws Exception {
    TimeStamp ts = new MillisecondTimeStamp(1000);
    assertEquals(1000, ts.msEpoch());
    assertEquals(1, ts.epoch());
    
    ts.setMax();
    assertEquals(Long.MAX_VALUE, ts.msEpoch());
    assertEquals(Long.MAX_VALUE / 1000, ts.epoch());
  }
}
