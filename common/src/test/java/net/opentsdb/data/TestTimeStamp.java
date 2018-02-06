// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
