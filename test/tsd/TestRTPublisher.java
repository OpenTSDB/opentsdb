// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import net.opentsdb.meta.Annotation;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertNotNull;

public abstract class TestRTPublisher {
  protected RTPublisher rt_publisher;

  @Test
  public void shutdown() throws Exception {
    assertNotNull(rt_publisher.shutdown());
  }

  @Test
  public void sinkDataPoint() throws Exception {
    assertNotNull(rt_publisher.sinkDataPoint("sys.cpu.user",
            System.currentTimeMillis(), new byte[]{0, 0, 0, 0, 0, 0, 0, 1},
            null, null, (short) 0x7));
  }

  @Test
  public void publishAnnotation() throws Exception {
    Annotation ann = new Annotation();
    HashMap<String, String> customMap = new HashMap<String, String>(1);
    customMap.put("test-custom-key", "test-custom-value");
    ann.setCustom(customMap);
    ann.setDescription("A test annotation");
    ann.setNotes("Test annotation notes");
    ann.setStartTime(System.currentTimeMillis());
    assertNotNull(rt_publisher.publishAnnotation(ann));
  }
}
