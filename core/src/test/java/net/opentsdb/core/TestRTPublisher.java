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
package net.opentsdb.core;

import com.google.common.collect.ImmutableMap;
import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.TimeseriesId;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public abstract class TestRTPublisher {
  protected RTPublisher rt_publisher;

  @Test
  public void sinkLongDataPoint() {
    assertNotNull(rt_publisher.publishDataPoint("sys.cpu.user", 123123123, 123,
        ImmutableMap.of("host", "east"), mock(TimeseriesId.class)));
  }

  @Test
  public void sinkDoubleDataPoint() {
    assertNotNull(rt_publisher.publishDataPoint("sys.cpu.user", 123123123, 12.5,
        ImmutableMap.of("host", "east"), mock(TimeseriesId.class)));
  }

  @Test
  public void publishAnnotation() {
    Annotation ann = new Annotation();
    HashMap<String, String> customMap = new HashMap<>(1);
    customMap.put("test-custom-key", "test-custom-value");
    ann.setCustom(customMap);
    ann.setDescription("A test annotation");
    ann.setNotes("Test annotation notes");
    ann.setStartTime(System.currentTimeMillis());
    assertNotNull(rt_publisher.publishAnnotation(ann));
  }
}
