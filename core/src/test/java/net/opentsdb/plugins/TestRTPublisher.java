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

package net.opentsdb.plugins;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.AnnotationFixtures;
import net.opentsdb.uid.TimeseriesId;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

public abstract class TestRTPublisher {
  protected RealTimePublisher rt_publisher;

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
    final Annotation ann = AnnotationFixtures.provideAnnotation();
    assertNotNull(rt_publisher.publishAnnotation(ann));
  }
}
