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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import org.junit.Test;

import java.util.NoSuchElementException;

public final class SpanTest {
  @Test(expected = IllegalArgumentException.class)
  public void ctorEmptyDataPoints() {
    new Span(ImmutableSortedSet.<DataPoints>of());
  }

  @Test(expected = IllegalArgumentException.class)
  public void ctorMissMatchedTSUID() {
    DataPoints row1 = mock(DataPoints.class);
    when(row1.getTSUIDs()).thenReturn(ImmutableList.of("tsuid1"));

    DataPoints row2 = mock(DataPoints.class);
    when(row1.getTSUIDs()).thenReturn(ImmutableList.of("tsuid2"));

    new Span(ImmutableSortedSet.of(row1, row2));
  }

  @Test
  public void datapointForIdxInBounds() {
    final DataPoint dp = mock(DataPoint.class);
    final DataPoints row = mock(DataPoints.class);

    final SeekableView seekableView = mock(SeekableView.class);
    when(seekableView.hasNext()).thenReturn(true);
    when(seekableView.next()).thenReturn(dp);

    when(row.iterator()).thenReturn(seekableView);
    when(row.size()).thenReturn(1);

    final Span span = new Span(ImmutableSortedSet.of(row));

    assertSame(dp, span.dataPointForIndex(0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void datapointForIdxOutsideBounds() {
    final DataPoint dp = mock(DataPoint.class);
    final DataPoints row = mock(DataPoints.class);

    when(row.size()).thenReturn(5);

    final Span span = new Span(ImmutableSortedSet.of(row));

    assertSame(dp, span.dataPointForIndex(10));
  }

  @Test
  public void size() {
    final int row1Size = 2;
    final DataPoints row1 = mock(DataPoints.class);
    when(row1.size()).thenReturn(row1Size);

    final int row2Size = 4;
    final DataPoints row2 = mock(DataPoints.class);
    when(row2.size()).thenReturn(row2Size);

    final Span span = new Span(ImmutableSortedSet.of(row1, row2));

    assertEquals(row1Size + row2Size, span.size());
  }

  @Test
  public void testIterateAllDataPoints() {
    // Mock the first seekable view with 1 datapoint
    final SeekableView seekableView1 = mock(SeekableView.class);

    when(seekableView1.hasNext())
        .thenReturn(true)
        .thenReturn(false);

    when(seekableView1.next())
        .thenReturn(mock(DataPoint.class))
        .thenThrow(new AssertionError(""));

    final DataPoints row1 = mock(DataPoints.class);
    when(row1.iterator()).thenReturn(seekableView1);

    // Mock a second seekable view with 2 datapoints
    final SeekableView seekableView2 = mock(SeekableView.class);

    when(seekableView2.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    when(seekableView2.next())
        .thenReturn(mock(DataPoint.class))
        .thenReturn(mock(DataPoint.class))
        .thenThrow(new AssertionError(""));

    final DataPoints row2 = mock(DataPoints.class);
    when(row2.iterator()).thenReturn(seekableView2);

    // Create a span from the above seekable views. This will create a span with
    // a total of 3 datapoints.
    final Span span = new Span(ImmutableSortedSet.of(row1, row2));
    final SeekableView spanIterator = span.iterator();

    final int numDataPoints = 3;

    for (int datapointIdx = 0; datapointIdx < numDataPoints; datapointIdx++) {
      spanIterator.next();
    }

    try {
      // The span should now have been exhausted and not have any more data
      // points so the next call to next should throw a NoSuchElementException.
      spanIterator.next();
      fail("The span should have been exhausted but was not");
    } catch (NoSuchElementException e) {
      // expected
    }
  }
}
