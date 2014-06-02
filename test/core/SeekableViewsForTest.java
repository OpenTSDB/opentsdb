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

import java.util.NoSuchElementException;

import org.junit.Ignore;

/** Helper class to mock SeekableView. */
@Ignore
public class SeekableViewsForTest {

  /**
   * Creates a {@link SeekableView} object to iterate the given data points.
   * @param data_points Test data.
   * @return A {@link SeekableView} object
   */
  public static SeekableView fromArray(final DataPoint[] data_points) {
    return new MockSeekableView(data_points);
  }

  /** Iterates an array of data points. */
  private static class MockSeekableView implements SeekableView {

    private final DataPoint[] data_points;
    private int index = 0;

    MockSeekableView(final DataPoint[] data_points) {
      this.data_points = data_points;
    }

    @Override
    public boolean hasNext() {
      return data_points.length > index;
    }

    @Override
    public DataPoint next() {
      if (hasNext()) {
        return data_points[index++];
      }
      throw new NoSuchElementException("no more values");
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long timestamp) {
      for (index = 0; index < data_points.length; ++index) {
        if (data_points[index].timestamp() >= timestamp) {
          break;
        }
      }
    }
  }
}