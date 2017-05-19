// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by haiyang on 6/15/16.
 */
public interface HistogramSeekableView extends Iterator<HistogramDataPoint> {

    /**
     * Returns {@code true} if this view has more elements.
     */
    boolean hasNext();

    /**
     * Returns a <em>view</em> on the next data point.
     * No new object gets created, the referenced returned is always the same
     * and must not be stored since its internal data structure will change the
     * next time {@code next()} is called.
     * @throws NoSuchElementException if there were no more elements to iterate
     * on (in which case {@link #hasNext} would have returned {@code false}.
     */
    HistogramDataPoint next();

    /**
     * Unsupported operation.
     * @throws UnsupportedOperationException always.
     */
    void remove();

    /**
     * Advances the iterator to the given point in time.
     * <p>
     * This allows the iterator to skip all the data points that are strictly
     * before the given timestamp.
     * @param timestamp A strictly positive 32 bit UNIX timestamp (in seconds).
     * @throws IllegalArgumentException if the timestamp is zero, or negative,
     * or doesn't fit on 32 bits (think "unsigned int" -- yay Java!).
     */
    void seek(long timestamp);

}
