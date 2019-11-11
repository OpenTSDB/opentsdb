// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.util.*;

import org.hbase.async.Bytes;
import org.hbase.async.Bytes.ByteMap;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.meta.Annotation;
import net.opentsdb.rollup.RollupQuery;

/**
 * Groups multiple spans together and offers a dynamic "view" on them.
 * <p>
 * This is used for queries to the TSDB, where we might group multiple
 * {@link Span}s that are for the same time series but different tags
 * together.  We need to "hide" data points that are outside of the
 * time period of the query and do on-the-fly aggregation of the data
 * points coming from the different Spans, using an {@link Aggregator}.
 * Since not all the Spans will have their data points at exactly the
 * same time, we also do on-the-fly linear interpolation.  If needed,
 * this view can also return the rate of change instead of the actual
 * data points.
 * <p>
 * This is one of the rare (if not the only) implementations of
 * {@link DataPoints} for which {@link #getTags} can potentially return
 * an empty map.
 * <p>
 * The implementation can also dynamically downsample the data when a
 * sampling interval a downsampling function (in the form of an
 * {@link Aggregator}) are given.  This is done by using a special
 * iterator when using the {@link Span.DownsamplingIterator}.
 */
abstract class AbstractSpanGroup implements DataPoints {
    /**
     * Finds the {@code i}th data point of this group in {@code O(n)}.
     * Where {@code n} is the number of data points in this group.
     */
    protected DataPoint getDataPoint(int i) {
        if (i < 0) {
            throw new IndexOutOfBoundsException("negative index: " + i);
        }
        final int saved_i = i;
        final SeekableView it = iterator();
        DataPoint dp = null;
        while (it.hasNext() && i >= 0) {
            dp = it.next();
            i--;
        }
        if (i != -1 || dp == null) {
            throw new IndexOutOfBoundsException("index " + saved_i
                    + " too large (it's >= " + size() + ") for " + this);
        }
        return dp;
    }
}
