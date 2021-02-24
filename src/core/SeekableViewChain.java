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

import java.util.List;
import java.util.NoSuchElementException;

public class SeekableViewChain implements SeekableView {

    private final List<SeekableView> iterators;
    private int currentIterator;

    SeekableViewChain(List<SeekableView> iterators) {
        this.iterators = iterators;
    }

    /**
     * Returns {@code true} if this view has more elements.
     */
    @Override
    public boolean hasNext() {
        SeekableView iterator = getCurrentIterator();
        return iterator != null && iterator.hasNext();
    }

    /**
     * Returns a <em>view</em> on the next data point.
     * No new object gets created, the referenced returned is always the same
     * and must not be stored since its internal data structure will change the
     * next time {@code next()} is called.
     *
     * @throws NoSuchElementException if there were no more elements to iterate
     *                                on (in which case {@link #hasNext} would have returned {@code false}.
     */
    @Override
    public DataPoint next() {
        SeekableView iterator = getCurrentIterator();
        if (iterator == null || !iterator.hasNext()) {
            throw new NoSuchElementException("No elements left in iterator");
        }

        DataPoint next = iterator.next();

        if (!iterator.hasNext()) {
            currentIterator++;
        }

        return next;
    }

    /**
     * Unsupported operation.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removing items is not supported");
    }

    /**
     * Advances the iterator to the given point in time.
     * <p>
     * This allows the iterator to skip all the data points that are strictly
     * before the given timestamp.
     *
     * @param timestamp A strictly positive 32 bit UNIX timestamp (in seconds).
     * @throws IllegalArgumentException if the timestamp is zero, or negative,
     *                                  or doesn't fit on 32 bits (think "unsigned int" -- yay Java!).
     */
    @Override
    public void seek(long timestamp) {
        for (final SeekableView it : iterators) {
            it.seek(timestamp);
        }
    }

    private SeekableView getCurrentIterator() {
        while (currentIterator < iterators.size()) {
            if (iterators.get(currentIterator).hasNext()) {
                return iterators.get(currentIterator);
            }
            currentIterator++;
        }
        return null;
    }
}
