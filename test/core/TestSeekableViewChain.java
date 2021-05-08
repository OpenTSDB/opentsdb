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
package net.opentsdb.core;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(PowerMockRunner.class)
public class TestSeekableViewChain {

    private static final long BASE_TIME = 1356998400000L;
    private static final DataPoint[] DATA_POINTS_1 = new DataPoint[]{
            MutableDataPoint.ofLongValue(BASE_TIME, 40),
            MutableDataPoint.ofLongValue(BASE_TIME + 10000, 50),
            MutableDataPoint.ofLongValue(BASE_TIME + 30000, 70)
    };

    @Before
    public void before() throws Exception {

    }

    @Test
    public void testIteratorChain() {
        List<SeekableView> iterators = new ArrayList<SeekableView>();
        for (int i = 0; i < 3; i++) {
            iterators.add(SeekableViewsForTest.fromArray(DATA_POINTS_1));
        }
        SeekableViewChain chain = new SeekableViewChain(iterators);

        int items = 0;
        while (chain.hasNext()) {
            chain.next();
            items += 1;
        }

        assertEquals(9, items);
    }

    @Test
    public void testSeek() {
        List<SeekableView> iterators = new ArrayList<SeekableView>();
        iterators.add(SeekableViewsForTest.generator(
                BASE_TIME, 10000, 5, true
        ));
        iterators.add(SeekableViewsForTest.generator(
                BASE_TIME + 50000, 10000, 5, true
        ));

        SeekableViewChain chain = new SeekableViewChain(iterators);

        chain.seek(BASE_TIME + 75000);

        int items = 0;
        while (chain.hasNext()) {
            chain.next();
            items += 1;
        }

        assertEquals(2, items);
    }

    @Test
    public void testEmptyChain() {
        SeekableViewChain chain = new SeekableViewChain(new ArrayList<SeekableView>());
        assertFalse(chain.hasNext());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveUnsupported() {
        makeChain(1).remove();
    }

    private SeekableViewChain makeChain(int numIterators) {
        List<SeekableView> iterators = new ArrayList<SeekableView>();
        for (int i = 0; i < numIterators; i++) {
            iterators.add(SeekableViewsForTest.fromArray(DATA_POINTS_1));
        }
        return new SeekableViewChain(iterators);
    }
}
