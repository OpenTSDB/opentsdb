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

import org.hbase.async.HBaseException;

public abstract class AbstractQuery implements Query {
    /**
     * Runs this query.
     *
     * @return The data points matched by this query.
     * <p>
     * Each element in the non-{@code null} but possibly empty array returned
     * corresponds to one time series for which some data points have been
     * matched by the query.
     * @throws HBaseException if there was a problem communicating with HBase to
     *                        perform the search.
     */
    @Override
    public DataPoints[] run() throws HBaseException {
        try {
            return runAsync().joinUninterruptibly();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Should never be here", e);
        }
    }

    /**
     * Runs this query.
     *
     * @return The data points matched by this query and applied with percentile calculation
     * <p>
     * Each element in the non-{@code null} but possibly empty array returned
     * corresponds to one time series for which some data points have been
     * matched by the query.
     * @throws HBaseException        if there was a problem communicating with HBase to
     *                               perform the search.
     * @throws IllegalStateException if the query is not a histogram query
     */
    @Override
    public DataPoints[] runHistogram() throws HBaseException {
        if (!isHistogramQuery()) {
            throw new RuntimeException("Should never be here");
        }

        try {
            return runHistogramAsync().joinUninterruptibly();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Should never be here", e);
        }
    }
}
