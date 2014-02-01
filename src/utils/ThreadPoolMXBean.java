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
package net.opentsdb.utils;

/**
 * <p>Title: ThreadPoolMXBean</p>
 * <p>Description: MXBean interface for thread pool executors</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.utils.ThreadPoolMXBean</code></p>
 */

public interface ThreadPoolMXBean {
    /**
     * Returns the approximate number of threads that are actively executing tasks.
     * @return the approximate number of threads that are actively executing tasks.
     */
    public int getActiveCount();

    /**
     * Returns the core number of threads.
     * @return the core number of threads.
     */
    public int getCorePoolSize();

    /**
     * Returns the largest number of threads that have ever simultaneously been in the pool.
     * @return the largest number of threads that have ever simultaneously been in the pool.
     */
    public int getLargestPoolSize();

    /**
     * Returns the current number of threads in the pool.
     * @return the current number of threads in the pool.
     */
    public int getPoolSize();

    /**
     * Returns the approximate total number of tasks that have completed execution.
     * @return the approximate total number of tasks that have completed execution.
     */
    public long getCompletedTaskCount();

}

