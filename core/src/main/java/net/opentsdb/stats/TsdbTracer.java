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
package net.opentsdb.stats;

import net.opentsdb.core.TsdbPlugin;

/**
 * A plugin abstraction definition used for implementations of the OpenTracing
 * API including reporters, etc.
 * 
 * @since 3.0
 */
public abstract class TsdbTracer extends TsdbPlugin {

  /**
   * Returns a new tracer for an operation. Uses the service name from the 
   * 'tsdb.tracer.service_name' config.
   * @param report Whether or not the tracer should report to external systems
   * or remain in-memory.
   * @return A trace object with methods to serialize the tracer results.
   */
  public abstract TsdbTrace getTracer(final boolean report);
  
  /**
   * Returns a new tracer for an operation with an override for the service 
   * name.
   * @param report Whether or not the tracer should report to external systems
   * or remain in-memory.
   * @param service_name An override for the service name. If null or empty,
   * the default is used.
   * @return A trace object with methods to serialize the tracer results.
   */
  public abstract TsdbTrace getTracer(final boolean report, 
                                      final String service_name);
  
}
