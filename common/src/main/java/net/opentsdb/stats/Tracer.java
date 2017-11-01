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

import net.opentsdb.core.TSDBPlugin;

/**
 * A tracing implementation for OpenTSDB that abstracts the actual framework.
 * This is meant to be implemented via plugins.
 * 
 * TODO - need APIs to pull from existing traces.
 * 
 * @since 3.0
 */
public interface Tracer extends TSDBPlugin {

  /**
   * Returns a new trace using the default service name.
   * @param report Whether or not the trace should report externally.
   * @param debug Whether or not to capture debug information.
   * @return A non-null trace ready to record spans.
   */
  public Trace newTrace(final boolean report, final boolean debug);
  
  /**
   * Returns a new trace.
   * @param report Whether or not the trace should report externally.
   * @param debug Whether or not to capture debug information.
   * @param service_name A non-null and non-empty service name.
   * @return A non-null trace ready to record spans.
   */
  public Trace newTrace(final boolean report, 
                        final boolean debug, 
                        final String service_name);
  
}
