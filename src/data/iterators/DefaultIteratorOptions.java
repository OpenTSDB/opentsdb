// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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
package net.opentsdb.data.iterators;

/**
 * A default implementation of the IteratorOptions interface that provides 
 * methods required for the current TSDB query engine.
 * 
 * @since 3.0
 */
public class DefaultIteratorOptions implements IteratorOptions {

  /** Whether or not the iterators returned must be synced on time */
  private boolean time_sync;
  
  /**
   * Private ctor loading data from the builder.
   * @param builder The builder to load from. May not be null or you'll see an
   * NPE.
   */
  private DefaultIteratorOptions(final Builder builder) {
    time_sync = builder.time_sync;
  }
  
  @Override
  public boolean timeSync() {
    return time_sync;
  }

  /** @return A new builder object to construct an options object. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Builder class for the DefaultIteratorOptions class.
   */
  public static class Builder {
    private boolean time_sync;
    
    /**
     * @param time_sync Whether or not iterators returned must be synched on time.
     * @return The builder object.
     */
    public Builder setTimeSync(final boolean time_sync) {
      this.time_sync = time_sync;
      return this;
    }
    
    /** @return A {@link DefaultIteratorOptions} instantiated from this builder. */
    public DefaultIteratorOptions build() {
      return new DefaultIteratorOptions(this); 
    }
  }
}
