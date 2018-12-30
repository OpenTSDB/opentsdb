// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.expression;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * An interface that helps merge different time series sets (e.g. different
 * metrics with a group by operator). The implementations handle joining the
 * two sets according to the {@link SetOperator}.
 * @since 2.3
 */
public interface VariableIterator {

  /** An operator that determines how to sets of time series are merged via
   * expression. */
  public enum SetOperator {
    /** A union, meaning results from all sets will appear, using FillPolicies
     * for missing series */
    UNION("union"),
    
    /** Computes the intersection, returning results only for series that appear
     * in all sets */
    INTERSECTION("intersection");
    
    /** The user-friendly name of this operator. */
    private final String name;
    
    /** @param the readable name of the operator */
    SetOperator(final String name) {
      this.name = name;
    }
    
    /** @return the readable name of the operator */
    @JsonValue
    public String getName() {
      return name;
    }
    
    /** 
     * Converts a string to lower case then looks up the operator
     * @param name The name to find an operator for
     * @return The operator if found.
     * @throws IllegalArgumentException if the operator wasn't found
     */
    @JsonCreator
    public static SetOperator fromString(final String name) {
      for (final SetOperator operator : SetOperator.values()) {
        if (operator.name.equalsIgnoreCase(name)) {
          return operator;
        }
      }
      throw new IllegalArgumentException("Unrecognized set operator: " + name);
    }
  }
  
  /**
   * Whether or not another set of results are available. Always call this
   * before calling next.
   * @return True if more results are available, false if not.
   */
  public boolean hasNext();
  
  /**
   * Iterates the {@link getResults()} to the next set of results. If there
   * aren't any results left, the implementation may throw an exception. Always
   * call {@link hasNext()} first.
   */
  public void next();

  /** 
   * Determines whether the individual series in the {@code values} array has 
   * another value. This may be used for non-synchronous iteration.
   * @param index The index of the series in the values array to check for
   * @return True if the series has another value, false if not
   */
  public boolean hasNext(final int index);
  
  /**
   * Fetches the next value for an individual series in the {@code values} array.
   * @param index The index of the series in the values array to advance
   */
  public void next(final int index);  
  
  /**
   * Returns a map of variable names to result series. You can maintain the
   * reference returned without having to call getResults() on every iteration.
   * Calling {@link next()} will simply update the ExpressionDataPoint array.
   * The implementation may return a null map if there weren't any results
   * available. Always all {@link hasNext()} before getting the results.
   * @return A map with results to read from.
   */
  public Map<String, ExpressionDataPoint[]> getResults();
  
  /** @return The number of time series after the join. This should match the
   * number of entries in the results data point array, not the number of 
   * variables in the results map. */
  public int getSeriesSize();
  
  /** @return the next timestamp for all results without iterating */
  public long nextTimestamp();
}
