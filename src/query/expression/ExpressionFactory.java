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

import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.TSDB;

/**
 * A static class that stores and instantiates a static map of the available
 * functions.
 * TODO - Enable plugable expression and load from the class path.
 * Since 2.3
 */
public final class ExpressionFactory {
  
  private static Map<String, Expression> available_functions = 
      new HashMap<String, Expression>();
  
  static {
    available_functions.put("scale", new Scale());
    available_functions.put("absolute", new Absolute());
    available_functions.put("movingAverage", new MovingAverage());
    available_functions.put("highestCurrent", new HighestCurrent());
    available_functions.put("highestMax", new HighestMax());
  }
  
  /** Don't instantiate me! */
  private ExpressionFactory() { }
  
  /**
   * Adds more functions to the map that depend on an instantiated TSDB object.
   * Only call this once please.
   * @param tsdb The TSDB object to initialize with
   */
  public static void addTSDBFunctions(final TSDB tsdb) {
    available_functions.put("divideSeries", new DivideSeries(tsdb));
    available_functions.put("sumSeries", new SumSeries(tsdb));
  }
  
  /**
   * Add an expression to the map.
   * WARNING: The map is not thread safe so don't use this to dynamically
   * modify the map while the TSD is running.
   * @param name The name of the expression
   * @param expr The expression object to store.
   * @throws IllegalArgumentException if the name is null or empty or the
   * function is null.
   */
  static void addFunction(final String name, final Expression expr) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Missing function name");
    }
    if (expr == null) {
      throw new IllegalArgumentException("Function cannot be null");
    }
    available_functions.put(name, expr);
  }
  
  /**
   * Returns the expression function given the name
   * @param function The name of the expression to use  
   * @return The expression when located
   * @throws UnsupportedOperationException if the requested function hasn't
   * been stored in the map.
   */
  public static Expression getByName(final String function) {
    final Expression expression = available_functions.get(function);
    if (expression == null) {
      throw new UnsupportedOperationException("Function " + function 
          + " has not been implemented");
    }
    return expression;
  }
}
