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
package net.opentsdb.query.processor;

import java.util.List;

import org.junit.Ignore;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.query.context.QueryContext;

/**
 * Helpers for testing out iterators.
 * <p>
 * For states, a * means a point is present and a _ means it's missing.
 */
@Ignore
public class ProcessorTestsHelpers {

  /**
   *   _ | * | *
   *   * | * | *
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState1(final MockNumericIterator a, 
      final MockNumericIterator b) {
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    b.data = data;
  }
  
  /**
   *   _ | _ | *
   *   * | * | *
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState2(final MockNumericIterator a, 
      final MockNumericIterator b) {
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    b.data = data;
  }

  /**
   *   * | _ | *
   *   * | * | *
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState3(final MockNumericIterator a, 
      final MockNumericIterator b) {
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    b.data = data;
  }

  /**
   *   * | _ | _
   *   * | * | *
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState4(final MockNumericIterator a, 
      final MockNumericIterator b) {
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    b.data = data;
  }

  /**
   *   * | _ | _
   *   _ | * | *
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState5(final MockNumericIterator a, 
      final MockNumericIterator b) {
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    b.data = data;
  }

  /**
   *   * | _ | _
   *   _ | _ | *
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState6(final MockNumericIterator a, 
      final MockNumericIterator b) {
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    b.data = data;
  }

  /**
   *   * | _ | _
   *   _ | _ | _
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState7(final MockNumericIterator a, 
      final MockNumericIterator b) {
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    b.data = data;
  }

  /**
   *   _ | * | _
   *   _ | _ | _
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState8(final MockNumericIterator a, 
      final MockNumericIterator b) {
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    b.data = data;
  }

  /**
   *   _ | _ | *
   *   _ | _ | _
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState9(final MockNumericIterator a, 
      final MockNumericIterator b) {
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    b.data = data;
  }

  /**
   *   * | _ | *
   *   _ | * | _
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState10(final MockNumericIterator a, 
      final MockNumericIterator b) {
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    b.data = data;
  }

  /**
   *   * | _ | *
   *   _ | * <-- done early
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState11(final MockNumericIterator a, 
      final MockNumericIterator b) {
    
    List<List<MutableNumericType>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(a.id(), new MillisecondTimeStamp(3000), 3, 1));
    data.add(set);
    a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(1000), 1, 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(2000), 2, 1));
    data.add(set);
    
    //set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(b.id(), new MillisecondTimeStamp(3000), 3, 1));
    //data.add(set);
    b.data = data;
  }

  /**
   * Mock that overloads the init and callback methods so that we can inject
   * or throw exceptions AND track the initialization order for proper testing.
   */
  public static class MockProcessor extends TimeSeriesProcessor {
    int throw_exception; // 0 = nope, 1 = in init, 2 = in callback
    final int id;
    final IllegalStateException e;
    
    public MockProcessor(final int id, final IllegalStateException e) throws Exception {
      this.id = id;
      this.e = e;
    }
    
    /** @param i 0 = nope, 1 = in init, 2 = in callback */
    public void setThrowException(final int i) {
      throw_exception = i;
    }
    
    @Override
    public Deferred<Object> initialize() {
      if (throw_exception == 1) {
        throw e;
      }
      if (e != null) {
        init_deferred.callback(e);
      } else {
        init_deferred.callback(null);
      }
      return init_deferred;
    }

    @Override
    public Callback<Deferred<Object>, Object> initializationCallback() {
      class InitCB implements Callback<Deferred<Object>, Object> {
        @Override
        public Deferred<Object> call(final Object result_or_exception) 
            throws Exception {
          if (result_or_exception instanceof Exception) {
            init_deferred.callback((Exception) result_or_exception);
            return init_deferred;
          }
          if (throw_exception == 2) {
            throw e;
          }
          return initialize();
        }
      }
      return new InitCB();
    }
    
    @Override
    public String toString() {
      return "[ID] " + id;
    }

    @Override
    public TimeSeriesProcessor getClone(QueryContext context) {
      return null;
    }
  }
}
