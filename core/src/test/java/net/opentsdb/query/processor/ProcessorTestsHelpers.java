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

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.MutableNumericType;

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

}
