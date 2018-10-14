// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.processor;

import java.util.List;

import org.junit.Ignore;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.types.numeric.MockNumericTimeSeries;
import net.opentsdb.data.types.numeric.MutableNumericValue;

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
  public static void setState1(final MockNumericTimeSeries a, 
      final MockNumericTimeSeries b) {
    
    List<List<MutableNumericValue>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericValue> set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //b.data = data;
  }
  
  /**
   *   _ | _ | *
   *   * | * | *
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState2(final MockNumericTimeSeries a, 
      final MockNumericTimeSeries b) {
    
    List<List<MutableNumericValue>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericValue> set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //b.data = data;
  }

  /**
   *   * | _ | *
   *   * | * | *
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState3(final MockNumericTimeSeries a, 
      final MockNumericTimeSeries b) {
    
    List<List<MutableNumericValue>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericValue> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //b.data = data;
  }

  /**
   *   * | _ | _
   *   * | * | *
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState4(final MockNumericTimeSeries a, 
      final MockNumericTimeSeries b) {
    
    List<List<MutableNumericValue>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericValue> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //b.data = data;
  }

  /**
   *   * | _ | _
   *   _ | * | *
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState5(final MockNumericTimeSeries a, 
      final MockNumericTimeSeries b) {
    
    List<List<MutableNumericValue>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericValue> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //b.data = data;
  }

  /**
   *   * | _ | _
   *   _ | _ | *
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState6(final MockNumericTimeSeries a, 
      final MockNumericTimeSeries b) {
    
    List<List<MutableNumericValue>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericValue> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //b.data = data;
  }

  /**
   *   * | _ | _
   *   _ | _ | _
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState7(final MockNumericTimeSeries a, 
      final MockNumericTimeSeries b) {
    
    List<List<MutableNumericValue>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericValue> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //b.data = data;
  }

  /**
   *   _ | * | _
   *   _ | _ | _
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState8(final MockNumericTimeSeries a, 
      final MockNumericTimeSeries b) {
    
    List<List<MutableNumericValue>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericValue> set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //b.data = data;
  }

  /**
   *   _ | _ | *
   *   _ | _ | _
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState9(final MockNumericTimeSeries a, 
      final MockNumericTimeSeries b) {
    
    List<List<MutableNumericValue>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericValue> set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //b.data = data;
  }

  /**
   *   * | _ | *
   *   _ | * | _
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState10(final MockNumericTimeSeries a, 
      final MockNumericTimeSeries b) {
    
    List<List<MutableNumericValue>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericValue> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //b.data = data;
  }

  /**
   *   * | _ | *
   *   _ | * <-- done early
   * Sets 3 data points, one per "chunk" and three chunks. 
   * @param a The A series to work with.
   * @param b The B series to work with.
   */
  public static void setState11(final MockNumericTimeSeries a, 
      final MockNumericTimeSeries b) {
    
    List<List<MutableNumericValue>> data = Lists.newArrayListWithCapacity(1);
    List<MutableNumericValue> set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(3000), 3));
    data.add(set);
    //a.data = data;
    
    data = Lists.newArrayListWithCapacity(1);
    set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    data.add(set);
    
    set = Lists.newArrayListWithCapacity(1);
    set.add(new MutableNumericValue(new MillisecondTimeStamp(2000), 2));
    data.add(set);
    
    //set = Lists.newArrayListWithCapacity(1);
    //set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    //data.add(set);
    //b.data = data;
  }

}
