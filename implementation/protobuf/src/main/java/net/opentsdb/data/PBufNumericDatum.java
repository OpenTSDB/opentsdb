// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.data;

import net.opentsdb.data.pbuf.NumericDatumPB.NumericDatum;
import net.opentsdb.data.types.numeric.NumericType;

/**
 * An implementation of the {@link NumericType} from OpenTSDB.
 * 
 * @since 3.0
 */
public class PBufNumericDatum implements NumericType {
  
  /** The encoded source. */
  private final NumericDatum source;
  
  /**
   * Default ctor.
   * @param source A non-null source.
   */
  public PBufNumericDatum(final NumericDatum source) {
    this.source = source;
  }
  
  @Override
  public boolean isInteger() {
    return source.getIsInteger();
  }

  @Override
  public long longValue() {
    if (!source.getIsInteger()) {
      throw new ClassCastException("Not a long in " + source);
    }
    return source.getValue();
  }

  @Override
  public double doubleValue() {
    if (source.getIsInteger()) {
      throw new ClassCastException("Not a double in " + source);
    }
    return Double.longBitsToDouble(source.getValue());
  }

  @Override
  public double toDouble() {
    if (isInteger()) {
      return (double) source.getValue();
    }
    return Double.longBitsToDouble(source.getValue());
  }

}
