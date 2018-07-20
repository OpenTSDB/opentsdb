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

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

import net.opentsdb.data.pbuf.NumericDatumPB.NumericDatum;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.query.serdes.PBufDatumSerdes;

/**
 * A serdes implementation to convert NumericType values.
 * 
 * @since 3.0
 */
public class PBNumericDatumSerdes implements PBufDatumSerdes {

  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericType.TYPE;
  }

  @Override
  public Any serialize(TimeSeriesDataType value) {
    return Any.pack(NumericDatum.newBuilder()
        .setIsInteger(((NumericType) value).isInteger() ? true : false)
        .setValue(((NumericType) value).isInteger() ?
            ((NumericType) value).longValue() :
              Double.doubleToRawLongBits(((NumericType) value).doubleValue()))
        .build());
  }

  @Override
  public TimeSeriesDataType deserialize(final Any data) {
    if (!data.is(NumericDatum.class)) {
      throw new SerdesException("The value was not a NumericDatum "
          + "object: " + data.getTypeUrl());
    }
    try {
      final NumericDatum datum = data.unpack(NumericDatum.class);
      return new PBufNumericDatum(datum);
    } catch (InvalidProtocolBufferException e) {
      throw new SerdesException("Failed to unpack data of type: " 
          + data.getTypeUrl());
    }
  }

}
