// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.summarizer;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryIterator;

public class SummarizerPassThroughNumericArrayIterator implements QueryIterator {
  SummarizedTimeSeries sts;
  
  /** The source iterator. */
  private TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator;

  SummarizerPassThroughNumericArrayIterator(final SummarizedTimeSeries sts) {
    this.sts = sts;
    iterator = sts.source.iterator(NumericArrayType.TYPE).get();
    if (!iterator.hasNext()) {
      sts.fillEmpty();
    }
  }
  
  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }
  
  @Override
  public Object next() {
    final TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    if (value.value() != null) {
      // filter on the query time span.
      final TimeStamp current = sts.result.timeSpecification().start();
      int offset = value.value().offset();
      while (current.compare(Op.LT, 
          sts.result.summarizerNode().pipelineContext().query().startTime())) {
        offset++;
        current.add(sts.result.timeSpecification().interval());
      }
      
      int end = offset;
      while (end < value.value().end() &&
          current.compare(Op.LT, 
              sts.result.summarizerNode().pipelineContext().query().endTime())) {
        end++;
        current.add(sts.result.timeSpecification().interval());
      }
      if (end > value.value().end()) {          
        end = value.value().end();
      }
      if (offset < end) {
        if (value.value().isInteger()) {
          sts.summarize(value.value().longArray(), offset, end);
        } else {
          sts.summarize(value.value().doubleArray(), offset, end);
        }
      }
    }
    return value;
  }
  
  @Override
  public TypeToken getType() {
    return NumericArrayType.TYPE;
  }
  
}
