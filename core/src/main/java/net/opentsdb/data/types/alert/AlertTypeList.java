// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.data.types.alert;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;

/**
 * A class to handle a list of alert values and provide an iterator over them.
 * 
 * @since 3.0
 */
public class AlertTypeList {

  private final List<AlertValue> values;
  
  public AlertTypeList() {
    values = Lists.newArrayList();
  }
  
  public void add(final AlertValue value) {
    values.add(value);
  }
  
  public TypedTimeSeriesIterator<AlertType> getIterator() {
    return new AlertTypeIterator();
  }
  
  class AlertTypeIterator implements TypedTimeSeriesIterator<AlertType> {
    private int idx = 0;

    @Override
    public boolean hasNext() {
      return idx < values.size();
    }

    @Override
    public TimeSeriesValue<AlertType> next() {
      return values.get(idx++);
    }

    @Override
    public TypeToken<AlertType> getType() {
      return AlertType.TYPE;
    }
    
    @Override
    public void close() {
      // no-op for now
    }
    
  }
}