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
package net.opentsdb.query.serdes;

import java.io.InputStream;
import java.io.OutputStream;

import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.stats.Span;

/**
 * TODO - playing around with this.
 * 
 * @since 3.0
 */
public interface TimeSeriesDataSerdes {

  public void serialize(final SerdesOptions options, 
                        final TimeSeriesDatum datum, 
                        final OutputStream stream, 
                        final Span span);
  
  public void serialize(final SerdesOptions options, 
                        final TimeSeriesSharedTagsAndTimeData data, 
                        final OutputStream stream, 
                        final Span span);
  
  public TimeSeriesDatum deserializeDatum(final SerdesOptions options, 
                                          final InputStream stream,
                                          final Span span);
  
  public TimeSeriesSharedTagsAndTimeData deserializeShared(
      final SerdesOptions options, 
      final InputStream stream,
      final Span span); 
  
  
}
