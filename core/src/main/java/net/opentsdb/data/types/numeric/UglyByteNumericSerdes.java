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
// see <http://www.gnu.org/licenses/>.package net.opentsdb.data;
package net.opentsdb.data.types.numeric;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map.Entry;

import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.execution.serdes.SerdesOptions;
import net.opentsdb.query.execution.serdes.TimeSeriesSerdes;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.Bytes;

/**
 * Just a super ugly and uncompressed serialization method for dumping
 * a numeric time series into a byte encoded output stream and back in.
 * <p>
 * TODO - Right now it's only supporting the {@link NumericMillisecondShard}
 * since it *does* provide a modicum of compression.
 * <p>
 * TODO - Handle ID serdes outside of this class. Since it's shared by all types.
 * @since 3.0
 */
public class UglyByteNumericSerdes implements 
  TimeSeriesSerdes<TimeSeriesIterator<NumericType>> {

  @Override
  public void serialize(final TimeSeriesQuery query,
                        final SerdesOptions options,
                        final OutputStream stream,
                        final TimeSeriesIterator<NumericType> data) {
    if (stream == null) {
      throw new IllegalArgumentException("Output stream may not be null.");
    }
    if (data == null) {
      throw new IllegalArgumentException("Data may not be null.");
    }
    if (data.id() == null) {
      throw new IllegalArgumentException("Iterator ID cannot be null.");
    }
    if (!(data instanceof NumericMillisecondShard)) {
      throw new UnsupportedOperationException(
          "Not writing this type of number data yet: " + data.getClass());
    }
    try {
      final TimeSeriesId id = data.id();
      
      byte[] buf = id.alias() == null ? null : id.alias().getBytes(Const.UTF8_CHARSET);
      stream.write(Bytes.fromInt(buf == null ? 0 : buf.length));
      if (buf != null) {
        stream.write(buf);
      }
      
      buf = id.namespace() == null ? null : id.namespace().getBytes(Const.UTF8_CHARSET);
      stream.write(Bytes.fromInt(buf == null ? 0 : buf.length));
      if (buf != null) {
        stream.write(buf);
      }
      
      buf = id.metric().getBytes(Const.UTF8_CHARSET);
      stream.write(Bytes.fromInt(buf.length));
      stream.write(buf);
      
      stream.write(Bytes.fromInt(id.tags().size()));
      for (final Entry<String, String> pair : id.tags().entrySet()) {
        buf = pair.getKey().getBytes(Const.UTF8_CHARSET);
        stream.write(Bytes.fromInt(buf.length));
        stream.write(buf);
        buf = pair.getValue().getBytes(Const.UTF8_CHARSET);
        stream.write(Bytes.fromInt(buf.length));
        stream.write(buf);
      }
      
      stream.write(Bytes.fromInt(id.aggregatedTags().size()));
      for (final String tag : id.aggregatedTags()) {
        buf = tag.getBytes(Const.UTF8_CHARSET);
        stream.write(Bytes.fromInt(buf.length));
        stream.write(buf);
      }
      
      stream.write(Bytes.fromInt(id.disjointTags().size()));
      for (final String tag : id.disjointTags()) {
        buf = tag.getBytes(Const.UTF8_CHARSET);
        stream.write(Bytes.fromInt(buf.length));
        stream.write(buf);
      }
      
      ((NumericMillisecondShard) data).serialize(stream);
      stream.flush();
    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception during "
          + "serialization of: " + data, e);
    }
  }

  @Override
  public TimeSeriesIterator<NumericType> deserialize(final SerdesOptions options,
                                                     final InputStream stream) {
    if (stream == null) {
      throw new IllegalArgumentException("Input stream may not be null.");
    }
    try {
      final BaseTimeSeriesId.Builder id = 
          BaseTimeSeriesId.newBuilder();
      byte[] buf = new byte[4];
      stream.read(buf);
      int len = Bytes.getInt(buf);
      if (len > 0) {
        byte[] array = new byte[len];
        stream.read(array);
        id.setAlias(new String(array, Const.UTF8_CHARSET));
      }
      
      stream.read(buf);
      len = Bytes.getInt(buf);
      if (len > 0) {
        byte[] array = new byte[len];
        stream.read(array);
        id.setNamespace(new String(array, Const.UTF8_CHARSET));
      }
      
      stream.read(buf);
      len = Bytes.getInt(buf);
      if (len > 0) {
        byte[] array = new byte[len];
        stream.read(array);
        id.setMetric(new String(array, Const.UTF8_CHARSET));
      }
      
      stream.read(buf);
      int tags = Bytes.getInt(buf);
      for (int x = 0; x < tags; x++) {
        stream.read(buf);
        byte[] tagk = new byte[Bytes.getInt(buf)];
        stream.read(tagk);
        
        stream.read(buf);
        byte[] tagv = new byte[Bytes.getInt(buf)];
        stream.read(tagv);
        id.addTags(new String(tagk, Const.UTF8_CHARSET), 
            new String(tagv, Const.UTF8_CHARSET));
      }
      
      // agg tags
      stream.read(buf);
      tags = Bytes.getInt(buf);
      for (int x = 0; x < tags; x++) {
        stream.read(buf);
        byte[] array = new byte[Bytes.getInt(buf)];
        stream.read(array);
        id.addAggregatedTag(new String(array, Const.UTF8_CHARSET));
      }
      
      // disjoints
      stream.read(buf);
      tags = Bytes.getInt(buf);
      for (int x = 0; x < tags; x++) {
        stream.read(buf);
        byte[] array = new byte[Bytes.getInt(buf)];
        stream.read(array);
        id.addDisjointTag(new String(array, Const.UTF8_CHARSET));
      }
      
      final NumericMillisecondShard shard = 
          NumericMillisecondShard.parseFrom(id.build(), stream);
      return shard;
    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception deserializing stream: " 
          + stream, e);
    }
  }

}
