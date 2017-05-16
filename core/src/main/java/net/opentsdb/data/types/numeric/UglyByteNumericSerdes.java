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
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.iterators.TimeSeriesIterator;
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
public class UglyByteNumericSerdes extends 
  TimeSeriesSerdes<TimeSeriesIterator<NumericType>> {

  @Override
  public void serialize(final TimeSeriesQuery query,
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
      
      stream.write(Bytes.fromInt(id.alias() == null ? 0 : id.alias().length));
      if (id.alias() != null && id.alias().length > 0) {
        stream.write(id.alias());
      }
      
      stream.write(Bytes.fromInt(id.namespaces().size()));
      for (final byte[] namespace : id.namespaces()) {
        stream.write(Bytes.fromInt(namespace.length));
        if (namespace.length > 0) {
          stream.write(namespace);
        }
      }
      
      stream.write(Bytes.fromInt(id.metrics().size()));
      for (final byte[] metric : id.metrics()) {
        stream.write(Bytes.fromInt(metric.length));
        stream.write(metric);
      }
      
      stream.write(Bytes.fromInt(id.tags().size()));
      for (final Entry<byte[], byte[]> pair : id.tags().entrySet()) {
        stream.write(Bytes.fromInt(pair.getKey().length));
        stream.write(pair.getKey());
        stream.write(Bytes.fromInt(pair.getValue().length));
        stream.write(pair.getValue());
      }
      
      stream.write(Bytes.fromInt(id.aggregatedTags().size()));
      for (final byte[] tag : id.aggregatedTags()) {
        stream.write(Bytes.fromInt(tag.length));
        stream.write(tag);
      }
      
      stream.write(Bytes.fromInt(id.disjointTags().size()));
      for (final byte[] tag : id.disjointTags()) {
        stream.write(Bytes.fromInt(tag.length));
        stream.write(tag);
      }
      
      ((NumericMillisecondShard) data).serialize(stream);
      stream.flush();
    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception during "
          + "serialization of: " + data, e);
    }
  }

  @Override
  public TimeSeriesIterator<NumericType> deserialize(final InputStream stream) {
    if (stream == null) {
      throw new IllegalArgumentException("Input stream may not be null.");
    }
    try {
      final SimpleStringTimeSeriesId.Builder id = 
          SimpleStringTimeSeriesId.newBuilder();
      byte[] buf = new byte[4];
      stream.read(buf);
      int len = Bytes.getInt(buf);
      if (len > 0) {
        byte[] array = new byte[len];
        stream.read(array);
        id.setAlias(new String(array, Const.UTF8_CHARSET));
      }
      
      stream.read(buf);
      int namespaces = Bytes.getInt(buf);
      for (int x = 0; x < namespaces; x++) {
        stream.read(buf);
        byte[] array = new byte[Bytes.getInt(buf)];
        stream.read(array);
        id.addNamespace(new String(array, Const.UTF8_CHARSET));
      }
      
      stream.read(buf);
      int metrics = Bytes.getInt(buf);
      for (int x = 0; x < metrics; x++) {
        stream.read(buf);
        byte[] array = new byte[Bytes.getInt(buf)];
        stream.read(array);
        id.addMetric(new String(array, Const.UTF8_CHARSET));
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
