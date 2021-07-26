/*
 * // This file is part of OpenTSDB.
 * // Copyright (C) 2021  The OpenTSDB Authors.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //   http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package net.opentsdb.data.influx;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.LowLevelMetricData;
import net.opentsdb.data.LowLevelMetricData.HashedLowLevelMetricData;
import net.opentsdb.data.LowLevelMetricData.ValueFormat;
import net.opentsdb.data.LowLevelTimeSeriesData;
import net.opentsdb.data.LowLevelTimeSeriesData.StringFormat;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesDatumStringId;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.storage.TimeSeriesDataConverter;
import net.opentsdb.storage.TimeSeriesDataConverterFactory;
import net.opentsdb.utils.StringUtils;
import net.opentsdb.utils.XXHash;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map.Entry;

/**
 * A converter to encode raw data into Influx Line Protocol payloads. Some work
 * remains, particularly around escaping.
 *
 * TODO - Handle binary IDs? We assume all IDs are strings.
 * TODO - handle parsing the field from the metric. Which dot?
 * TODO - Escape the strings!!!!
 * TODO - Value types. These should be user selectable whether or not we should
 * flag ints as ints.
 * TODO - The decode bits.
 */
public class InfluxLineProtocolConverter extends BaseTSDBPlugin
        implements TimeSeriesDataConverter,
                   TimeSeriesDataConverterFactory {

  public static final String TYPE = "InfluxLineProtocolConverter";

  // This is used to merge metrics into the proper measurements and fields.
  private final ThreadLocal<ILPLines> lineBuffers =
          ThreadLocal.withInitial(() -> new ILPLines());

  @Override
  public TimeSeriesDatum convert(String source) {
    // ILP doesn't easily support a single time series.
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public TimeSeriesDatum convert(byte[] source) {
    // ILP doesn't easily support a single time series.
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public TimeSeriesDatum convert(byte[] source, int offset, int length) {
    // ILP doesn't easily support a single time series.
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public TimeSeriesDatum convert(InputStream source) {
    // ILP doesn't easily support a single time series.
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public int serializationSize(TimeSeriesDatum datum) {
    int length = 1 + 1 + 1 + 1 + 20; //comma, space, comma, space and timestamp
    TimeSeriesDatumStringId id = (TimeSeriesDatumStringId) datum.id();
    // TODO - escape!
    if (id.metric().indexOf('.') < 0) {
      // need at least one dot
      return -1;
    }
    length += id.metric().length() - 1; // for tossing the dot
    for (Entry<String, String> entry : id.tags().entrySet()) {
      // TODO - escape!
      length += StringUtils.stringToUTF8BytesLength(entry.getKey())
              + 1
              + StringUtils.stringToUTF8BytesLength(entry.getValue());
    }
    // TODO - this is soooooo bad! Don't cast/waste strings
    if (datum.value().value().type() == NumericType.TYPE) {
      final NumericType value = (NumericType) datum.value().value();
      if (value.isInteger()) {
        length += Long.toString(value.longValue()).length();
      } else {
        length += Double.toString(value.doubleValue()).length();
      }
    } else {
      // not handling other types yet.
      return -1;
    }

    return length;
  }

  @Override
  public int serialize(TimeSeriesDatum datum, byte[] buffer, int offset) {
    // TODO figure out the dot
    int start = offset;
    TimeSeriesDatumStringId id = (TimeSeriesDatumStringId) datum.id();
    // TODO - super inefficient.
    // TODO - escape!
    byte[] metric = id.metric().getBytes(StandardCharsets.UTF_8);
    int dot = -1;
    for (int i = metric.length - 1; i >= 0; i--) {
      if (metric[i] == '.') {
        dot = i;
        break;
      }
    }
    if (dot < 0) {
      // need at least one dot
      throw new IllegalArgumentException("The metric must have at least one " +
              "dot: " + id.metric());
    }

    // measurement
    System.arraycopy(metric, 0, buffer, offset, dot);
    offset += dot;

    // tags
    // TODO - escape!
    for (Entry<String, String> entry : id.tags().entrySet()) {
      buffer[offset++] = ',';
      int len = StringUtils.stringToUTF8BytesLength(entry.getKey());
      StringUtils.stringToUTF8Bytes(entry.getKey(), buffer, offset);
      offset += len;

      buffer[offset++] = '=';
      len = StringUtils.stringToUTF8BytesLength(entry.getValue());
      StringUtils.stringToUTF8Bytes(entry.getValue(), buffer, offset);
      offset += len;
    }

    buffer[offset++] = ' ';

    // field
    int len = metric.length - (dot + 1);
    System.arraycopy(metric, dot + 1, buffer, offset, len);
    offset += len;
    buffer[offset++] = '=';

    // value
    if (datum.value().value().type() == NumericType.TYPE) {
      final NumericType value = (NumericType) datum.value().value();
      // TODO - eww eww eww!!!
      byte[] val;
      if (value.isInteger()) {
        val = Long.toString(value.longValue()).getBytes(StandardCharsets.UTF_8);
      } else {
        val = Double.toString(value.doubleValue()).getBytes(StandardCharsets.UTF_8);
      }
      System.arraycopy(val, 0, buffer, offset, val.length);
      offset += val.length;
    } else {
      // not handling other types yet.
      throw new IllegalArgumentException("Unsupported type: " + datum.value().type());
    }

    buffer[offset++] = ' ';
    // TODO - eww eww ewww!!!
    byte[] ts = Long.toString(datum.value().timestamp().epoch()).getBytes(StandardCharsets.UTF_8);
    System.arraycopy(ts, 0, buffer, offset, ts.length);
    offset += ts.length;

    ts = Long.toString(datum.value().timestamp().nanos()).getBytes(StandardCharsets.UTF_8);
    // need 9 chars of nanoseconds. So we may need to pad with zeros.
    for (int i = 0; i < (9 - ts.length); i++) {
      buffer[offset++] = '0';
    }
    System.arraycopy(ts, 0, buffer, offset, ts.length);
    offset += ts.length;
    return offset - start;
  }

  @Override
  public int serialize(TimeSeriesDatum datum, OutputStream stream) {
    int written = 0;
    try {
      // TODO figure out the dot
      TimeSeriesDatumStringId id = (TimeSeriesDatumStringId) datum.id();
      // TODO - super inefficient.
      byte[] metric = id.metric().getBytes(StandardCharsets.UTF_8);
      int dot = -1;
      for (int i = metric.length - 1; i >= 0; i--) {
        if (metric[i] == '.') {
          dot = i;
          break;
        }
      }
      if (dot < 0) {
        // need at least one dot
        throw new IllegalArgumentException("The metric must have at least one " +
                "dot: " + id.metric());
      }

      // measurement
      stream.write(metric, 0, dot);
      written += dot;

      // tags
      // TODO - escape!
      for (Entry<String, String> entry : id.tags().entrySet()) {
        stream.write(',');
        int len = StringUtils.stringToUTF8BytesLength(entry.getKey());
        StringUtils.stringToUTF8Bytes(entry.getKey(), stream);
        written += len + 1;

        stream.write('=');
        len = StringUtils.stringToUTF8BytesLength(entry.getValue());
        StringUtils.stringToUTF8Bytes(entry.getValue(), stream);
        written += len + 1;
      }

      stream.write(' ');
      written++;

      // field
      int len = metric.length - (dot + 1);
      stream.write(metric, dot + 1, len);
      stream.write('=');
      written += len + 1;

      // value
      if (datum.value().value().type() == NumericType.TYPE) {
        final NumericType value = (NumericType) datum.value().value();
        // TODO - eww eww eww!!!
        byte[] val;
        if (value.isInteger()) {
          val = Long.toString(value.longValue()).getBytes(StandardCharsets.UTF_8);
        } else {
          val = Double.toString(value.doubleValue()).getBytes(StandardCharsets.UTF_8);
        }
        stream.write(val);
        written += val.length;
      } else {
        // not handling other types yet.
        throw new IllegalArgumentException("Unsupported type: " + datum.value().type());
      }

      stream.write(' ');
      // TODO - eww eww ewww!!!
      byte[] ts = Long.toString(datum.value().timestamp().epoch()).getBytes(StandardCharsets.UTF_8);
      stream.write(ts);
      written += ts.length + 1;

      ts = Long.toString(datum.value().timestamp().nanos()).getBytes(StandardCharsets.UTF_8);
      // need 9 chars of nanoseconds. So we may need to pad with zeros.
      for (int i = 0; i < (9 - ts.length); i++) {
        stream.write('0');
        written++;
      }
      stream.write(ts);
      written += ts.length;
      return written;
    } catch (IOException e) {
      throw new IllegalStateException("IOException on writing to the stream", e);
    }
  }

  @Override
  public TimeSeriesSharedTagsAndTimeData convertShared(String source) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public TimeSeriesSharedTagsAndTimeData convertShared(byte[] source) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public TimeSeriesSharedTagsAndTimeData convertShared(byte[] source, int offset, int length) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public TimeSeriesSharedTagsAndTimeData convertShared(InputStream source) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public int serializationSize(TimeSeriesSharedTagsAndTimeData data) {
    int baseLineLength = 1 + 1 + 1 + 1 + 20; //comma, space, comma, space and timestamp

    // tags first
    int tagsLength = 0;
    for (Entry<String, String> entry : data.tags().entrySet()) {
      // TODO - escape!
      tagsLength += StringUtils.stringToUTF8BytesLength(entry.getKey())
              + 1
              + StringUtils.stringToUTF8BytesLength(entry.getValue());
    }

    // measurements
    // TODO - There is the possibility that if there are multiple measurements
    // in the same payload that we can return multiple fields for the same
    // measurement, e.g. sys.cpu user, sys, busy, etc. For now, to avoid
    // intermediate structures, we're just going to over-estimate.
    int length = 0;
    for (Entry<String, TimeSeriesDataType> entry : data.data().entries()) {
      if (length > 0) {
        // newline
        length++;
      }
      // TODO - escape!
      String metric = entry.getKey();
      if (metric.indexOf('.') < 0) {
        return -1;
      }
      length += baseLineLength + tagsLength;
      length += metric.length() - 1; // for tossing the dot.

      TimeSeriesDataType value = entry.getValue();
      if (value.type() == NumericType.TYPE) {
        final NumericType v = (NumericType) value;
        if (v.isInteger()) {
          length += Long.toString(v.longValue()).length();
        } else {
          length += Double.toString(v.doubleValue()).length();
        }
      } else {
        // TODO
        return -1;
      }
    }
    return length;
  }

  @Override
  public int serialize(TimeSeriesSharedTagsAndTimeData data, byte[] buffer, int offset) {
    ILPLines lines = aggregate(data);
    if (lines == null) {
      return 0;
    }

    // now we have a bunch of lines. We need to iterate and copy into the
    // buffer, adding a space and the timestamp and newline where appropriate.
    int start = offset;
    for (int i = 0; i < lines.validLines; i++) {
      System.arraycopy(lines.lineBuffers[i], 0, buffer, offset,
              lines.indices[i]);
      offset += lines.indices[i];
      buffer[offset++] = ' ';

      byte[] ts = Long.toString(data.timestamp().epoch()).getBytes(StandardCharsets.UTF_8);
      System.arraycopy(ts, 0, buffer, offset, ts.length);
      offset += ts.length;

      ts = Long.toString(data.timestamp().nanos()).getBytes(StandardCharsets.UTF_8);
      // need 9 chars of nanoseconds. So we may need to pad with zeros.
      for (int x = 0; x < (9 - ts.length); x++) {
        buffer[offset++] = '0';
      }
      System.arraycopy(ts, 0, buffer, offset, ts.length);
      offset += ts.length;

      if (i + 1 < lines.validLines) {
        buffer[offset++] = '\n';
      }
    }
    return offset - start;
  }

  @Override
  public int serialize(TimeSeriesSharedTagsAndTimeData data, OutputStream stream) {
    ILPLines lines = aggregate(data);
    if (lines == null) {
      return 0;
    }

    try {
      // now we have a bunch of lines. We need to iterate and copy into the
      // buffer, adding a space and the timestamp and newline where appropriate.
      int len = 0;
      for (int i = 0; i < lines.validLines; i++) {
        stream.write(lines.lineBuffers[i], 0, lines.indices[i]);
        stream.write(' ');
        len += lines.indices[i] + 1;

        byte[] ts = Long.toString(data.timestamp().epoch()).getBytes(StandardCharsets.UTF_8);
        stream.write(ts);
        len += ts.length;

        ts = Long.toString(data.timestamp().nanos()).getBytes(StandardCharsets.UTF_8);
        // need 9 chars of nanoseconds. So we may need to pad with zeros.
        for (int x = 0; x < (9 - ts.length); x++) {
          stream.write('0');
          len++;
        }
        stream.write(ts);
        len += ts.length;

        if (i + 1 < lines.validLines) {
          stream.write('\n');
          len++;
        }
      }
      return len;
    } catch (IOException e) {
      throw new IllegalStateException("IOException on writing to the stream", e);
    }
  }

  @Override
  public LowLevelTimeSeriesData convertLowLevelData(byte[] source) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public LowLevelTimeSeriesData convertLowLevelData(byte[] source, int offset, int length) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public LowLevelTimeSeriesData convertLowLevelData(InputStream source) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public int serializationSize(LowLevelTimeSeriesData data) {
    int baseLineLength = 1 + 1 + 1 + 1 + 20; //comma, space, comma, space and timestamp

    // will throw if it's not the proper type.
    LowLevelMetricData metricData = (LowLevelMetricData) data;

    // again we're going to be pessimistic and assume that every time series is
    // completely unique.
    int length = 0;
    while (data.advance()) {
      length += baseLineLength;
      if (metricData.metricFormat() == StringFormat.ENCODED) {
        return -1;
      }
      if (metricData.tagsFormat() == StringFormat.ENCODED) {
        return -1;
      }

      length += metricData.metricLength();
      length += metricData.tagBufferLength();
      length += metricData.tagSetCount() * 2; // = and ,
      if (metricData.valueFormat() == ValueFormat.INTEGER) {
        length += Long.toString(metricData.longValue()).length();
      } else {
        length += Double.toString(metricData.doubleValue()).length();
      }
    }
    return length;
  }

  @Override
  public int serialize(LowLevelTimeSeriesData data, byte[] buffer, int offset) {
    final LowLevelMetricData metricData = (LowLevelMetricData) data;
    ILPLines lines = aggregate(metricData);
    if (lines == null) {
      return 0;
    }

    // now we have a bunch of lines. We need to iterate and copy into the
    // buffer, adding a space and the timestamp and newline where appropriate.
    int start = offset;
    for (int i = 0; i < lines.validLines; i++) {
      System.arraycopy(lines.lineBuffers[i], 0, buffer, offset,
              lines.indices[i]);
      offset += lines.indices[i];
      buffer[offset++] = ' ';

      byte[] ts = Long.toString(data.commonTimestamp() ?
              data.timestamp().epoch() :
              lines.timestampEpochs[i]).getBytes(StandardCharsets.UTF_8);
      System.arraycopy(ts, 0, buffer, offset, ts.length);
      offset += ts.length;

      ts = Long.toString(data.commonTimestamp() ?
              data.timestamp().nanos() :
              lines.timestampNanos[i]).getBytes(StandardCharsets.UTF_8);
      // need 9 chars of nanoseconds. So we may need to pad with zeros.
      for (int x = 0; x < (9 - ts.length); x++) {
        buffer[offset++] = '0';
      }
      System.arraycopy(ts, 0, buffer, offset, ts.length);
      offset += ts.length;

      if (i + 1 < lines.validLines) {
        buffer[offset++] = '\n';
      }
    }
    return offset - start;
  }

  @Override
  public int serialize(LowLevelTimeSeriesData data, OutputStream stream) {
    final LowLevelMetricData metricData = (LowLevelMetricData) data;
    ILPLines lines = aggregate(metricData);
    if (lines == null) {
      return 0;
    }

    try {
      // now we have a bunch of lines. We need to iterate and copy into the
      // buffer, adding a space and the timestamp and newline where appropriate.
      int len = 0;
      for (int i = 0; i < lines.validLines; i++) {
        stream.write(lines.lineBuffers[i], 0, lines.indices[i]);
        stream.write(' ');
        len += lines.indices[i] + 1;

        byte[] ts = Long.toString(data.commonTimestamp() ?
                data.timestamp().epoch() :
                lines.timestampEpochs[i]).getBytes(StandardCharsets.UTF_8);
        stream.write(ts);
        len += ts.length;

        ts = Long.toString(data.commonTimestamp() ?
                data.timestamp().nanos() :
                lines.timestampNanos[i]).getBytes(StandardCharsets.UTF_8);
        // need 9 chars of nanoseconds. So we may need to pad with zeros.
        for (int x = 0; x < (9 - ts.length); x++) {
          stream.write('0');
          len++;
        }
        stream.write(ts);
        len += ts.length;

        if (i + 1 < lines.validLines) {
          stream.write('\n');
          len++;
        }
      }
      return len;
    } catch (IOException e) {
      throw new IllegalStateException("IOException on writing to the stream", e);
    }
  }

  protected ILPLines aggregate(TimeSeriesSharedTagsAndTimeData data) {
    ILPLines lines = lineBuffers.get();
    lines.validLines = 0;
    for (Entry<String, TimeSeriesDataType> entry : data.data().entries()) {
      byte[] metric = entry.getKey().getBytes(StandardCharsets.UTF_8);
      int dot = -1;
      // TODO - escape!
      for (int i = metric.length - 1; i >= 0; i--) {
        if (metric[i] == '.') {
          dot = i;
          break;
        }
      }
      if (dot < 0) {
        // need at least one dot
        throw new IllegalArgumentException("The metric must have at least one " +
                "dot: " + entry.getKey());
      }

      long hash = XXHash.hash(metric, 0, dot);
      int lineIndex = -1;
      for (int i = 0; i < lines.validLines; i++) {
        if (lines.measureHashes[i] == hash) {
          lineIndex = i;
          break;
        }
      }

      if (lineIndex < 0) {
        // new line!
        lineIndex = lines.validLines++;
        if (lineIndex > lines.lineBuffers.length) {
          lines.resize();
        }
        lines.indices[lineIndex] = 0;
        lines.measureHashes[lineIndex] = hash;

        // serialize measurement and tags
        lines.write(lineIndex, metric, 0, dot);

        // tags
        // TODO - escape!
        for (Entry<String, String> tagPair : data.tags().entrySet()) {
          lines.write(lineIndex, (byte) ',');
          int len = StringUtils.stringToUTF8BytesLength(tagPair.getKey());
          lines.require(lineIndex, len);
          StringUtils.stringToUTF8Bytes(tagPair.getKey(),
                  lines.lineBuffers[lineIndex],
                  lines.indices[lineIndex]);
          lines.indices[lineIndex] += len;

          lines.write(lineIndex, (byte) '=');
          len = StringUtils.stringToUTF8BytesLength(tagPair.getValue());
          lines.require(lineIndex, len);
          StringUtils.stringToUTF8Bytes(tagPair.getValue(),
                  lines.lineBuffers[lineIndex],
                  lines.indices[lineIndex]);
          lines.indices[lineIndex] += len;
        }

        lines.write(lineIndex, (byte) ' ');
      }

      // field!
      if (lines.lineBuffers[lineIndex][lines.indices[lineIndex] - 1] != ' ') {
        lines.write(lineIndex, (byte) ',');
      }
      int len = metric.length - (dot + 1);
      lines.write(lineIndex, metric, dot + 1, len);
      lines.write(lineIndex, (byte) '=');

      TimeSeriesDataType value = entry.getValue();
      if (value.type() == NumericType.TYPE) {
        final NumericType v = (NumericType) value;
        // TODO - ew eww ewwww!
        byte[] val;
        if (v.isInteger()) {
          val = Long.toString(v.longValue()).getBytes(StandardCharsets.UTF_8);
        } else {
          val = Double.toString(v.doubleValue()).getBytes(StandardCharsets.UTF_8);
        }
        lines.write(lineIndex, val, 0, val.length);
      } else {
        // TODO
        return null;
      }
    }
    return lines;
  }

  protected ILPLines aggregate(LowLevelMetricData metricData) {
    ILPLines lines = lineBuffers.get();
    lines.validLines = 0;
    while (metricData.advance()) {
      int dot = -1;
      for (int i = metricData.metricLength() - 1; i >= 0; i--) {
        if (metricData.metricBuffer()[i] == '.') {
          dot = i;
          break;
        }
      }
      if (dot < 0) {
        // need at least one dot
        throw new IllegalArgumentException("The metric must have at least one " +
                "dot.");
      }

      long hash = XXHash.hash(metricData.metricBuffer(), metricData.metricStart(),
              metricData.metricStart() + dot);
      int lineIndex = -1;
      long tagHash = 0;
      if (!metricData.commonTags()) {
        if (metricData instanceof HashedLowLevelMetricData) {
          tagHash = ((HashedLowLevelMetricData) metricData).tagsSetHash();
        } else {
          tagHash = XXHash.hash(metricData.tagsBuffer(),
                  metricData.tagBufferStart(),
                  metricData.tagBufferLength());
        }
      }

      for (int i = 0; i < lines.validLines; i++) {
        if (lines.measureHashes[i] == hash) {
          // check tags and timestamps if we need to
          if (metricData.commonTimestamp() && metricData.commonTags()) {
            lineIndex = i;
            break;
          }

          boolean matched = false;
          if (!metricData.commonTags()) {
            if (lines.tagHashes[i] == tagHash) {
              matched = true;
            }
          } else {
            matched = true;
          }

          if (!matched) {
            continue;
          }
          if (metricData.commonTimestamp()) {
            // good!
            lineIndex = i;
            break;
          }

          // gotta check timestamps now...
          if (lines.timestampEpochs[i] == metricData.timestamp().epoch() &&
                  lines.timestampNanos[i] == metricData.timestamp().nanos()) {
            lineIndex = i;
            break;
          }
        }
      }

      if (lineIndex < 0) {
        // new line!
        lineIndex = lines.validLines++;
        if (lineIndex > lines.lineBuffers.length) {
          lines.resize();
        }
        lines.indices[lineIndex] = 0;
        lines.measureHashes[lineIndex] = hash;
        if (!metricData.commonTags()) {
          lines.tagHashes[lineIndex] = tagHash;
        }
        if (!metricData.commonTimestamp()) {
          lines.timestampEpochs[lineIndex] = metricData.timestamp().epoch();
          lines.timestampNanos[lineIndex] = metricData.timestamp().nanos();
        }

        // serialize measurement and tags
        lines.write(lineIndex, metricData.metricBuffer(), metricData.metricStart(),
                metricData.metricStart() + dot);

        // tags
        while (metricData.advanceTagPair()) {
          lines.write(lineIndex, (byte) ',');
          lines.require(lineIndex, metricData.tagKeyLength());
          lines.write(lineIndex, metricData.tagsBuffer(), metricData.tagKeyStart(),
                  metricData.tagKeyLength());

          lines.write(lineIndex, (byte) '=');
          lines.require(lineIndex, metricData.tagValueLength());
          lines.write(lineIndex, metricData.tagsBuffer(), metricData.tagValueStart(),
                  metricData.tagValueLength());
        }

        lines.write(lineIndex, (byte) ' ');
      }

      // field!
      if (lines.lineBuffers[lineIndex][lines.indices[lineIndex] - 1] != ' ') {
        lines.write(lineIndex, (byte) ',');
      }
      int len = metricData.metricLength() - (dot + 1);
      lines.write(lineIndex, metricData.metricBuffer(),
              metricData.metricStart() + dot + 1, len);
      lines.write(lineIndex, (byte) '=');

      // TODO - ew eww ewwww!
      byte[] val;
      if (metricData.valueFormat() == ValueFormat.INTEGER) {
        val = Long.toString(metricData.longValue()).getBytes(StandardCharsets.UTF_8);
      } else {
        val = Double.toString(metricData.doubleValue()).getBytes(StandardCharsets.UTF_8);
      }
      lines.write(lineIndex, val, 0, val.length);
    }
    return lines;
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public TimeSeriesDataConverter newInstance() {
    return this;
  }

  /**
   * Helper that contains thread-local buffers we'll use to aggregate fields
   * for the same measurement to avoid spamming the destination with lots of
   * single line values.
   */
  private class ILPLines {
    byte[][] lineBuffers;
    int[] indices;
    long[] measureHashes;
    long[] tagHashes;
    long[] timestampEpochs;
    long[] timestampNanos;
    int validLines;

    ILPLines() {
      lineBuffers = new byte[8][];
      for (int i = 0; i < 8; i++) {
        lineBuffers[i] = new byte[1024];
      }
      indices = new int[8];
      measureHashes = new long[8];
      tagHashes = new long[8];
      timestampEpochs = new long[8];
      timestampNanos = new long[8];
    }

    void resize() {
      byte[][] tempLines = new byte[lineBuffers.length + 8][];
      System.arraycopy(lineBuffers, 0, tempLines, 0, validLines);
      for (int i = validLines; i < tempLines.length; i++) {
        tempLines[i] = new byte[1024];
      }
      lineBuffers = tempLines;

      indices = Arrays.copyOf(indices, indices.length + 8);
      measureHashes = Arrays.copyOf(measureHashes, measureHashes.length + 8);
      tagHashes = Arrays.copyOf(tagHashes, tagHashes.length + 8);
      timestampEpochs = Arrays.copyOf(timestampEpochs, timestampEpochs.length + 8);
      timestampNanos = Arrays.copyOf(timestampNanos, timestampEpochs.length + 8);
    }

    void require(int index, int length) {
      byte[] buf = lineBuffers[index];
      int curLength = indices[index];
      if (buf.length <= curLength + length) {
        lineBuffers[index] = Arrays.copyOf(buf, buf.length +
                Math.min(length, 1024));
      }
    }

    void write(int index, byte[] buffer, int offset, int length) {
      require(index, length);
      byte[] buf = lineBuffers[index];
      System.arraycopy(buffer, offset, buf, indices[index], length);
      indices[index] += length;
    }

    void write(int index, byte c) {
      require(index, 1);
      byte[] buf = lineBuffers[index];
      buf[indices[index]++] = c;
    }
  }
}
