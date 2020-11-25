// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.data.influx;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import net.opentsdb.common.Const;
import net.opentsdb.data.LowLevelMetricData.HashedLowLevelMetricData;
import net.opentsdb.data.LowLevelTimeSeriesData.HashedNamespacedLowLevelTimeSeriesData;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Parsing;
import net.opentsdb.utils.XXHash;

/**
 * A parser for the Influx line protocol. This parser will take one or more
 * lines of Influx style data, say from Telegraf, and spits out some data if 
 * everything is good. We're pretty much following the spec from:
 * https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
 * <p>
 * Fields are collapsed into the measurement to form a metric. E.g. 
 * "sys.if,tagKey=Value,tagk2=Value2 in=10.24,out=42 1465839830100400200"
 * will return a metric "sys.if.in" and another "sys.if.out" sharing the same
 * tag set and timestamp. In this case {@link #advance()} would be called twice.
 * <p>
 * Escaping of strings is implemented as per the spec with exception that if a 
 * measurement, tag key, tag value or field starts and ends with a quote, the 
 * quotes will be stripped. Escaping slashes are also eliminated.
 * <p>
 * String field values are passed over at this time.
 * TODO - we can turn those into annotations eventually.
 * <p>
 * Comments are also skipped as per the spec. We'll also do our best to skip 
 * over unnecessary white space and we allow the protocol components to be
 * separated by tabs instead of single spaces.
 * <p>
 * Malformed lines that don't pass will be skipped over. We'll set 
 * {@link #hasParsingError()} to true if one or more lines _before_ a successful
 * line had a problem when {@link #advance()} is called.
 * <p>
 * If no timestamp is present we use the current millis. Nano's don't make sense
 * for the servers this will run on and the amount of data coming in.
 * 
 * TODO - set some parsing errors.
 * 
 * TODO - handle duplicate tags.
 * 
 * TODO - sort tags.
 * 
 * @since 3.0
 */
public class InfluxLineProtocolParser implements HashedLowLevelMetricData, 
      HashedNamespacedLowLevelTimeSeriesData,
      CloseablePooledObject {
  private static final Logger LOG = LoggerFactory.getLogger(InfluxLineProtocolParser.class);
  public static int ESCAPE = 0x80000000;
  public static int UNESCAPE = 0x7FFFFFFF;
  public static int DEFAULT_STREAM_READ_AMOUNT = 4096;

  /** The pooled object ref. */
  protected PooledObject pooled_object;
  
  /** When something goes pear shaped. */
  protected boolean has_parsing_error;
  protected String parsing_exception;
  
  /** Comes in as a string so we'll just toBytes() here. */
  protected byte[] namespace;
  protected long namespace_hash;
  
  /** The raw buffer from the caller OR if the stream is used, where we'll
   * be dumping our chunks. */
  protected byte[] buffer;
  
  /** An offset where data starts into this buffer. Will almost always be 0. */
  protected int offset;
  
  /** The end of data in this buffer. */
  protected int end;
  
  /** The stream ref and end-of-stream flag. */
  protected InputStream stream;
  protected boolean eos;
  
  /** The line start and end when we've found a line in the payload. */
  protected int line_start;
  protected int line_end;
  
  protected int read_index;
  
  /** Since there can be multiple "fields" for the "measurement", we need to 
   * smoosh them into a buffer and append a dot, e.g. "measurement"."field". 
   */
  protected byte[] metric_buffer;
  protected int measurement_index;
  protected int metric_bytes;
  
  /** The field and value indices within the main buffer. There will be two 
   * values per field and value, the first being the start index of the field
   * or value, the second being the ending offset. Note the field start offset
   * may have it's first bit set if the field name is escaped.
   */
  protected int[] field_indices;
  protected int field_index;
  protected int[] value_indices;
  protected int value_index;
  
  /** Where copy the tag strings and sort them for a consistent hash. */
  protected byte[] tag_buffer;
  protected int tags_length;
  protected int tags_count;
  protected int tag_index;
  protected int tag_key_start;
  protected int tag_key_length;
  protected int tag_value_start;
  protected int tag_value_length;
  
  /** The timestamp that we'll update each time. */
  protected ZonedNanoTimeStamp timestamp;
  
  /** The values. We have them as arrays because our Parsing.parse<num>() 
   * methods will return true or false instead of throwing an exception and 
   * therefore we can't return the value. So we pass the array in and that
   * lets us return the result from parsing without creating a tuple or making
   * more garbage. 
   */
  protected long[] long_value;
  protected double[] double_value;
  protected long[] temp_long;
  protected double[] temp_double;
  protected ValueFormat value_format;
  
  /** Hashes when enabled. */
  protected boolean hash_it;
  protected long series_hash;
  protected long metric_hash;
  protected long tag_set_hash;
  protected long[] tag_key_hashes;
  protected long tag_key_hash;
  protected long tag_value_hash;
  protected long[] tag_value_hashes;
  protected long tag_pair_hash;
  
  /** Default ctor inits the buffers, etc. */
  public InfluxLineProtocolParser() {
    metric_buffer = new byte[1024];
    field_indices = new int[16];
    value_indices = new int[16];
    tag_buffer = new byte[26];
    timestamp = new ZonedNanoTimeStamp(0, Const.UTC);
    long_value = new long[1];
    double_value = new double[1];
    temp_long = new long[1];
    temp_double = new double[1];
    tag_key_hashes = new long[16];
    tag_value_hashes = new long[16];
  }
  
  /**
   * Sets the internal reference to the given buffer.
   * @param buffer The non-null and non-empty buffer to parse.
   */
  public void setBuffer(final byte[] buffer) {
    this.buffer = buffer;
    offset = 0;
    end = buffer.length;
    line_start = offset;
    line_end = line_start;
    namespace = null;
  }
  
  /**
   * Sets the internal reference to the given buffer.
   * @param buffer The non-null and non-empty buffer to parse.
   * @param offset An offset into the original buffer to begin parsing at.
   * @param length The length of the data to parse in the buffer.
   */
  public void setBuffer(byte[] buffer, int offset, int length) {
    this.buffer = buffer;
    this.offset = offset;
    end = offset + length;
    line_start = offset;
    line_end = line_start;
    namespace = null;
  }
  
  /**
   * Sets a reference to the stream for parsing.
   * @param stream The non-null and open stream to process.
   */
  public void setInputStream(final InputStream stream) {
    if (buffer == null) {
      buffer = new byte[DEFAULT_STREAM_READ_AMOUNT * 2];
    }
    this.stream = stream;
    eos = false;
    offset = 0;
    end = 0;
    line_start = 0;
    line_end = line_start;
    namespace = null;
  }
  
  /**
   * For async cases will re-use the buffer if we're part of the pool.
   * @param stream
   * @throws IOException 
   */
  public void fillBufferFromStream(final InputStream stream) throws IOException {
    if (buffer == null) {
      buffer = new byte[DEFAULT_STREAM_READ_AMOUNT];
    }
    int idx = 0;
    while (true) {
      if (idx + DEFAULT_STREAM_READ_AMOUNT >= buffer.length) {
        byte[] temp = new byte[buffer.length * 2];
        System.arraycopy(buffer, 0, temp, 0, idx);
        buffer = temp;
      }
      int read = stream.read(buffer, idx, DEFAULT_STREAM_READ_AMOUNT);
      if (read < 0) {
        break;
      }
      idx += read;
    }
    offset = 0;
    end = offset + idx;
    line_start = offset;
    line_end = line_start;
    namespace = null;
  }
  
  /**
   * Sets the namespace for this payload.
   * @param namespace The namespace to store. If null or empty, treats the 
   * namespace as null.
   */
  public void setNamespace(final String namespace) {
    if (Strings.isNullOrEmpty(namespace)) {
      this.namespace = null;
      namespace_hash = 0;
      return;
    }
    this.namespace = namespace.getBytes(Const.UTF8_CHARSET);
    namespace_hash = XXHash.hash(this.namespace);
  }

  /**
   * Whether or not to compute the hashes.
   * @param compute_hashes Set to true if we should compute as we parse, false
   * if not.
   */
  public void computeHashes(final boolean compute_hashes) {
    hash_it = compute_hashes;
  }
  
  @Override
  public void close() {
    release();
  }
  
  @Override
  public void release() {
    namespace = null;
    if (stream != null) {
      // stream mode so we close the stream, null it and leave the buffer as
      // we're likely going to be consuming another stream.
      // TODO - double check we haven't grown the buffer too much if we're pooled.
      try {
        stream.close();
      } catch (IOException e) {
        LOG.warn("Failed to close the stream for the influx parser", e);
      }
      stream = null;
    } else if (pooled_object == null) {
      // byte buffer mode so release the ref so it can be consumed.
      buffer = null;
    } // but if it IS part of a pool, leave it
      // TODO downsize if it grew too big. 
    if (pooled_object != null) {
      pooled_object.release();
    }
  }
  
  @Override
  public void setPooledObject(final PooledObject pooled_object) {
    this.pooled_object = pooled_object;
  }
  
  @Override
  public Object object() {
    return this;
  }

  @Override
  public boolean advance() {
    // first see if we have more fields to read for the current line.
    if (read_index < field_index) {
      int start = field_indices[read_index * 2];
      int end = field_indices[(read_index * 2) + 1];
      appendField(start, end);
      
      start = value_indices[read_index * 2];
      end = value_indices[(read_index * 2) + 1];
      parseValue(start, end, true);
      read_index++;
      tag_index = 0;
      return true;
    }
    
    // reset
    read_index = 1;
    tags_length = 0;
    line_start = line_end > 0 ? line_end + 1 : 0;
    
    if (stream != null) {
      return advanceStream();
    }
    return advanceBytes();
  }

  @Override
  public boolean hasParsingError() {
    return has_parsing_error;
  }
  
  @Override
  public String parsingError() {
    return parsing_exception;
  }
  
  @Override
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public byte[] tagsBuffer() {
    return tag_buffer;
  }

  @Override
  public StringFormat tagsFormat() {
    return StringFormat.UTF8_STRING;
  }

  @Override
  public byte tagDelimiter() {
    return 0;
  }

  @Override
  public int tagSetCount() {
    return tags_count;
  }
  
  @Override
  public boolean advanceTagPair() {
    if (tag_index >= tags_length) {
      return false;
    }
    
    int start = tag_index;
    while (true) {
      if (tag_buffer[tag_index] == 0) {
        break;
      }
      tag_index++;
    }
    
    if (hash_it) {
      tag_key_hash = XXHash.hash(tag_buffer, start, tag_key_length);
    }
    tag_key_start = start;
    tag_key_length = tag_index - tag_key_start;
    
    int pairHashStart = start;
    tag_index++;
    start = tag_index;
    while (tag_index < tag_index + tags_length) {
      if (tag_buffer[tag_index] == 0) {
        break;
      }
      tag_index++;
    }
    
    if (hash_it) {
      tag_value_hash = XXHash.hash(tag_buffer, start, tag_index - start);
      tag_pair_hash = XXHash.hash(tag_buffer, pairHashStart, tag_index - pairHashStart);
    }
    
    tag_value_start = start;
    tag_value_length = tag_index - start;
    tag_index++;
    return true;
  }

  @Override
  public int tagKeyStart() {
    return tag_key_start;
  }

  @Override
  public int tagKeyLength() {
    return tag_key_length;
  }

  @Override
  public int tagValueStart() {
    return tag_value_start;
  }

  @Override
  public int tagValueLength() {
    return tag_value_length;
  }

  @Override
  public int tagBufferStart() {
    return 0;
  }

  @Override
  public int tagBufferLength() {
    return tags_length - 1;
  }

  @Override
  public long metricHash() {
    return metric_hash;
  }

  @Override
  public long timeSeriesHash() {
    return series_hash;
  }

  @Override
  public long tagsSetHash() {
    return tag_set_hash;
  }

  @Override
  public long tagPairHash() {
    return tag_pair_hash;
  }

  @Override
  public long tagKeyHash() {
    return tag_key_hash;
  }

  @Override
  public long tagValueHash() {
    return tag_value_hash;
  }

  public byte[] namespaceBuffer() {
    return namespace;
  }
  
  public int namespaceStart() {
    return 0;
  }
  
  public int namespaceLength() {
    return namespace != null ? namespace.length : 0;
  }

  @Override
  public StringFormat namespaceFormat() {
    return StringFormat.UTF8_STRING;
  }
  
  @Override
  public long namespaceHash() {
    return namespace_hash;
  }

  @Override
  public StringFormat metricFormat() {
    return StringFormat.UTF8_STRING;
  }

  @Override
  public int metricStart() {
    return 0;
  }

  @Override
  public int metricLength() {
    return metric_bytes;
  }

  @Override
  public byte[] metricBuffer() {
    return metric_buffer;
  }

  @Override
  public ValueFormat valueFormat() {
    return value_format;
  }

  @Override
  public long longValue() {
    return long_value[0];
  }

  @Override
  public float floatValue() {
    return (float) double_value[0];
  }

  @Override
  public double doubleValue() {
    return double_value[0];
  }
  
  /**
   * Hunts through the buffer for the next new line character.
   * @param start The offset to start from.
   * @return The index of the next new line char in the buffer, end if the end
   * of the buffer was reached or -1 if the stream is set, we're not at the end
   * of stream and no new line was found (meaning read more data).
   */
  private int findNextNewLine(final int start) {
    int printableChars = 0;
    for (int i = start + 1; i < end; i++) {
      if (buffer[i] == '\n') {
        if (printableChars >= 5) {
          // we had what could possibly be valid data.
          return i;
        }
        printableChars = 0;
      } else if (!Character.isISOControl(buffer[i]) && buffer[i] != ' ') {
        printableChars++;
      }
    }
    if (stream != null && !eos) {
      return -1;
    }
    return end;
  }
  
  /**
   * Iterates until we find the next non-space character.
   * @param i The index to start from.
   * @return The index of the first non-space character or the end of the buffer.
   */
  private int findNextChar(int i) {
    for (; i < end; i++) {
      if (!Character.isISOControl((char) buffer[i]) && !(buffer[i] == ' ')) {
        return i;
      }
    }
    return end;
  }
  
  /**
   * Runs through the line and parses the fields making sure we have valid data.
   * Called by {@link #advance()}, the proper current fieds will be set when a
   * valid line is found.
   * @return True if the line was valid and had useable data, false if not.
   */
  private boolean processLine() {
    field_index = value_index = tags_length = tag_index = tags_count = 0;
    boolean has_quote = false;
    boolean escaped_char = false;
    int idx = line_start;
    if (buffer[idx] == '"' || buffer[idx] == '\'') {
      has_quote = true;
      idx++;
    }
    
    int i = line_start;
    for (; i < line_end; i++) {
      if (has_quote && buffer[i] == '"' || buffer[i] == '\'') {
        // not part of spec but since we're trimming quotes, we can accept escaped
        // quotes.
        if (i -1 >= offset) {
          if (buffer[i - 1] == '\\') {
            escaped_char = true;
            continue;
          }
        }
        break;
      } else if (!has_quote && (buffer[i] == ',' || buffer[i] == ' ' || 
            buffer[i] == '\t')) {
        if (i -1 >= offset && (buffer[i] == ',' || buffer[i] == ' ')) {
          if (buffer[i - 1] == '\\') {
            escaped_char = true;
            continue;
          }
        }
        break;
      }
    }
    
    if (metric_buffer.length < i - idx + 1) {
      metric_buffer = new byte[metric_buffer.length * 2];
    }
    
    if (escaped_char) {
      measurement_index = 0;
      for (int x = idx; x < i; x++) {
        if (buffer[x] == '\\' && (
            buffer[x + 1] == ' ' ||
            buffer[x + 1] == '"' ||
            buffer[x + 1] == ',' ||
            buffer[x + 1] == '\'')) {
          continue;
        }
        metric_buffer[measurement_index++] = buffer[x];
      }
      metric_buffer[measurement_index++] = '.';
      escaped_char = false;
    } else {
      System.arraycopy(buffer, idx, metric_buffer, 0, i - idx);
      metric_buffer[i - idx] = '.';
      measurement_index = i - idx + 1;
    }
    metric_bytes = measurement_index;
    idx = i;
    
    // now at tags, possibly
    if (has_quote) {
      idx++;
      has_quote = false;
    }
    
    if (idx >= line_end) {
      return false;
    }
    
    if (buffer[idx] == ',') {
      idx++;
      // ---------------------- TAGS ------------------
      // parse tags!
      int matched = 0;
      while (true) {
        if (idx >= line_end) {
          return false;
        }
        // TODO - sort!
        if (buffer[idx] == '"' || buffer[idx] == '\'') {
          has_quote = true;
          idx++;
        }
        
        i = idx;
        for (; i < line_end; i++) {
          if (has_quote && buffer[i] == '"' || buffer[i] == '\'') {
            // not part of spec but since we're trimming quotes, we can accept escaped
            // quotes.
            if (i -1 >= offset) {
              if (buffer[i - 1] == '\\') {
                escaped_char = true;
                continue;
              }
            }
            break;
          } else if (!has_quote && (buffer[i] == '=' || buffer[i] == ',' 
              || buffer[i] == ' ' || buffer[i] == '\t')) {
            if (i -1 >= offset && (buffer[i] == ',' || buffer[i] == ' ' 
                || buffer[i] == '=')) {
              if (buffer[i - 1] == '\\') {
                escaped_char = true;
                continue;
              }
            }
            break;
          }
        }
        
        // copy key or value
        if (tags_length + (i - idx) + 1 >= tag_buffer.length) {
          growTagBuffer();
        }
        
        if (i > idx) {
          int tag_start = tags_length;
          if (escaped_char) {
            for (int x = idx; x < i; x++) {
              if (buffer[x] == '\\' && (
                  buffer[x + 1] == '=' ||
                  buffer[x + 1] == ' ' ||
                  buffer[x + 1] == '"' ||
                  buffer[x + 1] == ',' ||
                  buffer[x + 1] == '\'')) {
                continue;
              }
              tag_buffer[tags_length++] = buffer[x];
            }
            escaped_char = false;
          } else {
            System.arraycopy(buffer, idx, tag_buffer, tags_length, i - idx);
            tags_length += (i - idx);
          }
          
          if (hash_it) {
            if (matched % 2 == 0) {
              tag_value_hashes[tags_count] = 
                  XXHash.hash(tag_buffer, tag_start, tags_length - 1 - tag_start);
            } else {
              tag_key_hashes[tags_count] = 
                  XXHash.hash(tag_buffer, tag_start, tags_length - 1 - tag_start);
            }
          }
          tag_buffer[tags_length++] = 0; // null it with the delimiter.
          idx = i;
          if (++matched % 2 == 0) {
            tags_count++;
          }
        }
        
        if (has_quote) {
          idx++;
          has_quote = false;
        }
        
        if (idx >= line_end) {
          return false;
        }
        if (buffer[idx] == '=' || buffer[idx] == ',') {
          idx++;
          continue;
        }
        
        if (buffer[idx] == ' ' || buffer[idx] == '\t') {
          // end of tags;
          break;
        }
      }
      
      if (matched % 2 != 0) {
        return false;
      }
      // done with tags!
      // TODO - sort !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      if (hash_it) {
        sortTagHashes(0, tags_count - 1);
        for (int x = 0; x < tags_count; x++) {
          if (x == 0) {
            tag_set_hash = tag_key_hashes[x];
          } else {
            tag_set_hash = XXHash.combineHashes(tag_set_hash, tag_key_hashes[x]);
          }
          tag_set_hash = XXHash.combineHashes(tag_set_hash, tag_value_hashes[x]);
        }
      }
    } else {
      tags_count = 0;
    }
    
    // skip to field(s)
    while (buffer[idx] == ' ' || buffer[idx] == '\t') {
      idx++;
    }
    escaped_char = false;
    
    // now at a field
    while (true) {
      // starting at fieldname
      if (buffer[idx] == '"' || buffer[idx] == '\'') {
        has_quote = true;
        idx++;
      }
      
      i = idx;
      for (; i < line_end; i++) {
        if (has_quote && buffer[i] == '"' || buffer[i] == '\'') {
          if (i -1 >= offset) {
            if (buffer[i - 1] == '\\') {
              escaped_char = true;
              continue;
            }
          }
          break;
        } else if (!has_quote && (buffer[i] == ',' || buffer[i] == ' ')) {
          if (i -1 >= offset && (buffer[i] == ',' || buffer[i] == ' ')) {
            if (buffer[i - 1] == '\\') {
              escaped_char = true;
              continue;
            }
          }
        } else if (!has_quote && buffer[i] == '=') {
          if (i -1 >= offset && buffer[i] == '=' && buffer[i - 1] == '\\') {
            escaped_char = true;
            continue;
          }
          break;
        }
      }
      
      int f = escaped_char ? (idx | ESCAPE) : idx;
      if ((field_index * 2) + 1 >= field_indices.length) {
        int[] temp = new int[field_indices.length * 2];
        System.arraycopy(field_indices, 0, temp, 0, (field_index * 2));
        field_indices = temp;
      }
      field_indices[field_index * 2] = f;
      field_indices[(field_index * 2) + 1] = i;
      field_index++;
      
      if (field_index == 1) {
        // we always copy the first field into the metric buffer.
        appendField(escaped_char ? idx | ESCAPE : idx, i);
      }
      escaped_char = false;
      idx = i;
      if (has_quote) {
        idx += 2;
        has_quote = false;
      } else {
        idx++;
      }
      
      if (idx >= line_end) {
        return false;
      }
      
      // value
      if (buffer[idx] == '"' || buffer[idx] == '\'') {
        // WARNING: We don't handle strings at this time so we skip it.
        has_quote = true;
        idx++;
      }
      
      i = idx;
      for (; i < line_end; i++) {
        if (buffer[i] == ',' || buffer[i] == ' ' || buffer[i] == '\t') {
          break;
        }
      }
      
      if (has_quote) {
        has_quote = false;
        field_index--;
        idx = i + 1;
      } else if (!parseValue(idx, i, value_index == 0)) {
        field_index--;
        idx = i;
      } else {
        if ((value_index * 2) + 1 >= value_indices.length) {
          int[] temp = new int[value_indices.length * 2];
          System.arraycopy(value_indices, 0, temp, 0, (value_index * 2));
          value_indices = temp;
        }
        value_indices[value_index * 2] = idx;
        value_indices[(value_index * 2) + 1] = i;
        value_index++;
        idx = i;
      }
      
      if (i >= line_end) {
        idx = line_end;
        break;
      }
      
      if (buffer[i] == ',') {
        idx++;
        continue;
      }
      idx++;
      break; // done with values
    }
    
    // consume empty space
    while (idx < line_end && (buffer[idx] == ' ' || buffer[idx] == '\t')) {
      idx++;
    }
    
    if (idx >= line_end) {
      // no timestamp!!! use current time and we'll use millis since nanos
      // are so imprecise here.
      timestamp.updateMsEpoch(DateTime.currentTimeMillis());
    } else {
      // now the timestamp if it's there.
      i = idx;
      for (; i < line_end; i++) {
        if (Character.isISOControl(buffer[i]) || buffer[i] == '\n' || 
            buffer[i] == ' ' || buffer[i] == '\t') {
          break;
        }
      }
      
      if (idx < i) {
        if (Parsing.parseLong(buffer, idx, i, temp_long)) {
          long seconds = temp_long[0] / 1000 / 1000 / 1000;
          long nanos = temp_long[0] - (seconds * 1000 * 1000 * 1000);
          timestamp.update(seconds, nanos);
        } else {
          // hmm, we couldn't parse the timestamp?
          return false;
        }
      }
      idx = i + 1;
    }
    
    return field_index > 0;
  }

  /**
   * Appends the current field to the metric buffer after the measurement and
   * period separator.
   * @param start The starting index of the field in the buffer.
   * @param end The end index of the field in the buffer.
   */
  private void appendField(final int start, final int end) {
    boolean escaped = (start & ESCAPE) != 0;

    int s = start & UNESCAPE;
    if (measurement_index + (end - s) >= metric_buffer.length) {
      byte[] temp = new byte[metric_buffer.length * 2];
      System.arraycopy(metric_buffer, 0, temp, 0, measurement_index);
      metric_buffer = temp;
    }
    
    if (escaped) {
      int idx = measurement_index;
      for (int x = s; x < end; x++) {
        if (buffer[x] == '\\' && (
            buffer[x + 1] == '=' ||
            buffer[x + 1] == ' ' ||
            buffer[x + 1] == '"' ||
            buffer[x + 1] == ',' ||
            buffer[x + 1] == '\'')) {
          continue;
        }
        metric_buffer[idx++] = buffer[x];
      }
      metric_bytes = idx;
    } else {
      System.arraycopy(buffer, s, metric_buffer, measurement_index, end - s);
      metric_bytes = measurement_index + (end - s);
    }
    
    if (hash_it) {
      metric_hash = XXHash.hash(metric_buffer, 0, metric_bytes);
      series_hash = XXHash.combineHashes(metric_hash, tag_set_hash);
    }
  }
  
  /**
   * Parses the field value treating booleans as 1 for true and 0 for false. For
   * unsigned ints, converts them to a double for now.
   * @param start The starting index of the value.
   * @param end The end index of the value.
   * @param set Whether or not we store the result in the current value array or
   * in the temp array.
   * @return True if parsing was successful, false if there was an error of some
   * kind.
   */
  private boolean parseValue(final int start, int end, final boolean set) {
    // boolean to 1 or 0
    if (buffer[start] == 't' || buffer[start] == 'T') {
      if (set) {
        value_format = ValueFormat.INTEGER;
        long_value[0] = 1;
      }
      return true;
    } else if (buffer[start] == 'f' || buffer[start] == 'F') {
      if (set) {
        value_format = ValueFormat.INTEGER;
        long_value[0] = 0;
      }
      return true;
    } else if (buffer[end - 1] == 'i') {
      // long!
      if (set) {
        value_format = ValueFormat.INTEGER;
      }
      return Parsing.parseLong(buffer, start, end - 1, set ? long_value : temp_long);
    } else {
      if (buffer[end - 1] == 'u') {
        // parse as double for now
        end--;
      }
      
      if (set) {
        value_format = ValueFormat.DOUBLE;
      }
      
      return Parsing.parseDouble(buffer, start, end, set ? double_value : temp_double);
    }
  }
  
  /**
   * Grows the tag buffer.
   */
  private void growTagBuffer() {
    byte[] temp = new byte[tag_buffer.length * 2];
    System.arraycopy(tag_buffer, 0, temp, 0, tags_length);
    tag_buffer = temp;
  }
 
  /**
   * Called when the stream is null and we're parsing the byte buffer.
   * @return True if we found a line, false if not.
   */
  private boolean advanceBytes() {
    line_start = line_end > 0 ? line_end + 1 : 0;
    
    if (line_start >= end) {
      return false;
    }
    
    // consume whitespace to get to the first measurement.
    while (line_start < end) {
      line_start = findNextChar(line_start);
      if (line_start >= end) {
        return false;
      }
      if (buffer[line_start] == '#') {
        // it's a comment;
        line_end = findNextNewLine(line_start);
        line_start = line_end;
        continue;
      }
      
      if (line_start >= end) {
        return false;
      }
      
      line_end = findNextNewLine(line_start);
      if (processLine()) {
        return true;
      }

      // shift and try again
      line_start = line_end;
    }
    
    // fell through so nothing left.
    line_start = end;
    return false;
  }

  /**
   * Advances the stream until we find a good line or we hit the end of stream.
   * @return True if we found a valid line, false if not.
   */
  private boolean advanceStream() {
    if (eos) {
      return false;
    }
    
    while (!eos) {
      // find the next new line
      int newline = findNextNewLine(line_start);
      if (newline < 0) {
        // need more data!
        newline = readFromStream();
        if (newline < 0) {
          // all done
          return false;
        }
      }
      
      line_start = findNextChar(line_start);
      if (line_start >= end) {
        continue;
      }
      
      if (buffer[line_start] == '#') {
        // it's a comment;
        line_end = findNextNewLine(line_start);
        line_start = line_end;
        continue;
      }
      
      if (line_start >= end) {
        continue;
      }
      
      line_end = newline;
      if (processLine()) {
        return true;
      }
      
      // failed to match so move to the next line and retry
      line_start = line_end;
    }
    
    // eos
    return false;
  }

  /**
   * Reads the next chunk of data from the stream until we have a new line (does
   * not validate the line). It blocks on the stream right now. If possible it
   * will shift unread data in the buffer to the top of the buffer so we avoid
   * expanding it.
   * 
   * @return The offset to the end of the new line in the buffer if found (or
   * we hit the end of the stream), -1 if no new line was found.
   */
  private int readFromStream() {
    // to avoid growing the buffer if we don't have to we shift what hasn't
    // been processed.
    if (line_start > 0) {
      if (buffer[line_end] == '\n') {
        line_end++;
      }
      System.arraycopy(buffer, line_end, buffer, 0, end - line_end);
      end = end - line_end;
      line_start = line_end = 0;
    }
    
    while (!eos) {
      if (end + DEFAULT_STREAM_READ_AMOUNT >= buffer.length) {
        byte[] temp = new byte[buffer.length * 2];
        System.arraycopy(buffer, 0, temp, 0, end);
        buffer = temp;
      }
      
      int read;
      try {
        read = stream.read(buffer, end, DEFAULT_STREAM_READ_AMOUNT);
        if (read < 0) {
          eos = true;
          return -1;
        }
        end += read;
        int newline = findNextNewLine(line_start);
        if (newline > 0) {
          return newline;
        }
        // get some more info.
      } catch (IOException e) {
        throw new IllegalStateException("Failed to read from the stream", e);
      }
    }
    if (end > line_start) {
      return end;
    }
    return -1;
  }

  /**
   * Sorts the tag hashes in numerical order so we can get a deterministic
   * hash.
   * @param low The start index.
   * @param high The end index.
   */
  private void sortTagHashes(final int low, final int high) {
    if (low < high) {
      /* pi is partitioning index, arr[pi] is now at right place */
      int pi = partition(low, high);

      // Recursively sort elements before partition and after partition
      sortTagHashes(low, pi - 1);
      sortTagHashes(pi + 1, high);
    }
  }

  /**
   * Partition function for the {@link #sortTagHashes(int, int)} function.
   * @param low The start index. 
   * @param high The ending index.
   * @return The partition index.
   */
  private int partition(final int low, final int high) {
    long pivot = tag_key_hashes[high];
    int i = (low - 1); // index of smaller element
    for (int j = low; j < high; j++) {
      // If current element is smaller than the pivot
      if (tag_key_hashes[j] < pivot) {
        i++;

        // swap arr[i] and arr[j]
        long temp = tag_key_hashes[i];
        tag_key_hashes[i] = tag_key_hashes[j];
        tag_key_hashes[j] = temp;

        temp = tag_value_hashes[i];
        tag_value_hashes[i] = tag_value_hashes[j];
        tag_value_hashes[j] = temp;
      }
    }

    // swap arr[i+1] and arr[high] (or pivot)
    long temp = tag_key_hashes[i + 1];
    tag_key_hashes[i + 1] = tag_key_hashes[high];
    tag_key_hashes[high] = temp;

    temp = tag_value_hashes[i + 1];
    tag_value_hashes[i + 1] = tag_value_hashes[high];
    tag_value_hashes[high] = temp;

    return i + 1;
  }

}