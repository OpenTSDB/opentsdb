// This file is part of OpenTSDB.
// Copyright (C) 2014 The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.plugins.ganglia;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;

import javax.annotation.Nullable;


/** Base Ganglia message class for OpenTSDB insertion. */
abstract class GangliaMessage {

  /** An empty Ganglia message. */
  private static final GangliaMessage EMPTY_MESSAGE = new EmptyMessage();

  /** Ganglia protocols that are not binary compatible. */
  enum ProtocolVersion { V30x, V31x };

  /** @return Ganglia protocol version. */
  abstract ProtocolVersion protocolVersion();

  /** @return True if the Ganglia message has an error. */
  abstract boolean hasError();

  /** @return True if the Ganglia message is a metadata. */
  boolean isMetadata() {
    return false;
  }

  /**
   * Converts a Ganglia message to a metric that has a numeric value
   * with a name if the value is not a string.
   * @return A converted metric if the value is numeric. Otherwise, null.
   * @throws IllegalArgumentException if the Ganglia message is malformed.
   */
  @Nullable
  abstract Metric getMetricIfNumeric();

  /** @return OpenTSDB Tags for this metric message. */
  abstract Collection<Tag> getTags(final InetAddress inet_addr);

  /** @return a Tag for hostname. */
  static Tag createHostnameTag(String hostname) {
    return new Tag("host", hostname);
  }

  /** @return a Tag for IP address. */
  static Tag createIpAddrTag(String ip_addr) {
    return new Tag("ipaddr", ip_addr);
  }

  /** @return A Ganglia message with no metric data in it. */
  static GangliaMessage emptyMessage() {
    return EMPTY_MESSAGE;
  }

  /**
   * Ensures the given reference is not null.
   * @param ref an object reference
   * @param err_msg an error message in case of null
   * @return The validated non-null reference
   * @throws IllegalArgumentException if it is null.
   */
  static <T> T ensureNotNull(final T ref, final String err_msg) {
    if (ref == null) {
      throw new IllegalArgumentException(err_msg);
    }
    return ref;
  }

  /**
   * Ensures the given reference has a non-empty string.
   * @param ref a reference to a string
   * @param err_msg an error message in case of null or empty
   * @return The validated non-empty reference
   * @throws IllegalArgumentException if it is null or empty.
   */
  static String ensureNotEmpty(final String ref, final String err_msg) {
    if (ref == null || ref.isEmpty()) {
      throw new IllegalArgumentException(err_msg);
    }
    return ref;
  }

  /** Ganglia message that has no metric data. */
  static class EmptyMessage extends GangliaMessage {

    @Override
    ProtocolVersion protocolVersion() {
      return ProtocolVersion.V30x;
    }

    @Override
    boolean hasError() {
      return true;
    }

    @Override
    @Nullable
    Metric getMetricIfNumeric() {
      return null;
    }

    @Override
    Collection<Tag> getTags(InetAddress inet_addr) {
      return Collections.emptyList();
    }
  }

  /** A key-value pair to tag metric values. */
  static class Tag {

    /** OpenTSDB Tag key */
    final String key;
    /** OpenTSDB Tag value */
    final String value;

    Tag(String key, String value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      return sb.append("Tag [key=").append(key)
          .append(", value=").append(value)
          .append(']').toString();
    }
  }

  /** Simplified Ganglia metric to import to OpenTSDB. */
  static class Metric {

    /** Metric name */
    private final String name;
    /**
     * The time Ganglia metric arrived. Ganglia message does not come with
     * a timestamp.
     */
    private final long timestamp_millis;
    /** True if the value is stored at long_value. */
    private final boolean is_integer;
    /** Metric value as a long. */
    private final long long_value;
    /** Metric value as a float. We don't support double here. */
    private final float float_value;

    /** Constructor for an integer value. */
    public Metric(final String name, final long timestamp_millis,
        final long long_value) {
      this.name = name;
      this.timestamp_millis = timestamp_millis;
      this.is_integer = true;
      this.long_value = long_value;
      this.float_value = long_value;
    }

    /** Constructor for a floating point value. */
    public Metric(final String name, final long timestamp_millis,
        final float float_value) {
      this.name = name;
      this.timestamp_millis = timestamp_millis;
      this.is_integer = false;
      this.long_value = 0;
      this.float_value = float_value;
    }

    /** @return metric name. */
    public String name() {
      return name;
    }

    /** @return the timestamp of the metric in milliseconds. */
    public long timestamp() {
      return timestamp_millis;
    }

    /** @return true for an integer. false for a float. */
    public boolean isInteger() {
      return is_integer;
    }

    /** @return an integer value. It should not be called for a float. */
    public long longValue() {
      return long_value;
    }

    /** @return a float value. It should not be called for an integer. */
    public float floatValue() {
      return float_value;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      return sb.append("Metric [name=").append(name)
          .append(", timestamp_millis=").append(timestamp_millis)
          .append(", is_integer=").append(is_integer)
          .append(", long_value=").append(long_value)
          .append(", float_value=").append(float_value)
          .append(']').toString();
    }
  }
}
