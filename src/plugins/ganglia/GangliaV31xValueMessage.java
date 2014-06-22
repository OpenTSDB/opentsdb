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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_double;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_float;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_int;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_short;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_string;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_uint;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_gmetric_ushort;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_metric_id;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_msg_formats;
import info.ganglia.gmetric4j.xdr.v31x.Ganglia_value_msg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrDecodingStream;

import net.opentsdb.core.Tags;


/** wrapper of a {@link Ganglia_value_msg}. */
class GangliaV31xValueMessage extends GangliaMessage {

  private static final Pattern FLOATING_POINT_NUMBER =
      Pattern.compile("^[-+]?[0-9]*(\\.?[0-9]*([eE][-+]?[0-9]+)?$)");

  /** Ganglia 31x protocol value message */
  private final Ganglia_value_msg msg;
  /** The metric of this Ganglia message. */
  private final Ganglia_metric_id metric_id;
  /** Metric value. */
  @Nullable
  private final Metric value;
  /** Lazily allocated key-value pairs to tag the metric value. */
  private Collection<Tag> tags;

  GangliaV31xValueMessage(final Ganglia_value_msg msg,
      final Ganglia_metric_id metric_id, @Nullable final Metric value) {
    this.msg = msg;
    this.metric_id = metric_id;
    this.value = value;
  }

  /** @return true if the message id is one of v31x value messages. */
  static boolean canDecode(int id) {
    // TODO: Support metadata.
    return Ganglia_msg_formats.gmetadata_full < id &&
        id < Ganglia_msg_formats.gmetadata_request;
  }

  /** @return Ganglia protocol version. */
  @Override
  ProtocolVersion protocolVersion() {
    return ProtocolVersion.V31x;
  }

  /** @return True if the Ganglia message has no metric data. */
  @Override
  boolean hasError() {
    return false;
  }

  /**
   * Decodes a Ganglia v31x value message from a XDR stream.
   *
   * @param xdr XDR stream to decode a Ganglia v31x value message
   * @return A {@link GangliaV31xValueMessage} instance
   * @throws OncRpcException
   * @throws IOException
   */
  static GangliaV31xValueMessage decode(final XdrDecodingStream xdr)
      throws OncRpcException, IOException {
    final Ganglia_value_msg msg = new Ganglia_value_msg();
    xdr.beginDecoding();
    msg.xdrDecode(xdr);
    xdr.endDecoding();
    return newGangliaV31xValueMessage(msg);
  }

  /**
   * Converts Ganglia 31x value message to a metric that has a numeric value
   * with a name if the value is not a string.
   * @return A converted metric unless the value is numeric. Otherwise, null.
   * @throws IllegalArgumentException if the Ganglia message is malformed.
   */
  @Override
  @Nullable
  Metric getMetricIfNumeric() {
    return value;
  }

  /** @return Tags for this metric message. */
  @Override
  Collection<Tag> getTags(final InetAddress inet_addr) {
    if (tags == null) {
      if (metric_id.spoof) {
        // Spoof host names come in the form of "ip_address:hostname".
        String[] ipAddrAndHost = metric_id.host.split(":");
        if (ipAddrAndHost.length == 2) {
          tags = ImmutableList.of(createIpAddrTag(ipAddrAndHost[0]),
              createHostnameTag(ipAddrAndHost[1]));
        } else {
          // Assumes it is just a hostname if it is not clear.
          tags = Lists.newArrayList(createHostnameTag(metric_id.host));
        }
      } else {
        tags = ImmutableList.of(createHostnameTag(metric_id.host),
            createIpAddrTag(inet_addr.getHostAddress()));
      }
    }
    return tags;
  }

  /**
   * Imports a single metric value.
   * @param msg A Ganglia_value_msg object.
   * @return A converted metric unless the value is numeric. Otherwise, null.
   * @throws NumberFormatException if the timestamp or value is invalid.
   * @throws IllegalArgumentException if the Ganglia message is malformed.
   */
  @Nullable
  private static GangliaV31xValueMessage newGangliaV31xValueMessage(
      final Ganglia_value_msg msg) {
    // Ganglia metrics do not come with a timestamp. We should add it.
    long timestamp = System.currentTimeMillis();
    Ganglia_metric_id metric_id;
    Metric metric_value;
    switch (msg.id) {
    case Ganglia_msg_formats.gmetric_ushort:
      Ganglia_gmetric_ushort gu_short = ensureNotNull(msg.gu_short,
          "gu_short is null");
      metric_id = ensureNotNull(msg.gu_short.metric_id,
          "gu_short.metric_id is null");
      metric_value = new Metric(metric_id.name, timestamp, gu_short.us);
      break;
    case Ganglia_msg_formats.gmetric_short:
      Ganglia_gmetric_short gs_short = ensureNotNull(msg.gs_short,
          "gs_short is null");
      metric_id = ensureNotNull(msg.gs_short.metric_id,
          "gs_short.metric_id is null");
      metric_value = new Metric(metric_id.name, timestamp, gs_short.ss);
      break;
    case Ganglia_msg_formats.gmetric_int:
      Ganglia_gmetric_int gs_int = ensureNotNull(msg.gs_int,
          "gs_int is null");
      metric_id = ensureNotNull(msg.gs_int.metric_id,
          "gs_int.metric_id is null");
      metric_value = new Metric(metric_id.name, timestamp, gs_int.si);
      break;
    case Ganglia_msg_formats.gmetric_uint:
      Ganglia_gmetric_uint gu_int = ensureNotNull(msg.gu_int,
          "gu_int is null");
      metric_id = ensureNotNull(msg.gu_int.metric_id,
          "gu_int.metric_id is null");
      metric_value = new Metric(metric_id.name, timestamp, gu_int.ui);
      break;
    case Ganglia_msg_formats.gmetric_string:
      Ganglia_gmetric_string gstr = ensureNotNull(msg.gstr,
          "gstr is null");
      metric_id = ensureNotNull(msg.gstr.metric_id,
          "gstr.metric_id is null");
      // NOTE: In many cases, users send numeric metric values in string.
      // We just don't know the type of metrics. So, we just try to convert
      // string values to numeric values as far as the string values are in
      // the numeric form.
      String value = ensureNotNull(gstr.str, "gstr.str is null");
      metric_value = newMetricIfNumericStringValue(metric_id.name, timestamp,
          value);
      break;
    case Ganglia_msg_formats.gmetric_float:
      Ganglia_gmetric_float gf = ensureNotNull(msg.gf,
          "gf is null");
      metric_id = ensureNotNull(msg.gf.metric_id,
          "gf.metric_id is null");
      metric_value = new Metric(metric_id.name, timestamp, gf.f);
      break;
    case Ganglia_msg_formats.gmetric_double:
      // TODO: Support double precision values.
      // We do not support it yet to save storage space.
      Ganglia_gmetric_double gd = ensureNotNull(msg.gd,
          "gd is null");
      metric_id = ensureNotNull(msg.gd.metric_id,
          "gd.metric_id is null");
      metric_value = new Metric(metric_id.name, timestamp, (float)gd.d);
      break;
    default:
      throw new IllegalArgumentException("Unknown metric id - " + msg.id);
    }
    ensureNotEmpty(metric_id.host, "metric_id.host is null for " + msg.id);
    ensureNotEmpty(metric_id.host, "metric_id.name is null for " + msg.id);
    return new GangliaV31xValueMessage(msg, metric_id, metric_value);
  }

  /**
   * Converts a numeric string value to a Metric object.
   * @param name metric name.
   * @param timestamp metric timestamp.
   * @param value metric value in string
   * @return A metric object if the given value is a valid numeric string.
   * Otherwise, returns null.
   */
  @Nullable
  private static Metric newMetricIfNumericStringValue(
      String name, long timestamp, String value) {
    try {
      // Guesses whether the given string is an integer, a float value, or
      // a non-numeric string to reduce the expensive operation of throwing
      // NumberFormatException.
      Matcher matcher = (FLOATING_POINT_NUMBER.matcher(value));
      if (matcher.matches()) {
        if (matcher.group(1).isEmpty()) {
          return new Metric(name, timestamp, Tags.parseLong(value));
        } else {
          // TODO: Support double precision values.
          // We do not support it yet to save storage space.
          return new Metric(name, timestamp, Float.parseFloat(value));
        }
      }
    } catch (NumberFormatException ignored) {
    }
    // Not a numeric string. Ignores the value.
    return null;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Ganglia_value_msg [id=").append(msg.id)
      .append(", metric_id=").append(metricIdToString(metric_id))
      .append(", metric=").append(value);
    if (msg.gu_short != null) {
      sb.append(", gu_short=[fmt=").append(msg.gu_short.fmt)
        .append(", us=").append(msg.gu_short.us)
        .append(']');
    }
    if (msg.gs_short != null) {
      sb.append(", gs_short=[fmt=").append(msg.gs_short.fmt)
        .append(", ss=").append(msg.gs_short.ss)
        .append(']');
    }
    if (msg.gs_int != null) {
      sb.append(", gs_int=[fmt=").append(msg.gs_int.fmt)
        .append(", si=").append(msg.gs_int.si)
        .append(']');
    }
    if (msg.gu_int != null) {
      sb.append(", gu_int=[fmt=").append(msg.gu_int.fmt)
        .append(", ui=").append(msg.gu_int.ui)
        .append(']');
    }
    if (msg.gstr != null) {
      sb.append(", gstr=[fmt=").append(msg.gstr.fmt)
        .append(", str=").append(msg.gstr.str)
        .append(']');
    }
    if (msg.gf != null) {
      sb.append(", gf=[fmt=").append(msg.gf.fmt)
        .append(", f=").append(msg.gf.f)
        .append(']');
    }
    if (msg.gd != null) {
      sb.append(", gd=[fmt=").append(msg.gd.fmt)
        .append(", d=").append(msg.gd.d)
        .append(']');
    }
    if (msg.uuid != null) {
      sb.append(", uuid=[uuid=").append(msg.uuid.uuid).append(']');
    }
    return sb.append(']').toString();
  }

  private String metricIdToString(Ganglia_metric_id id) {
    StringBuilder sb = new StringBuilder();
    return sb.append("Ganglia_metric_id [host=").append(id.host)
        .append(", name=").append(id.name)
        .append(", spoof=").append(id.spoof)
        .append(']').toString();
  }

  /** Helper class for unit tests. */
  @VisibleForTesting
  static class ForTesting {

    /** @return Ganglia message. */
    static Ganglia_value_msg get_ganglia_message(GangliaV31xValueMessage msg) {
      return msg.msg;
    }
  }
}
