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
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import info.ganglia.gmetric4j.gmetric.GMetricType;
import info.ganglia.gmetric4j.xdr.v30x.Ganglia_gmetric_message;
import info.ganglia.gmetric4j.xdr.v30x.Ganglia_message;
import info.ganglia.gmetric4j.xdr.v30x.Ganglia_message_formats;
import info.ganglia.gmetric4j.xdr.v30x.Ganglia_spoof_header;
import info.ganglia.gmetric4j.xdr.v30x.Ganglia_spoof_message;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrDecodingStream;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.Tags;


/** wrapper of a {@link Ganglia_message}. */
class GangliaV30xMessage extends GangliaMessage {

  /** Ganglia message */
  private final Ganglia_message msg;
  /** Lazily allocated key-value pairs to tag the metric value. */
  private Collection<Tag> tags;

  GangliaV30xMessage(final Ganglia_message msg) {
    this.msg = msg;
  }

  /** @return true if the message id is one of v30x Ganglia messages. */
  static boolean canDecode(int id) {
    return (id >= Ganglia_message_formats.metric_user_defined &&
        id < Ganglia_message_formats.GANGLIA_NUM_25_METRICS) ||
        (id == Ganglia_message_formats.spoof_metric) ||
        (id == Ganglia_message_formats.spoof_heartbeat);
  }

  /** @return Ganglia protocol version. */
  @Override
  ProtocolVersion protocolVersion() {
    return ProtocolVersion.V30x;
  }

  /** @return True if the Ganglia message has no metric data. */
  @Override
  boolean hasError() {
    return false;
  }

  /**
   * Decodes a Ganglia v30x message from a XDR stream.
   *
   * @param xdr XDR stream to decode a Ganglia v30x message
   * @return A {@link GangliaV30xMessage}.
   * @throws OncRpcException
   * @throws IOException
   */
  static GangliaV30xMessage decode(final XdrDecodingStream xdr)
      throws OncRpcException, IOException {
    final Ganglia_message msg = new Ganglia_message();
    xdr.beginDecoding();
    msg.xdrDecode(xdr);
    xdr.endDecoding();
    return new GangliaV30xMessage(msg);
  }

  /**
   * Converts Ganglia v30x message to a metric that has a {@link DataPoint}
   * with a name if the value is not a string.
   * @return A converted metric unless the value is numeric. Otherwise, null.
   * @throws IllegalArgumentException if the Ganglia message is malformed.
   */
  @Override
  @Nullable
  Metric getMetricIfNumeric() {
    return importGangliaMessageIfNumeric();
  }

  /** @return Tags for this metric message. */
  @Override
  Collection<Tag> getTags(final InetAddress inet_addr) {
    if (tags == null) {
      Ganglia_spoof_header spheader = getSpoofHeaderIfSpoofMetric();
      if (spheader != null) {
        tags = ImmutableList.of(createHostnameTag(spheader.spoofName),
            createIpAddrTag(spheader.spoofIP));
      } else {
        tags = ImmutableList.of(
            createHostnameTag(inet_addr.getCanonicalHostName()),
            createIpAddrTag(inet_addr.getHostAddress()));
      }
    }
    return tags;
  }

  /** @return Spoof header if it has spoof metrics. Otherwise, returns null. */
  @Nullable
  @VisibleForTesting
  Ganglia_spoof_header getSpoofHeaderIfSpoofMetric() {
    if (msg.spmetric != null) {
      final Ganglia_spoof_header spheader = msg.spmetric.spheader;
      ensureNotNull(spheader, "No spoof spheader");
      ensureNotEmpty(spheader.spoofIP, "No spoof header IP");
      ensureNotEmpty(spheader.spoofName, "No spoof header hostname");
      return spheader;
    }
    return null;
  }

  /**
   * Imports a single metric value.
   * @return A converted metric unless the value is numeric. Otherwise, null.
   * @throws NumberFormatException if the timestamp or value is invalid.
   * @throws IllegalArgumentException if the Ganglia message is malformed.
   */
  @Nullable
  private Metric importGangliaMessageIfNumeric() {
    if (msg.spmetric != null) {
      return importSpoofMetricIfNumeric(msg.spmetric.gmetric);
    } else if (msg.spheader != null) {
      // Ignores spoof header without metric.
      return null;
    }
    if (msg.gmetric != null) {
      return importGmetricIfNumeric(msg.gmetric);
    } else if (msg.id < Ganglia_message_formats.GANGLIA_NUM_25_METRICS) {
      return importBaseMetricIfNumeric();
    } else {
      throw new IllegalArgumentException("Unknown metric id - " + msg.id);
    }
  }

  /** @return A converted metric from spmetric if numeric. Otherwise, null. */
  @Nullable
  private Metric importSpoofMetricIfNumeric(
      final Ganglia_gmetric_message gmetric) {
    ensureNotNull(gmetric, "No spoof gmetric");
    return importGmetricIfNumeric(gmetric);
  }

  /** @return A converted metric from spmetric if numeric. Otherwise, null. */
  @Nullable
  private Metric importGmetricIfNumeric(final Ganglia_gmetric_message gmt) {
    if (GMetricType.STRING.getGangliaType().equals(gmt.type)) {
      // NOTE: We do not import string value metrics.
      // TODO: Support string value metrics.
      return null;
    }

    String name = ensureNotEmpty(gmt.name, "No gmetric name");;
    String value = ensureNotEmpty(gmt.value, "No gmetric value");
    // Ganglia metrics do not come with a timestamp. We should add it.
    long timestamp = System.currentTimeMillis();
    if (Tags.looksLikeInteger(value)) {
      return new Metric(name, timestamp, Tags.parseLong(value));
    } else {
      // TODO: Support double precision values.
      // We do not support double yet to save storage space.
      return new Metric(name, timestamp, Float.parseFloat(value));
    }
  }

  /** @return A converted metric from base metric if numeric. Otherwise, null. */
  @Nullable
  private Metric importBaseMetricIfNumeric() {
    BaseMetrics base_metric = ensureNotNull(BaseMetrics.METRICS.get(msg.id),
        "Unknown metric id");
    // Ganglia metrics do not come with a timestamp. We should add it.
    long timestamp = System.currentTimeMillis();
    switch (base_metric.type) {
    case DOUBLE:
      // TODO: Support double precision values.
      // We do not support double yet to save storage space.
      return new Metric(base_metric.name, timestamp, (float)msg.d);
    case FLOAT:
      return new Metric(base_metric.name, timestamp, msg.f);
    case INT16:
    case UINT16:
      return new Metric(base_metric.name, timestamp, msg.u_short1);
    case INT32:
    case UINT32:
      return new Metric(base_metric.name, timestamp, msg.u_int1);
    case STRING:
      // NOTE: We do not import string value metrics.
      // TODO: Support string value metrics.
      return null;
    case INT8:
    case UINT8:
      // NOTE: No base metrics have either INT8 or UINT8 types.
    default:
      break;
    }
    throw new IllegalArgumentException("Unexpeted type - " + base_metric);
  }

  @Override
  public String toString() {
    return gmessageToString(msg).toString();
  }

  public static String gmessageToString(final Ganglia_message gm) {
    StringBuilder sb = new StringBuilder();
    return sb.append("Ganglia_message [id=").append(gm.id)
        .append(", gmetric=").append(gmetricToString(gm.gmetric))
        .append(", spmetric=").append(spmetricToString(gm.spmetric))
        .append(", spheader=").append(spheaderToString(gm.spheader))
        .append(", u_short1=").append(gm.u_short1)
        .append(", u_int1=").append(gm.u_int1)
        .append(", str=").append(gm.str)
        .append(", f=").append(gm.f)
        .append(", d=").append(gm.d)
        .append("]").toString();
  }

  public static String gmetricToString(final Ganglia_gmetric_message gmt) {
    if (gmt == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    return sb.append("Ganglia_gmetric_message [type=").append(gmt.type)
        .append(", name=").append(gmt.name)
        .append(", value=").append(gmt.value)
        .append(", units=").append(gmt.units)
        .append(", slope=").append(gmt.slope)
        .append(", tmax=").append(gmt.tmax)
        .append(", dmax=").append(gmt.dmax)
        .append("]").toString();
  }

  public static String spheaderToString(final Ganglia_spoof_header spheader) {
    if (spheader == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    return sb.append("Ganglia_spoof_header [spoofName=")
        .append(spheader.spoofName)
        .append(", spoofIp=").append(spheader.spoofIP)
        .append("]").toString();
  }

  public static String spmetricToString(final Ganglia_spoof_message spmetric) {
    if (spmetric == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    return sb.append("Ganglia_spoof_message [spheader=")
        .append(spheaderToString(spmetric.spheader))
        .append(", gmetric=")
        .append(gmetricToString(spmetric.gmetric))
        .append("]").toString();
  }

  /** A map of base metrics. */
  private static class BaseMetrics {

    private static final List<BaseMetrics> ALL_BASE_METRICS = ImmutableList.of(
        new BaseMetrics(Ganglia_message_formats.metric_cpu_num,
            "cpu_num", GMetricType.UINT16),

        new BaseMetrics(Ganglia_message_formats.metric_cpu_speed,
            "cpu_speed", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_mem_total,
            "mem_total", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_swap_total,
            "swap_total", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_boottime,
            "boottime", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_sys_clock,
            "sys_clock", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_proc_run,
            "proc_run", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_proc_total,
            "proc_total", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_mem_free,
            "mem_free", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_mem_shared,
            "mem_shared", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_mem_buffers,
            "mem_buffers", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_mem_cached,
            "mem_cached", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_swap_free,
            "swap_free", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_heartbeat,
            "heartbeat", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_mtu,
            "mtu", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_mem_arm,
            "mem_arm", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_mem_rm,
            "mem_rm", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_mem_avm,
            "mem_avm", GMetricType.UINT32),
        new BaseMetrics(Ganglia_message_formats.metric_mem_vm,
            "mem_vm", GMetricType.UINT32),

        new BaseMetrics(Ganglia_message_formats.metric_machine_type,
            "machine_type", GMetricType.STRING),
        new BaseMetrics(Ganglia_message_formats.metric_os_name,
            "os_name", GMetricType.STRING),
        new BaseMetrics(Ganglia_message_formats.metric_os_release,
            "os_release", GMetricType.STRING),
        new BaseMetrics(Ganglia_message_formats.metric_gexec,
            "gexec", GMetricType.STRING),
        new BaseMetrics(Ganglia_message_formats.metric_location,
            "location", GMetricType.STRING),

        new BaseMetrics(Ganglia_message_formats.metric_cpu_user,
            "cpu_user", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_cpu_nice,
            "cpu_nice", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_cpu_system,
            "cpu_system", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_cpu_idle,
            "cpu_idle", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_cpu_aidle,
            "cpu_aidle", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_load_one,
            "load_one", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_load_five,
            "load_five", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_load_fifteen,
            "load_fifteen", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_bytes_in,
            "bytes_in", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_bytes_out,
            "bytes_out", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_pkts_in,
            "pkts_in", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_pkts_out,
            "pkts_out", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_part_max_used,
            "part_max_used", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_cpu_wio,
            "cpu_wio", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_bread_sec,
            "bread_sec", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_bwrite_sec,
            "bwrite_sec", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_lread_sec,
            "lread_sec", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_lwrite_sec,
            "lwrite_sec", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_rcache,
            "rcache", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_wcache,
            "wcache", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_phread_sec,
            "phread_sec", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_phwrite_sec,
            "phwrite_sec", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_cpu_intr,
            "cpu_intr", GMetricType.FLOAT),
        new BaseMetrics(Ganglia_message_formats.metric_cpu_sintr,
            "cpu_sintr", GMetricType.FLOAT),

        new BaseMetrics(Ganglia_message_formats.metric_disk_total,
            "disk_total", GMetricType.DOUBLE),
        new BaseMetrics(Ganglia_message_formats.metric_disk_free,
            "disk_free", GMetricType.DOUBLE));
    private static final Map<Integer, BaseMetrics> METRICS;

    static {
      // Creates a map of metric ids to {@link BaseMetrics}.
      Map<Integer, BaseMetrics> metrics =
          Maps.newHashMapWithExpectedSize(ALL_BASE_METRICS.size());
      for (BaseMetrics m: ALL_BASE_METRICS) {
        metrics.put(m.id, m);
      }
      METRICS = ImmutableMap.copyOf(metrics);
    }

    /** Metric id */
    private final int id;
    /** Metric name */
    private final String name;
    /** Metric type */
    private final GMetricType type;

    private BaseMetrics(final int id, final String name,
        final GMetricType type) {
      this.id = id;
      this.type = type;
      this.name = name;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      return sb.append("BaseMetrics [id=").append(id)
          .append(", name=").append(name)
          .append(", type=").append(type)
          .append("]").toString();
    }
  }

  /** Helper class for unit tests. */
  @VisibleForTesting
  static class ForTesting {

    /** @return Ganglia message. */
    static Ganglia_message get_ganglia_message(GangliaV30xMessage message) {
      return message.msg;
    }
  }
}
