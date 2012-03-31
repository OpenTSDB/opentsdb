// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.graph;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;

/**
 * Produces files to generate graphs with Gnuplot.
 * <p>
 * This class takes a bunch of {@link DataPoints} instances and generates a
 * Gnuplot script as well as the corresponding data files to feed to Gnuplot.
 */
public final class Plot {

  private static final Logger LOG = LoggerFactory.getLogger(Plot.class);

  /** Mask to use on 32-bit unsigned integers to avoid sign extension.  */
  private static final long UNSIGNED = 0x00000000FFFFFFFFL;

  /** Default (current) timezone.  */
  private static final TimeZone DEFAULT_TZ = TimeZone.getDefault();

  /** Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private final int start_time;

  /** End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private final int end_time;

  /** All the DataPoints we want to plot. */
  private ArrayList<DataPoints> datapoints =
    new ArrayList<DataPoints>();

  /** Per-DataPoints Gnuplot options. */
  private ArrayList<String> options = new ArrayList<String>();

  /** Global Gnuplot parameters. */
  private Map<String, String> params;

  /** Minimum width / height allowed. */
  private static final short MIN_PIXELS = 100;

  /** Width of the graph to generate, in pixels. */
  private short width = (short) 1024;

  /** Height of the graph to generate, in pixels. */
  private short height = (short) 768;

  /**
   * Number of seconds of difference to apply in order to get local time.
   * Gnuplot always renders timestamps in UTC, so we simply apply a delta
   * to get local time.
   */
  private final short utc_offset;

  /**
   * Constructor.
   * @param start_time Timestamp of the start time of the graph.
   * @param end_time Timestamp of the end time of the graph.
   * @throws IllegalArgumentException if either timestamp is 0 or negative.
   * @throws IllegalArgumentException if {@code start_time >= end_time}.
   */
  public Plot(final long start_time, final long end_time) {
    this(start_time, end_time, DEFAULT_TZ);
  }

  /**
   * Constructor.
   * @param start_time Timestamp of the start time of the graph.
   * @param end_time Timestamp of the end time of the graph.
   * @param tz Timezone to use to render the timestamps.
   * If {@code null} the current timezone as of when the JVM started is used.
   * @throws IllegalArgumentException if either timestamp is 0 or negative.
   * @throws IllegalArgumentException if {@code start_time >= end_time}.
   * @since 1.1
   */
   public Plot(final long start_time, final long end_time, TimeZone tz) {
    if ((start_time & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid start time: " + start_time);
    } else if ((end_time & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid end time: " + end_time);
    } else if (start_time >= end_time) {
      throw new IllegalArgumentException("start time (" + start_time
        + ") is greater than or equal to end time: " + end_time);
    }
    this.start_time = (int) start_time;
    this.end_time = (int) end_time;
    if (tz == null) {
      tz = DEFAULT_TZ;
    }
    this.utc_offset = (short) (tz.getOffset(System.currentTimeMillis()) / 1000);
  }

  /**
   * Sets the global parameters for this plot.
   * @param params Each entry is a Gnuplot setting that will be written as-is
   * in the Gnuplot script file: {@code set KEY VALUE}.
   * When the value is {@code null} the script will instead contain
   * {@code unset KEY}.
   * <p>
   * Special parameters with a special meaning (since OpenTSDB 1.1):
   * <ul>
   * <li>{@code bgcolor}: Either {@code transparent} or an RGB color in
   * hexadecimal (with a leading 'x' as in {@code x01AB23}).</li>
   * <li>{@code fgcolor}: An RGB color in hexadecimal ({@code x42BEE7}).</li>
   * </ul>
   */
  public void setParams(final Map<String, String> params) {
    this.params = params;
  }

  /**
   * Sets the dimensions of the graph (in pixels).
   * @param width The width of the graph produced (in pixels).
   * @param height The height of the graph produced (in pixels).
   * @throws IllegalArgumentException if the width or height are negative,
   * zero or "too small" (e.g. less than 100x100 pixels).
   */
  public void setDimensions(final short width, final short height) {
    if (width < MIN_PIXELS || height < MIN_PIXELS) {
      final String what = width < MIN_PIXELS ? "width" : "height";
      throw new IllegalArgumentException(what + " smaller than " + MIN_PIXELS
                                         + " in " + width + 'x' + height);
    }
    this.width = width;
    this.height = height;
  }

  /**
   * Adds some data points to this plot.
   * @param datapoints The data points to plot.
   * @param options The options to apply to this specific series.
   */
  public void add(final DataPoints datapoints,
                  final String options) {
    // Technically, we could check the number of data points in the
    // datapoints argument in order to do something when there are none, but
    // this is potentially expensive with a SpanGroup since it requires
    // iterating through the entire SpanGroup.  We'll check this later
    // when we're trying to use the data, in order to avoid multiple passes
    // through the entire data.
    this.datapoints.add(datapoints);
    this.options.add(options);
  }

  /**
   * Returns a view on the datapoints in this plot.
   * Do not attempt to modify the return value.
   */
  public Iterable<DataPoints> getDataPoints() {
    return datapoints;
  }

  /**
   * Generates the Gnuplot script and data files.
   * @param basepath The base path to use.  A number of new files will be
   * created and their names will all start with this string.
   * @return The number of data points sent to Gnuplot.  This can be less
   * than the number of data points involved in the query due to things like
   * aggregation or downsampling.
   * @throws IOException if there was an error while writing one of the files.
   */
  public int dumpToFiles(final String basepath) throws IOException {
    int npoints = 0;
    final int nseries = datapoints.size();
    final String datafiles[] = nseries > 0 ? new String[nseries] : null;
    for (int i = 0; i < nseries; i++) {
      datafiles[i] = basepath + "_" + i + ".dat";
      final PrintWriter datafile = new PrintWriter(datafiles[i]);
      try {
        for (final DataPoint d : datapoints.get(i)) {
          final long ts = d.timestamp();
          if (ts >= (start_time & UNSIGNED) && ts <= (end_time & UNSIGNED)) {
            npoints++;
          }
          datafile.print(ts + utc_offset);
          datafile.print(' ');
          if (d.isInteger()) {
            datafile.print(d.longValue());
          } else {
            final double value = d.doubleValue();
            if (value != value || Double.isInfinite(value)) {
              throw new IllegalStateException("NaN or Infinity found in"
                  + " datapoints #" + i + ": " + value + " d=" + d);
            }
            datafile.print(value);
          }
          datafile.print('\n');
        }
      } finally {
        datafile.close();
      }
    }

    if (npoints == 0) {
      // Gnuplot doesn't like empty graphs when xrange and yrange aren't
      // entirely defined, because it can't decide on good ranges with no
      // data.  We always set the xrange, but the yrange is supplied by the
      // user.  Let's make sure it defines a min and a max.
      params.put("yrange", "[0:10]");  // Doesn't matter what values we use.
    }
    writeGnuplotScript(basepath, datafiles);
    return npoints;
  }

  /**
   * Generates the Gnuplot script.
   * @param basepath The base path to use.
   * @param datafiles The names of the data files that need to be plotted,
   * in the order in which they ought to be plotted.  It is assumed that
   * the ith file will correspond to the ith entry in {@code datapoints}.
   * Can be {@code null} if there's no data to plot.
   */
  private void writeGnuplotScript(final String basepath,
                                  final String[] datafiles) throws IOException {
    final String script_path = basepath + ".gnuplot";
    final PrintWriter gp = new PrintWriter(script_path);
    try {
      // XXX don't hardcode all those settings.  At least not like that.
      gp.append("set term png small size ")
        // Why the fuck didn't they also add methods for numbers?
        .append(Short.toString(width)).append(",")
        .append(Short.toString(height));
      final String fgcolor = params.remove("fgcolor");
      String bgcolor = params.remove("bgcolor");
      if (fgcolor != null && bgcolor == null) {
        // We can't specify a fgcolor without specifying a bgcolor.
        bgcolor = "xFFFFFF";  // So use a default.
      }
      if (bgcolor != null) {
        if (fgcolor != null && "transparent".equals(bgcolor)) {
          // In case we need to specify a fgcolor but we wanted a transparent
          // background, we also need to pass a bgcolor otherwise the first
          // hex color will be mistakenly taken as a bgcolor by Gnuplot.
          bgcolor = "transparent xFFFFFF";
        }
        gp.append(' ').append(bgcolor);
      }
      if (fgcolor != null) {
        gp.append(' ').append(fgcolor);
      }

      gp.append("\n"
                + "set xdata time\n"
                + "set timefmt \"%s\"\n"
                + "set xtic rotate\n"
                + "set output \"").append(basepath + ".png").append("\"\n"
                + "set xrange [\"")
        .append(String.valueOf((start_time & UNSIGNED) + utc_offset))
        .append("\":\"")
        .append(String.valueOf((end_time & UNSIGNED) + utc_offset))
        .append("\"]\n");
      if (!params.containsKey("format x")) {
        gp.append("set format x \"").append(xFormat()).append("\"\n");
      }
      final int nseries = datapoints.size();
      if (nseries > 0) {
        gp.write("set grid\n"
                 + "set style data linespoints\n");
        if (!params.containsKey("key")) {
          gp.write("set key right box\n");
        }
      } else {
        gp.write("unset key\n");
        if (params == null || !params.containsKey("label")) {
          gp.write("set label \"No data\" at graph 0.5,0.9 center\n");
        }
      }

      if (params != null) {
        for (final Map.Entry<String, String> entry : params.entrySet()) {
          final String key = entry.getKey();
          final String value = entry.getValue();
          if (value != null) {
            gp.append("set ").append(key)
              .append(' ').append(value).write('\n');
          } else {
            gp.append("unset ").append(key).write('\n');
          }
        }
      }
      for (final String opts : options) {
        if (opts.contains("x1y2")) {
          // Create a second scale for the y-axis on the right-hand side.
          gp.write("set y2tics border\n");
          break;
        }
      }

      gp.write("plot ");
      for (int i = 0; i < nseries; i++) {
        final DataPoints dp = datapoints.get(i);
        final String title = dp.metricName() + dp.getTags();
        gp.append(" \"").append(datafiles[i]).append("\" using 1:2 title \"")
          // TODO(tsuna): Escape double quotes in title.
          .append(title).write('"');
        final String opts = options.get(i);
        if (!opts.isEmpty()) {
          gp.append(' ').write(opts);
        }
        if (i != nseries - 1) {
          gp.print(", \\");
        }
        gp.write('\n');
      }
      if (nseries == 0) {
        gp.write('0');
      }
    } finally {
      gp.close();
      LOG.info("Wrote Gnuplot script to " + script_path);
    }
  }

  /**
   * Finds some sensible default formatting for the X axis (time).
   * @return The Gnuplot time format string to use.
   */
  private String xFormat() {
    long timespan = (end_time & UNSIGNED) - (start_time & UNSIGNED);
    if (timespan < 2100) {  // 35m
      return "%H:%M:%S";
    } else if (timespan < 86400) {  // 1d
      return "%H:%M";
    } else if (timespan < 604800) {  // 1w
      return "%a %H:%M";
    } else if (timespan < 1209600) {  // 2w
      return "%a %d %H:%M";
    } else if (timespan < 7776000) {  // 90d
      return "%b %d";
    } else {
      return "%Y/%m/%d";
    }
  }

}
