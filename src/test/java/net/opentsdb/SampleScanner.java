package net.opentsdb;


import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import javassist.bytecode.SyntheticAttribute;
import net.opentsdb.core.*;
import net.opentsdb.uid.UniqueId;
import org.hbase.async.*;
import org.joda.time.DateTime;
import javax.print.attribute.standard.DateTimeAtCompleted;

public class SampleScanner {
    private static final int UNSET = -1;

    /** Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
    private int start_time = UNSET;

    /** End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
    private int end_time = UNSET;

    /** Minimum time interval (in seconds) wanted between each data point. */
    private int sample_interval;

    public void setStartTime(final long timestamp) {
        if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
          throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
        } else if (end_time != UNSET && timestamp >= getEndTime()) {
          throw new IllegalArgumentException("new start time (" + timestamp
              + ") is greater than or equal to end time: " + getEndTime());
        }
        // Keep the 32 bits.
        start_time = (int) timestamp;
      }

    public long getStartTime() {
        if (start_time == UNSET) {
            throw new IllegalStateException("setStartTime was never called!");
        }
        return start_time & 0x00000000FFFFFFFFL;
    }

    public void setEndTime(final long timestamp) {
        if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
            throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
        } else if (start_time != UNSET && timestamp <= getStartTime()) {
            throw new IllegalArgumentException("new end time (" + timestamp
                    + ") is less than or equal to start time: " + getStartTime());
        }
        // Keep the 32 bits.
        end_time = (int) timestamp;
    }

    public long getEndTime() {
        if (end_time == UNSET) {
            setEndTime(System.currentTimeMillis() / 1000);
        }
        return end_time;
    }


    /** Returns the UNIX timestamp from which we must start scanning.  */
    private long getScanStartTime() {
        // The reason we look before by `MAX_TIMESPAN * 2' seconds is because of
        // the following.  Let's assume MAX_TIMESPAN = 600 (10 minutes) and the
        // start_time = ... 12:31:00.  If we initialize the scanner to look
        // only 10 minutes before, we'll start scanning at time=12:21, which will
        // give us the row that starts at 12:30 (remember: rows are always aligned
        // on MAX_TIMESPAN boundaries -- so in this example, on 10m boundaries).
        // But we need to start scanning at least 1 row before, so we actually
        // look back by twice MAX_TIMESPAN.  Only when start_time is aligned on a
        // MAX_TIMESPAN boundary then we'll mistakenly scan back by an extra row,
        // but this doesn't really matter.
        // Additionally, in case our sample_interval is large, we need to look
        // even further before/after, so use that too.
        final long ts = getStartTime() - Const.MAX_TIMESPAN * 2 - sample_interval;
        return ts > 0 ? ts : 0;
    }

    /** Returns the UNIX timestamp at which we must stop scanning.  */
    private long getScanEndTime() {
        // For the end_time, we have a different problem.  For instance if our
        // end_time = ... 12:30:00, we'll stop scanning when we get to 12:40, but
        // once again we wanna try to look ahead one more row, so to avoid this
        // problem we always add 1 second to the end_time.  Only when the end_time
        // is of the form HH:59:59 then we will scan ahead an extra row, but once
        // again that doesn't really matter.
        // Additionally, in case our sample_interval is large, we need to look
        // even further before/after, so use that too.
        return getEndTime() + Const.MAX_TIMESPAN + 1 + sample_interval;
    }

    public Scanner getScanner(HBaseClient client, String metric) throws HBaseException {

        final short metric_width = 3;
        UniqueId uid = new UniqueId(client, "tsdb-uid".getBytes(), "metrics", metric_width);
        uid.getId(metric);

        final byte[] start_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
        final byte[] end_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
        // We search at least one row before and one row after the start & end
        // time we've been given as it's quite likely that the exact timestamp
        // we're looking for is in the middle of a row.  Plus, a number of things
        // rely on having a few extra data points before & after the exact start
        // & end dates in order to do proper rate calculation or downsampling near
        // the "edges" of the graph.
        Bytes.setInt(start_row, (int)getScanStartTime(), metric_width);
        Bytes.setInt(end_row,(int)getScanEndTime(), metric_width);
        System.arraycopy(uid.getId(metric), 0, start_row, 0, metric_width);
        System.arraycopy(uid.getId(metric), 0, end_row, 0, metric_width);

        final Scanner scanner = client.newScanner("tsdb");
        scanner.setStartKey(start_row);
        scanner.setStopKey(end_row);

        scanner.setFamily("t".getBytes());
        return scanner;
    }

    public static void main(String[] args) {
        HBaseClient client = new HBaseClient("localhost");
        SampleScanner sample = new SampleScanner();

        sample.setStartTime(DateTime.now().minusHours(2).getMillis() / 1000);
        sample.setEndTime(DateTime.now().getMillis() / 1000);
        Scanner scanner = sample.getScanner(client, args[0]);
        try{
            ArrayList<ArrayList<KeyValue>> rows;
            while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    for(KeyValue value : row)  {
                        System.out.println(Bytes.getLong(value.value()));
                        System.out.println(value.timestamp());
                        System.out.println(new Date(value.timestamp()));
                    }
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Should never be here", e);
        }
        scanner.close();
        client.shutdown();
    }

}
