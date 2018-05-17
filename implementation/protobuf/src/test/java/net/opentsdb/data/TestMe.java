package net.opentsdb.data;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Optional;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.query.serdes.PBufSerdes;
import net.opentsdb.utils.Bytes;

public class TestMe {

  @Test
  public void foo() throws Exception {
    System.out.println("TYPE: " + NumericType.TYPE.getRawType().getSimpleName());
    TimeSeriesQuery q = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1525824000")
            .setEnd("1525827600")
            .build())
        .build();
    
    QueryContext ctx = mock(QueryContext.class);
    when(ctx.query()).thenReturn(q);
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    MutableNumericValue v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824000000L), 42);
    ts.addValue(v);
    
    v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824060000L), 25.6);
    ts.addValue(v);
    
    
    MockTimeSeries ts2 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.bar")
        .addTags("host", "web02")
        .addTags("dc", "phx")
        .build());
    
    v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824000000L), 128);
    ts2.addValue(v);
    
    v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824060000L), 888.997968);
    ts2.addValue(v);
    
    QueryResult result = mock(QueryResult.class);
    when(result.resolution()).thenReturn(ChronoUnit.SECONDS);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts, ts2));
    
    PBufSerdes serdes = new PBufSerdes();
    
    ///////////////////////////
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serdes.serialize(ctx, null, baos, result, null);
    
    System.out.println(Bytes.pretty(baos.toByteArray()));
    
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    
    serdes = new PBufSerdes();
    QueryNode node = mock(QueryNode.class);
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final QueryResult parsed = (QueryResult) invocation.getArguments()[0];
        for (final TimeSeries series : parsed.timeSeries()) {
          System.out.println("ID: " + series.id());
          Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> it = 
              series.iterator(NumericType.TYPE).get();
          while(it.hasNext()) {
            final TimeSeriesValue<NumericType> value = (TimeSeriesValue<NumericType>) it.next();
            if (value.value() == null) {
              System.out.println(value.timestamp() + " Null");
            } else if (value.value().isInteger()) {
              System.out.println(value.timestamp() + " " + value.value().longValue());
            } else {
              System.out.println(value.timestamp() + " " + value.value().doubleValue());
            }
          }
        }
        return null;
      }
      
    }).when(node).onNext(any(QueryResult.class));
    serdes.deserialize(null, bais, node, null);
    
    
  }
  
  @Test
  public void fooNans() throws Exception {
    System.out.println("TYPE: " + NumericType.TYPE.getRawType().getSimpleName());
    TimeSeriesQuery q = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1525824000")
            .setEnd("1525827600")
            .build())
        .build();
    
    QueryContext ctx = mock(QueryContext.class);
    when(ctx.query()).thenReturn(q);
    
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.foo")
        .addTags("host", "web01")
        .build());
    
    MutableNumericValue v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824000000L), Double.NaN);
    ts.addValue(v);
    
    v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824060000L), Double.NaN);
    ts.addValue(v);
    
    v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824120000L), 25.6);
    ts.addValue(v);
    
    MockTimeSeries ts2 = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("metric.bar")
        .addTags("host", "web02")
        .addTags("dc", "phx")
        .build());
    
    v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824000000L), Double.NaN);
    ts2.addValue(v);
    
    v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1525824060000L), 888.997968);
    ts2.addValue(v);
    
    QueryResult result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Lists.newArrayList(ts, ts2));
    
    PBufSerdes serdes = new PBufSerdes();
    
    ///////////////////////////
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serdes.serialize(ctx, null, baos, result, null);
    
    System.out.println(Bytes.pretty(baos.toByteArray()));
    
    ByteArrayInputStream bais = new ByteArrayInputStream(new byte[] { 10, -123, 7, 10, 49, 26, 12, 99, 111, 119, 98, 111, 121, 46, 98, 101, 98, 111, 112, 34, 14, 10, 4, 104, 111, 115, 116, 18, 6, 104, 111, 98, 98, 101, 115, 34, 11, 10, 6, 116, 104, 114, 101, 97, 100, 18, 1, 48, 50, 4, 99, 117, 115, 116, 18, -49, 6, 10, 43, 110, 101, 116, 46, 111, 112, 101, 110, 116, 115, 100, 98, 46, 100, 97, 116, 97, 46, 116, 121, 112, 101, 115, 46, 110, 117, 109, 101, 114, 105, 99, 46, 78, 117, 109, 101, 114, 105, 99, 84, 121, 112, 101, 18, -97, 6, 10, 11, 8, -112, -90, -1, -42, 5, 26, 3, 85, 84, 67, 18, 6, 8, -96, -62, -1, -42, 5, 26, -121, 6, 10, 35, 116, 121, 112, 101, 46, 103, 111, 111, 103, 108, 101, 97, 112, 105, 115, 46, 99, 111, 109, 47, 78, 117, 109, 101, 114, 105, 99, 83, 101, 113, 117, 101, 110, 99, 101, 18, -33, 5, 10, -36, 5, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 64, -39, 94, -81, 86, -21, 43, 28, 0, 0, 0, 15, 64, 115, 81, 125, 8, -10, 102, -119, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 64, -71, -29, -115, 92, -101, -78, 70, 0, 0, 0, 15, 64, -48, -80, -34, 19, -53, -19, 86, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 64, -41, 124, -90, -15, 3, -60, 15, 0, 0, 0, 15, 64, -115, 107, 37, -37, 116, 106, -62, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 64, -67, 22, -108, 54, -18, 12, 36, 0, 0, 0, 15, 64, -49, -13, -17, -46, -88, 35, 81, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 64, -45, -57, 123, -42, 116, -27, -2, 0, 0, 0, 15, 64, 119, -21, 88, -3, -123, -44, -127, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 64, -65, -67, 58, -99, 98, -8, 4, 0, 0, 0, 15, 64, -47, -80, -17, 99, 6, 18, -65, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15, 127, -8, 0, 0, 0, 0, 0, 0});
    
    serdes = new PBufSerdes();
    QueryNode node = mock(QueryNode.class);
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final QueryResult parsed = (QueryResult) invocation.getArguments()[0];
        for (final TimeSeries series : parsed.timeSeries()) {
          System.out.println("ID: " + series.id());
          Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> it = 
              series.iterator(NumericType.TYPE).get();
          while(it.hasNext()) {
            final TimeSeriesValue<NumericType> value = (TimeSeriesValue<NumericType>) it.next();
            if (value.value() == null) {
              System.out.println(value.timestamp() + " Null");
            } else if (value.value().isInteger()) {
              System.out.println(value.timestamp() + " " + value.value().longValue());
            } else {
              System.out.println(value.timestamp() + " " + value.value().doubleValue());
            }
          }
        }
        return null;
      }
      
    }).when(node).onNext(any(QueryResult.class));
    serdes.deserialize(null, bais, node, null);
    
  }
}
