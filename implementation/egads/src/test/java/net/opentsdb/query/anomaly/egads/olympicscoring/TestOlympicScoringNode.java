// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.anomaly.egads.olympicscoring;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.PluginConfigValidator;
import net.opentsdb.core.PluginsConfig;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.core.PluginsConfig.PluginConfig;
import net.opentsdb.data.BaseTimeSeriesDatumStringId;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesDatumStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.alert.AlertType;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySinkCallback;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.anomaly.AnomalyConfig.ExecutionMode;
import net.opentsdb.query.anomaly.AnomalyPredictionState.State;
import net.opentsdb.query.anomaly.AnomalyPredictionState;
import net.opentsdb.query.anomaly.MemoryPredictionCache;
import net.opentsdb.query.anomaly.PredictionCache;
import net.opentsdb.query.execution.serdes.JsonV3QuerySerdesOptions;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.ProcessorFactory;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.readcache.JsonReadCacheSerdes;
import net.opentsdb.query.readcache.ReadCacheSerdesFactory;
import net.opentsdb.query.serdes.SerdesFactory;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.storage.MockDataStoreFactory;
import net.opentsdb.storage.WritableTimeSeriesDataStore;
import net.opentsdb.storage.WritableTimeSeriesDataStoreFactory;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

public class TestOlympicScoringNode {

  private static final int BASE_TIME = 1546300800;
  private static final String HOULRY_METRIC = "egads.metric.hourly";
  private static final String TAGK_STRING = "host";
  private static final String TAGV_A_STRING = "web01";
  private static final String TAGV_B_STRING = "web02";
  private static NumericInterpolatorConfig INTERPOLATOR;
  private static MockTSDB TSDB;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB(true);
    TSDB.registry = new DefaultRegistry(TSDB);
    ((DefaultRegistry) TSDB.registry).initialize(true);
    
    if (!TSDB.getConfig().hasProperty("MockDataStore.register.writer")) {
      TSDB.config.register("MockDataStore.register.writer", true, false, "UT");
    }
    if (!TSDB.getConfig().hasProperty("MockDataStore.threadpool.enable")) {
      TSDB.config.register("MockDataStore.threadpool.enable", false, false, "UT");
    }
    
    MockDataStoreFactory factory = new MockDataStoreFactory();
    factory.initialize(TSDB, null).join(30000);
    ((DefaultRegistry) TSDB.registry).registerPlugin(
        TimeSeriesDataSourceFactory.class, null, (TSDBPlugin) factory);
    
    storeHourlyData();
    
    ((DefaultRegistry) TSDB.registry).registerPlugin(
        ReadCacheSerdesFactory.class, null, new JsonReadCacheSerdes());
    MemoryPredictionCache cache = new MemoryPredictionCache();
    cache.initialize(TSDB, null);
    ((DefaultRegistry) TSDB.registry).registerPlugin(PredictionCache.class, null, cache);
    
    OlympicScoringFactory f = (OlympicScoringFactory) TSDB.registry.getPlugin(ProcessorFactory.class, OlympicScoringFactory.TYPE);
    f.setCache(cache);
    
//    byte[] ck = new byte[] { 54, 75, 38, -45, 109, -48, 104, -124, -3, -50, 94, 33, 92, 43, 72, -88 };
//    AnomalyPredictionState state = new AnomalyPredictionState();
//    state.host = "localhost";
//    state.hash = 1L;
//    state.startTime = state.lastUpdateTime = DateTime.currentTimeMillis() / 1000;
//    state.state = State.RUNNING;
//    cache.setState(ck, state, 100000000);
    
    
    INTERPOLATOR = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
  }
  
  //@Test
  public void hourly() throws Exception {
    
    SemanticQuery baseline_query = SemanticQuery.newBuilder()
        .setStart(Integer.toString(BASE_TIME + (3600 * 11) + 300))
        .setEnd(Integer.toString(BASE_TIME + (3600 * 12) + 300))
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric(HOULRY_METRIC)
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setInterval("1m")
            .setAggregator("avg")
            .setFill(true)
            .addInterpolatorConfig(INTERPOLATOR)
            .addSource("m1")
            .setId("ds")
            .build())
        .build();
    
    final SemanticQuery egads_query = SemanticQuery.newBuilder()
        .setStart(Integer.toString(BASE_TIME + (3600 * 11) + 300))
        //.setEnd(Integer.toString(BASE_TIME + (3600 * 12) + 300))
        .setEnd(Integer.toString(BASE_TIME + (3600 * 11) + 600))
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric(HOULRY_METRIC)
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setInterval("1m")
            .setAggregator("avg")
            .setFill(true)
            .addInterpolatorConfig(INTERPOLATOR)
            .addSource("m1")
            .setId("ds")
            .build())
        .addExecutionGraphNode(OlympicScoringConfig.newBuilder()
            .setBaselinePeriod("1h")
            .setBaselineNumPeriods(3)
            .setBaselineAggregator("avg")
            .setBaselineQuery(baseline_query)
            .setSerializeObserved(true)
            .setSerializeThresholds(true)
            .setLowerThresholdBad(100)
            //.setUpperThreshold(100)
            //.setMode(ExecutionMode.CONFIG)
            .setMode(ExecutionMode.EVALUATE)
            .addInterpolatorConfig(INTERPOLATOR)
            .addSource("ds")
            .setId("egads")
            .build())
//        .addSerdesConfig(JsonV3QuerySerdesOptions.newBuilder()
//            .setId("foo")
//            .addFilter("egads")
//            .addFilter("ds")
//            .build())
        .build();
    System.out.println(JSON.serializeToString(egads_query));
    
    boolean[] flag = new boolean[1];
    Object waity = new Object();
    class Sink implements QuerySink {
      TimeSeriesSerdes serdes = null;
      ByteArrayOutputStream baos;

      Sink() {
        baos = new ByteArrayOutputStream();
        SerdesOptions options = JsonV3QuerySerdesOptions.newBuilder()
            .setId("serdes")
            .build();
        final SerdesFactory factory = TSDB.getRegistry()
            .getPlugin(SerdesFactory.class, options.getType());
        QueryContext ctx = mock(QueryContext.class);
        when(ctx.tsdb()).thenReturn(TSDB);
        when(ctx.query()).thenReturn(egads_query);
        serdes = factory.newInstance(ctx, options, baos);
      }
      
      @Override
      public void onComplete() {
        // TODO Auto-generated method stub
        System.out.println("DONE!!");
        try {
          serdes.serializeComplete(null);
          System.out.println("[JSON]: " + new String(baos.toByteArray()));
        } catch (Exception e) {
          e.printStackTrace();
        }
        
        if (flag[0]) {
          synchronized (waity) {
            waity.notify();
          }
          System.out.println("--------- DONE with waity ----------");
        } else {
          flag[0] = true;
          System.out.println("------------ RUNNING NEXT QUERY!!!!!!!----------------------------------------------");
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          SemanticQuery q = egads_query.toBuilder()
              .setStart(Integer.toString(BASE_TIME + (3600 * 11) + 360))
              //.setEnd(Integer.toString(BASE_TIME + (3600 * 12) + 300))
              .setEnd(Integer.toString(BASE_TIME + (3600 * 11) + 660))
              .build();
          QueryContext ctx = SemanticQueryContext.newBuilder()
              .setTSDB(TSDB)
              .addSink(new Sink())
              .setQuery(q)
              //.setQuery(baseline_query)
              .setMode(QueryMode.SINGLE)
              .build();
          try {
            ctx.initialize(null).join();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          System.out.println("  INITIALIZED. now fetching next");
          ctx.fetchNext(null);
        }
      }

      @Override
      public void onNext(QueryResult next) {
        try {
          serdes.serialize(next, null).addCallback(new Callback<Void, Object>() {
            @Override
            public Void call(Object arg) throws Exception {
              next.close();
              return null;
            }
          })
          .addErrback(new Callback<Object, Exception>() {
            @Override
            public Void call(Exception arg) throws Exception {
              arg.printStackTrace();
              next.close();
              return null;
            }
          });
        // TODO Auto-generated method stub
//        System.out.println("[RESULT]: " + next.source().config().getId() + ":" + next.dataSource());
//        try {
//          if (next.timeSpecification() != null) {
//            System.out.println("     TIME SPEC: " + next.timeSpecification().start().epoch() + " " 
//                + next.timeSpecification().end().epoch());
//          }
//          
//          for (final TimeSeries ts : next.timeSeries()) {
//            System.out.println("[SERIES] " + ts.id() + "  HASH: [" + ts.id().buildHashCode() + "] TYPES: " + ts.types());
//            for (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> it : ts.iterators()) {
//              System.out.println("      IT: " + it.getType());
//              int x = 0;
//              StringBuilder buf = null;
//              while (it.hasNext()) {
//                TimeSeriesValue<? extends TimeSeriesDataType> value = it.next();
//                
//                if (it.getType() == NumericArrayType.TYPE) {
//                  TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) value;
//                  if (value.value() == null) {
//                    System.out.println("WTF? Null value at: " + v.timestamp());
//                    continue;
//                  }
//                  if (v.value().isInteger()) {
//                    System.out.println("   " + Arrays.toString(v.value().longArray()));
//                  } else {
//                    System.out.println("   " + Arrays.toString(v.value().doubleArray()));
//                  }
//                } else if (it.getType() == NumericType.TYPE) {
//                  TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) value;
//                  if (buf == null) {
//                    buf = new StringBuilder()
//                        .append("{");
//                  }
//                  if (x > 0) {
//                    buf.append(", ");
//                  }
//                  buf.append(v.value().toDouble());
//                  //System.out.println(v.timestamp().epoch() + "  " + v.value().toDouble());
//                } else if (it.getType() == AlertType.TYPE) {
//                  TimeSeriesValue<AlertType> v = (TimeSeriesValue<AlertType>) value;
//                  System.out.println("   ALERT! " + v.timestamp().epoch() + "  " + v.value().message());
//                }
//                
//                x++;
//                if (x > 121) {
//                  System.out.println("WHOOP? " + x);
//                  return;
//                }
//              }
//              
//              if (buf != null) {
//                buf.append("}");
//                System.out.println("     " + buf.toString());
//              }
//              System.out.println("   READ: " + x);
//            }
//          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          //next.close();
        }
      }

      @Override
      public void onNext(PartialTimeSeries next, QuerySinkCallback callback) {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void onError(Throwable t) {
        // TODO Auto-generated method stub
        t.printStackTrace();
        waity.notify();
      }
      
    }
    
    QueryContext ctx = SemanticQueryContext.newBuilder()
        .setTSDB(TSDB)
        .addSink(new Sink())
        .setQuery(egads_query)
        //.setQuery(baseline_query)
        .setMode(QueryMode.SINGLE)
        .build();
    ctx.initialize(null).join();
    System.out.println("  INITIALIZED. now fetching next");
    ctx.fetchNext(null);
    
    synchronized (waity) {
      waity.wait(10000);
    }
    System.out.println("---- EXIT ----");
  }
  
  //@Test
  public void weekly() throws Exception {
    //storeWeeklyData();
    SemanticQuery baseline_query = SemanticQuery.newBuilder()
        .setStart(Integer.toString(1545090600))
        .setEnd(Integer.toString(1545091200))
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric(HOULRY_METRIC)
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setInterval("1m")
            .setAggregator("avg")
            .setFill(true)
            .addInterpolatorConfig(INTERPOLATOR)
            .addSource("m1")
            .setId("ds")
            .build())
        .build();
    
    final SemanticQuery egads_query = SemanticQuery.newBuilder()
        .setStart(Integer.toString(BASE_TIME - 300))
        .setEnd(Integer.toString(BASE_TIME))
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric(HOULRY_METRIC)
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setInterval("1m")
            .setAggregator("avg")
            .setFill(true)
            .addInterpolatorConfig(INTERPOLATOR)
            .addSource("m1")
            .setId("ds")
            .build())
        .addExecutionGraphNode(OlympicScoringConfig.newBuilder()
            .setBaselinePeriod("1w")
            .setBaselineNumPeriods(2)
            .setBaselineAggregator("avg")
            .setBaselineQuery(baseline_query)
//            .setSerializeObserved(true)
//            .setSerializeThresholds(true)
            .setLowerThresholdBad(100)
            //.setUpperThreshold(100)
            .setMode(ExecutionMode.CONFIG)
            //.setMode(ExecutionMode.EVALUATE)
            .addInterpolatorConfig(INTERPOLATOR)
            .addSource("ds")
            .setId("egads")
            .build())
//        .addSerdesConfig(JsonV3QuerySerdesOptions.newBuilder()
//            .setId("foo")
//            .addFilter("egads")
//            .addFilter("ds")
//            .build())
        .build();
    //System.out.println(JSON.serializeToString(egads_query));
    
    Object waity = new Object();
    class Sink implements QuerySink {
      TimeSeriesSerdes serdes = null;
      ByteArrayOutputStream baos;

      Sink() {
        baos = new ByteArrayOutputStream();
        SerdesOptions options = JsonV3QuerySerdesOptions.newBuilder()
            .setId("serdes")
            .build();
        final SerdesFactory factory = TSDB.getRegistry()
            .getPlugin(SerdesFactory.class, options.getType());
        QueryContext ctx = mock(QueryContext.class);
        when(ctx.tsdb()).thenReturn(TSDB);
        when(ctx.query()).thenReturn(egads_query);
        serdes = factory.newInstance(ctx, options, baos);
      }
      
      @Override
      public void onComplete() {
        // TODO Auto-generated method stub
        System.out.println("DONE!!");
        try {
          serdes.serializeComplete(null);
          System.out.println("[JSON]: " + new String(baos.toByteArray()));
        } catch (Exception e) {
          e.printStackTrace();
        }
        
        synchronized (waity) {
          waity.notify();
        }
        System.out.println("--------- DONE with waity ----------");
      }

      @Override
      public void onNext(QueryResult next) {
        try {
          serdes.serialize(next, null).addCallback(new Callback<Void, Object>() {
            @Override
            public Void call(Object arg) throws Exception {
              next.close();
              return null;
            }
          })
          .addErrback(new Callback<Object, Exception>() {
            @Override
            public Void call(Exception arg) throws Exception {
              arg.printStackTrace();
              next.close();
              return null;
            }
          });
        // TODO Auto-generated method stub
//        System.out.println("[RESULT]: " + next.source().config().getId() + ":" + next.dataSource());
//        try {
//          if (next.timeSpecification() != null) {
//            System.out.println("     TIME SPEC: " + next.timeSpecification().start().epoch() + " " 
//                + next.timeSpecification().end().epoch());
//          }
//          
//          for (final TimeSeries ts : next.timeSeries()) {
//            System.out.println("[SERIES] " + ts.id() + "  HASH: [" + ts.id().buildHashCode() + "] TYPES: " + ts.types());
//            for (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> it : ts.iterators()) {
//              System.out.println("      IT: " + it.getType());
//              int x = 0;
//              StringBuilder buf = null;
//              while (it.hasNext()) {
//                TimeSeriesValue<? extends TimeSeriesDataType> value = it.next();
//                
//                if (it.getType() == NumericArrayType.TYPE) {
//                  TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) value;
//                  if (value.value() == null) {
//                    System.out.println("WTF? Null value at: " + v.timestamp());
//                    continue;
//                  }
//                  if (v.value().isInteger()) {
//                    System.out.println("   " + Arrays.toString(v.value().longArray()));
//                  } else {
//                    System.out.println("   " + Arrays.toString(v.value().doubleArray()));
//                  }
//                } else if (it.getType() == NumericType.TYPE) {
//                  TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) value;
//                  if (buf == null) {
//                    buf = new StringBuilder()
//                        .append("{");
//                  }
//                  if (x > 0) {
//                    buf.append(", ");
//                  }
//                  buf.append(v.value().toDouble());
//                  //System.out.println(v.timestamp().epoch() + "  " + v.value().toDouble());
//                } else if (it.getType() == AlertType.TYPE) {
//                  TimeSeriesValue<AlertType> v = (TimeSeriesValue<AlertType>) value;
//                  System.out.println("   ALERT! " + v.timestamp().epoch() + "  " + v.value().message());
//                }
//                
//                x++;
//                if (x > 121) {
//                  System.out.println("WHOOP? " + x);
//                  return;
//                }
//              }
//              
//              if (buf != null) {
//                buf.append("}");
//                System.out.println("     " + buf.toString());
//              }
//              System.out.println("   READ: " + x);
//            }
//          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          //next.close();
        }
      }

      @Override
      public void onNext(PartialTimeSeries next, QuerySinkCallback callback) {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void onError(Throwable t) {
        // TODO Auto-generated method stub
        t.printStackTrace();
        waity.notify();
      }
      
    }
    
    QueryContext ctx = SemanticQueryContext.newBuilder()
        .setTSDB(TSDB)
        .addSink(new Sink())
        .setQuery(egads_query)
        //.setQuery(baseline_query)
        //.setMode(QueryMode.SINGLE)
        .build();
    ctx.initialize(null).join();
    System.out.println("  INITIALIZED. now fetching next");
    ctx.fetchNext(null);
    
    synchronized (waity) {
      waity.wait(10000);
    }
    System.out.println("---- EXIT ----");
  }
  
  static void storeHourlyData() throws Exception {
    WritableTimeSeriesDataStoreFactory factory = TSDB.getRegistry().getDefaultPlugin(WritableTimeSeriesDataStoreFactory.class);
    WritableTimeSeriesDataStore store = factory.newStoreInstance(TSDB, null);
    
    TimeSeriesDatumStringId id_a = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric(HOULRY_METRIC)
        .addTags(TAGK_STRING, TAGV_A_STRING)
        .build();
    TimeSeriesDatumStringId id_b = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric(HOULRY_METRIC)
        .addTags(TAGK_STRING, TAGV_B_STRING)
        .build();
    
    int ts = BASE_TIME;
    int wrote = 0;
    for (int x = 0; x < 12; x++) {
      for (int i = 0; i < 60; i++) {
        double value = Math.sin((ts % 3600) / 10);
        
        MutableNumericValue v = 
            new MutableNumericValue(new SecondTimeStamp(ts), value);
        store.write(null, TimeSeriesDatum.wrap(id_a, v), null).join();
        
        if (ts == 1546340580) {
          v = new MutableNumericValue(new SecondTimeStamp(1546340610), value * 10);
          store.write(null, TimeSeriesDatum.wrap(id_a, v), null).join();
          wrote++;
        }
        
        v = new MutableNumericValue(new SecondTimeStamp(ts), value * 10);
        store.write(null, TimeSeriesDatum.wrap(id_b, v), null).join();
        ts += 60;
        wrote++;
      }
    }
    System.out.println(" ------ WROTE TO " + System.identityHashCode(store) + " STORE!  " + wrote);
  }
  
  static void storeWeeklyData() throws Exception {
    WritableTimeSeriesDataStoreFactory factory = TSDB.getRegistry().getDefaultPlugin(WritableTimeSeriesDataStoreFactory.class);
    WritableTimeSeriesDataStore store = factory.newStoreInstance(TSDB, null);
    
    TimeSeriesDatumStringId id_a = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric(HOULRY_METRIC)
        .addTags(TAGK_STRING, TAGV_A_STRING)
        .build();
    TimeSeriesDatumStringId id_b = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric(HOULRY_METRIC)
        .addTags(TAGK_STRING, TAGV_B_STRING)
        .build();
    
    int ts = BASE_TIME - (86400 * 16);
    System.out.println("         WRITE START: " + ts);
    int wrote = 0;
    while (ts <= BASE_TIME) {
      for (int i = 0; i < 60; i++) {
        double value = Math.sin((ts % 3600) / 10);
        
        MutableNumericValue v = 
            new MutableNumericValue(new SecondTimeStamp(ts), value);
        store.write(null, TimeSeriesDatum.wrap(id_a, v), null).join();
                
        v = new MutableNumericValue(new SecondTimeStamp(ts), value * 10);
        store.write(null, TimeSeriesDatum.wrap(id_b, v), null).join();
        ts += 60;
        wrote++;
      }
    }
    System.out.println(" ------ WROTE " + wrote + " dps! ending at " + ts);
  }
}
