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
package net.opentsdb.query.processor.bucketquantile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.bucketquantile.BucketQuantileConfig.OutputOfBucket;
import net.opentsdb.utils.JSON;

public class TestBucketQuantileConfig {

  private NumericInterpolatorConfig numeric_config;
  
  @Before
  public void before() throws Exception {
    numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
  }
  
  @Test
  public void ctor() throws Exception {
    BucketQuantileConfig config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    
    assertEquals("quantile", config.getAs());
    assertEquals("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)", config.getBucketRegex());
    assertEquals(1024, config.getOverflowMax(), 0.001);
    assertEquals("m_overflow", config.getOverflow());
    assertEquals(1, config.getUnderflowMin(), 0.001);
    assertEquals("m_underflow", config.getUnderflow());
    assertEquals(OutputOfBucket.BOTTOM, config.getOutputOfBucket());
    assertEquals(3, config.getHistograms().size());
    assertEquals("m1", config.getHistograms().get(0));
    assertEquals("m2", config.getHistograms().get(1));
    assertEquals("m3", config.getHistograms().get(2));
    assertEquals(3, config.getQuantiles().size());
    assertEquals(0.990, config.getQuantiles().get(0), 0.001);
    assertEquals(0.999, config.getQuantiles().get(1), 0.001);
    assertEquals(0.9999, config.getQuantiles().get(2), 0.001);
    assertTrue(config.getCumulativeBuckets());
    assertTrue(config.getCounterBuckets());
    assertEquals(50, config.getNanThreshold(), 0.001);
    assertEquals(10, config.getMissingMetricThreshold(), 0.001);
    assertTrue(config.getInfectiousNan());
    assertSame(numeric_config, config.interpolatorConfig(NumericType.TYPE));
    assertEquals("q", config.getId());
    
    // defaults
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertEquals("quantile", config.getAs());
    assertEquals(BucketQuantileConfig.DEFAULT_PATTERN, config.getBucketRegex());
    assertEquals(Double.MAX_VALUE, config.getOverflowMax(), 0.001);
    assertNull(config.getOverflow());
    assertEquals(0, config.getUnderflowMin(), 0.001);
    assertNull(config.getUnderflow());
    assertEquals(OutputOfBucket.MEAN, config.getOutputOfBucket());
    assertEquals(3, config.getHistograms().size());
    assertEquals("m1", config.getHistograms().get(0));
    assertEquals("m2", config.getHistograms().get(1));
    assertEquals("m3", config.getHistograms().get(2));
    assertEquals(3, config.getQuantiles().size());
    assertEquals(0.990, config.getQuantiles().get(0), 0.001);
    assertEquals(0.999, config.getQuantiles().get(1), 0.001);
    assertEquals(0.9999, config.getQuantiles().get(2), 0.001);
    assertFalse(config.getCumulativeBuckets());
    assertFalse(config.getCounterBuckets());
    assertEquals(0, config.getNanThreshold(), 0.001);
    assertEquals(0, config.getMissingMetricThreshold(), 0.001);
    assertFalse(config.getInfectiousNan());
    assertSame(numeric_config, config.interpolatorConfig(NumericType.TYPE));
    assertEquals("q", config.getId());
    
    try {
      config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
          //.setAs("quantile")
          .addHistogram("m2")
          .addHistogram("m3")
          .addHistogram("m1")
          .addQuantile(99.9)
          .addQuantile(99.99)
          .addQuantile(99.0)
          .addInterpolatorConfig(numeric_config)
          .setId("q")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
          .setAs("quantile")
          //.addHistogram("m2")
          //.addHistogram("m3")
          //.addHistogram("m1")
          .addQuantile(99.9)
          .addQuantile(99.99)
          .addQuantile(99.0)
          .addInterpolatorConfig(numeric_config)
          .setId("q")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
          .setAs("quantile")
          .addHistogram("m2")
          .addHistogram("m3")
          .addHistogram("m1")
          //.addQuantile(99.9)
          //.addQuantile(99.99)
          //.addQuantile(99.0)
          .addInterpolatorConfig(numeric_config)
          .setId("q")
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
          .setAs("quantile")
          .setBucketRegex("does not (compute")
          .addHistogram("m2")
          .addQuantile(99.9)
          .addInterpolatorConfig(numeric_config)
          .setId("q")
          .build();
      fail("Expected PatternSyntaxException");
    } catch (PatternSyntaxException e) { }
  }

  @Test
  public void equalsAndHashCode() throws Exception {
    final BucketQuantileConfig config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    
    BucketQuantileConfig config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertTrue(config.equals(config2));
    assertEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quan") // <-- Diff
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        //.setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        //.setOverFlowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        //.setOverFlow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        //.setUnderFlowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        //.setUnderFlow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        //.setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m1") // <-- Diff order is ok
        .addHistogram("m2")
        .addHistogram("m3")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertTrue(config.equals(config2));
    assertEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        //.addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.0) // <-- Diff order ok
        .addQuantile(99.9)
        .addQuantile(99.99)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertTrue(config.equals(config2));
    assertEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        //.addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        //.setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        //.setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        //.setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        //.setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        //.setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q1") // <-- Diff
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
    
    config2 = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig((NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NOT_A_NUMBER)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .setDataType(NumericType.TYPE.toString())
            .build())
        .setId("q")
        .build();
    assertFalse(config.equals(config2));
    assertNotEquals(config.hashCode(), config2.hashCode());
  }

  @Test
  public void serdes() throws Exception {
    BucketQuantileConfig config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    final String json = JSON.serializeToString(config);
    System.out.println(json);
    assertTrue(json.contains("\"id\":\"q\""));
    assertTrue(json.contains("\"interpolatorConfigs\":["));
    assertTrue(json.contains("\"infectiousNan\":true"));
    assertTrue(json.contains("\"cumulativeBuckets\":true"));
    assertTrue(json.contains("\"counterBuckets\":true"));
    assertTrue(json.contains("\"as\":\"quantile\""));
    assertTrue(json.contains("\"bucketRegex\":\"([\\\\-0-9\\\\.]+)[_\\\\-]([\\\\-0-9\\\\.]+)\""));
    assertTrue(json.contains("\"overflowMax\":1024.0"));
    assertTrue(json.contains("\"overflow\":\"m_overflow\""));
    assertTrue(json.contains("\"underflowMin\":1.0"));
    assertTrue(json.contains("\"underflow\":\"m_underflow\""));
    assertTrue(json.contains("\"outputOfBucket\":\"BOTTOM\""));
    assertTrue(json.contains("\"histograms\":[\"m1\",\"m2\",\"m3\"]"));
    assertTrue(json.contains("\"quantiles\":[0.99,0.9990000000000001,0.9998999999999999]"));
    assertTrue(json.contains("\"type\":\"BucketQuantile\""));
    assertTrue(json.contains("\"nanThreshold\":50.0"));
    assertTrue(json.contains("\"missingMetricThreshold\":10.0"));
    
    MockTSDB tsdb = MockTSDBDefault.getMockTSDB();
    JsonNode node = JSON.getMapper().readTree(json);
    config = (BucketQuantileConfig) BucketQuantileConfig.parse(JSON.getMapper(), tsdb, node);
    
    assertEquals("quantile", config.getAs());
    assertEquals("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)", config.getBucketRegex());
    assertEquals(1024, config.getOverflowMax(), 0.001);
    assertEquals("m_overflow", config.getOverflow());
    assertEquals(1, config.getUnderflowMin(), 0.001);
    assertEquals("m_underflow", config.getUnderflow());
    assertEquals(OutputOfBucket.BOTTOM, config.getOutputOfBucket());
    assertEquals(3, config.getHistograms().size());
    assertEquals("m1", config.getHistograms().get(0));
    assertEquals("m2", config.getHistograms().get(1));
    assertEquals("m3", config.getHistograms().get(2));
    assertEquals(3, config.getQuantiles().size());
    assertEquals(0.990, config.getQuantiles().get(0), 0.001);
    assertEquals(0.999, config.getQuantiles().get(1), 0.001);
    assertEquals(0.9999, config.getQuantiles().get(2), 0.001);
    assertTrue(config.getCumulativeBuckets());
    assertTrue(config.getCounterBuckets());
    assertEquals(50, config.getNanThreshold(), 0.001);
    assertEquals(10, config.getMissingMetricThreshold(), 0.001);
    assertTrue(config.getInfectiousNan());
    assertEquals(numeric_config, config.interpolatorConfig(NumericType.TYPE));
    assertEquals("q", config.getId());
  }
  
  @Test
  public void toBuilder() throws Exception {
    BucketQuantileConfig old_config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setBucketRegex("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)")
        .setOverflowMax(1024)
        .setOverflow("m_overflow")
        .setUnderflowMin(1)
        .setUnderflow("m_underflow")
        .setOutputOfBucket(OutputOfBucket.BOTTOM)
        .addHistogram("m2")
        .addHistogram("m3")
        .addHistogram("m1")
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .setCumulativeBuckets(true)
        .setCounterBuckets(true)
        .setNanThreshold(50)
        .setMissingMetricThreshold(10)
        .setInfectiousNan(true)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    
    BucketQuantileConfig config = (BucketQuantileConfig) old_config.toBuilder().build();
    assertEquals("quantile", config.getAs());
    assertEquals("([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)", config.getBucketRegex());
    assertEquals(1024, config.getOverflowMax(), 0.001);
    assertEquals("m_overflow", config.getOverflow());
    assertEquals(1, config.getUnderflowMin(), 0.001);
    assertEquals("m_underflow", config.getUnderflow());
    assertEquals(OutputOfBucket.BOTTOM, config.getOutputOfBucket());
    assertEquals(3, config.getHistograms().size());
    assertEquals("m1", config.getHistograms().get(0));
    assertEquals("m2", config.getHistograms().get(1));
    assertEquals("m3", config.getHistograms().get(2));
    assertEquals(3, config.getQuantiles().size());
    assertEquals(0.990, config.getQuantiles().get(0), 0.001);
    assertEquals(0.999, config.getQuantiles().get(1), 0.001);
    assertEquals(0.9999, config.getQuantiles().get(2), 0.001);
    assertTrue(config.getCumulativeBuckets());
    assertTrue(config.getCounterBuckets());
    assertEquals(50, config.getNanThreshold(), 0.001);
    assertEquals(10, config.getMissingMetricThreshold(), 0.001);
    assertTrue(config.getInfectiousNan());
    assertEquals(numeric_config, config.interpolatorConfig(NumericType.TYPE));
    assertEquals("q", config.getId());
  }

  @Test
  public void defaultRegex() throws Exception {
    final Pattern pattern = Pattern.compile(BucketQuantileConfig.DEFAULT_PATTERN);
    String metric = "m_0_250";
    Matcher matcher = pattern.matcher(metric);
    assertTrue(matcher.find());
    assertEquals("0", matcher.group(1));
    assertEquals("250", matcher.group(2));
    
    metric = "m-0-250";
    matcher = pattern.matcher(metric);
    assertTrue(matcher.find());
    assertEquals("0", matcher.group(1));
    assertEquals("250", matcher.group(2));
    
    metric = "m.0-250";
    matcher = pattern.matcher(metric);
    assertTrue(matcher.find());
    assertEquals("0", matcher.group(1));
    assertEquals("250", matcher.group(2));
    
    metric = "m.0.25-1.75";
    matcher = pattern.matcher(metric);
    assertTrue(matcher.find());
    assertEquals("0.25", matcher.group(1));
    assertEquals("1.75", matcher.group(2));
    
    metric = "m.1.55e16-1.75e16";
    matcher = pattern.matcher(metric);
    assertTrue(matcher.find());
    assertEquals("1.55e16", matcher.group(1));
    assertEquals("1.75e16", matcher.group(2));
    
    metric = "m.1.55e-16-1.75e-16";
    matcher = pattern.matcher(metric);
    assertTrue(matcher.find());
    assertEquals("1.55e-16", matcher.group(1));
    assertEquals("1.75e-16", matcher.group(2));
    
    metric = "m.-1.55e-16_-1.75e-16";
    matcher = pattern.matcher(metric);
    assertTrue(matcher.find());
    assertEquals("-1.55e-16", matcher.group(1));
    assertEquals("-1.75e-16", matcher.group(2));
    
    // NOTE This is an error. Needs an underscore separator.
    metric = "m--0.25--1.75";
    matcher = pattern.matcher(metric);
    assertTrue(matcher.find());
    assertEquals("-0.25-", matcher.group(1));
    assertEquals("1.75", matcher.group(2));
  }
}
