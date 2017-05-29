// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Map;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.opentsdb.core.HistogramDataPoint.HistogramBucket;
import net.opentsdb.core.HistogramDataPoint.HistogramBucket.BucketType;

public class TestSimpleHistogram {
  
  @Test
  public void verifyE2EKryo() {
    Kryo kryo = new Kryo();

    //Encoding stage
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    Output output = new Output(outBuffer);
    output.writeByte(0 /* This is the type of histogram (or sketch) written 
    // to storage. HistoType.SimpleHistogramType.ordinal()*/);
    output.writeShort(8);
    output.writeFloat(1.0f);
    output.writeFloat(2.0f);
    output.writeLong(5, true);
    output.writeFloat(2.0f);
    output.writeFloat(3.0f);
    output.writeLong(5, true);
    output.writeFloat(3.0f);
    output.writeFloat(4.0f);
    output.writeLong(5, true);
    output.writeFloat(4.0f);
    output.writeFloat(5.0f);
    output.writeLong(0, true);
    output.writeFloat(5.0f);
    output.writeFloat(6.0f);
    output.writeLong(0, true);
    output.writeFloat(6.0f);
    output.writeFloat(7.0f);
    output.writeLong(0, true);
    output.writeFloat(7.0f);
    output.writeFloat(8.0f);
    output.writeLong(0, true);
    output.writeFloat(8.0f);
    output.writeFloat(9.0f);
    output.writeLong(0, true);
    output.writeLong(0, true);
    output.writeLong(2, true);
    output.close();
    
    //Decoding stage
    Input input = new Input(new ByteArrayInputStream(outBuffer.toByteArray()));
    int metricType = input.readByte();

    switch (metricType) {
      case 0:
        SimpleHistogram y1Hist = new SimpleHistogram();
        y1Hist.read(kryo, input);
        Input verifyHist = new Input(new ByteArrayInputStream(y1Hist.histogram()));

        int bucketCount = verifyHist.readShort();
        assertEquals(bucketCount, 8);

        Float bucketLowerBound = verifyHist.readFloat();
        Float bucketUpperBound = verifyHist.readFloat();
        long bucketVal = verifyHist.readLong(true);
        assertEquals(bucketLowerBound, 1.0f, 0.0001);
        assertEquals(bucketVal, 5);
        assertEquals(y1Hist.getOverflow(), Long.valueOf(2L));
        break;
      default:
        System.out.println("Failed to detect histogram type");
        assertTrue(false);
    }
    input.close();
  }

  @Test
  public void testHistogramSerialization() {
    Kryo kryo = new Kryo();
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    Output output = new Output(outBuffer);

    SimpleHistogram y1Hist = new SimpleHistogram();
    y1Hist.addBucket(1.0f, 2.0f, 5L);
    y1Hist.addBucket(2.0f, 3.0f, 5L);
    y1Hist.addBucket(3.0f, 10.0f, 0L);
    y1Hist.write(kryo, output);
    output.close();

    SimpleHistogram y1HistVerify = new SimpleHistogram();
    y1HistVerify.fromHistogram(outBuffer.toByteArray());

    Input input = new Input(new ByteArrayInputStream(y1HistVerify.histogram()));
    int bucketCount = input.readShort();
    assertEquals(bucketCount, 3);

    Float bucketLB = input.readFloat();
    Float bucketUB = input.readFloat();
    long bucketVal = input.readLong(true);
    assertEquals(bucketLB, 1.0f, 0.001);
    assertEquals(bucketVal, 5);
    input.close();
  }

  @Test
  public void testIncompletByteArray() {
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    Output output = new Output(outBuffer);
    output.writeShort(4);
    output.close();

    SimpleHistogram y1Hist = new SimpleHistogram();
    boolean exceptionCaught = false;
    try {
      y1Hist.fromHistogram(outBuffer.toByteArray());
    }
    catch(Exception e) {
      exceptionCaught = true;
    }

    assertFalse(exceptionCaught);
  }

  @Test
  public void testInvalidHistogramLength() {
    Kryo kryo = new Kryo();
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    Output output = new Output(outBuffer);
    output.writeShort(1);
    output.writeInt(-1);
    output.close();
    Input input = new Input(new ByteArrayInputStream(outBuffer.toByteArray()));
    Integer metricType = input.readInt();

    SimpleHistogram y1Hist = new SimpleHistogram();
    boolean exceptionCaught = false;
    try {
      y1Hist.read(kryo, input);
    }
    catch(Exception e) {
      exceptionCaught = true;
    }

    assertFalse(exceptionCaught);
  }

  @Test
  public void testSinglePercentile() {
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    Output output = new Output(outBuffer);
    output.writeShort(3);
    output.writeFloat(1.0f);
    output.writeFloat(6.0f);
    output.writeLong(5, true);
    output.writeFloat(6.0f);
    output.writeFloat(10.0f);
    output.writeLong(10, true);
    output.writeFloat(10.0f);
    output.writeFloat(20.0f);
    output.writeLong(1, true);
    output.writeLong(0, true);
    output.writeLong(5, true);
    output.close();

    SimpleHistogram y1Hist = new SimpleHistogram();
    y1Hist.fromHistogram(outBuffer.toByteArray());
    double perc50 = y1Hist.percentile(50.0f);

    assertEquals(perc50, 8.0f, 0.0001);
    assertEquals(y1Hist.percentile(1000.0f), -1.0f, 0.0001);
  }

  @Test
  public void testPercentileList() {
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    Output output = new Output(outBuffer);
    output.writeShort(4);
    output.writeFloat(1.0f);
    output.writeFloat(6.0f);
    output.writeLong(5, true);
    output.writeFloat(6.0f);
    output.writeFloat(10.0f);
    output.writeLong(10, true);
    output.writeFloat(10.0f);
    output.writeFloat(20.0f);
    output.writeLong(1, true);
    output.writeFloat(20.0f);
    output.writeFloat(40.0f);
    output.writeLong(0, true);
    output.writeLong(0, true);
    output.writeLong(5, true);
    output.close();

    SimpleHistogram y1Hist = new SimpleHistogram();
    y1Hist.fromHistogram(outBuffer.toByteArray());
    ArrayList<Double> percs = new ArrayList<Double>();
    percs.add(50.0);
    percs.add(99.0);
    ArrayList<Double> percValues = (ArrayList<Double>) y1Hist.percentiles(percs);
    double perc50 = percValues.get(0);
    double perc99 = percValues.get(1);

    assertEquals(perc50, 8.0, 0.001);
    assertEquals(perc99, 15.0, 0.001);
  }

  @Test
  public void testSingleHistogramMerge() {
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    Output output = new Output(outBuffer);
    output.writeShort(4);
    output.writeFloat(1.0f);
    output.writeFloat(6.0f);
    output.writeLong(5, true);
    output.writeFloat(6.0f);
    output.writeFloat(10.0f);
    output.writeLong(10, true);
    output.writeFloat(10.0f);
    output.writeFloat(20.0f);
    output.writeLong(1, true);
    output.writeFloat(20.0f);
    output.writeFloat(40.0f);
    output.writeLong(0, true);
    output.writeLong(0, true);
    output.writeLong(5, true);
    output.close();

    SimpleHistogram y1Hist = new SimpleHistogram();
    y1Hist.fromHistogram(outBuffer.toByteArray());

    SimpleHistogram y1Hist1 = new SimpleHistogram();
    y1Hist1.fromHistogram(outBuffer.toByteArray());
    y1Hist1.setUnderflow(2L);

    y1Hist.aggregate(y1Hist1, HistogramAggregation.SUM);
    assertEquals(y1Hist.getBucketCount(1.0f, 6.0f), Long.valueOf(10L));
    assertEquals(y1Hist.getBucketCount(6.0f, 10.0f), Long.valueOf(20L));
    assertEquals(y1Hist.getBucketCount(10.0f, 20.0f), Long.valueOf(2L));
    assertEquals(y1Hist.getOverflow(), Long.valueOf(10L));
    assertEquals(y1Hist.getUnderflow(), Long.valueOf(2L));
  }

  @Test
  public void testMultipleHistogramMerge() {
    ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    Output output = new Output(outBuffer);
    output.writeShort(4);
    output.writeFloat(1.0f);
    output.writeFloat(6.0f);
    output.writeLong(5, true);
    output.writeFloat(6.0f);
    output.writeFloat(10.0f);
    output.writeLong(10, true);
    output.writeFloat(10.0f);
    output.writeFloat(20.0f);
    output.writeLong(1, true);
    output.writeFloat(20.0f);
    output.writeFloat(40.0f);
    output.writeLong(0, true);
    output.writeLong(0, true);
    output.writeLong(5, true);
    output.close();

    SimpleHistogram y1Hist = new SimpleHistogram();
    y1Hist.fromHistogram(outBuffer.toByteArray());

    ArrayList<Histogram> histos = new ArrayList<Histogram>();
    SimpleHistogram y1Hist1 = new SimpleHistogram();
    y1Hist1.fromHistogram(outBuffer.toByteArray());
    histos.add(y1Hist1);
    SimpleHistogram y1Hist2 = new SimpleHistogram();
    y1Hist2.fromHistogram(outBuffer.toByteArray());
    histos.add(y1Hist2);

    y1Hist.aggregate(histos, HistogramAggregation.SUM);
    assertEquals(y1Hist.getBucketCount(1.0f, 6.0f), Long.valueOf(15L));
    assertEquals(y1Hist.getBucketCount(6.0f, 10.0f), Long.valueOf(30L));
    assertEquals(y1Hist.getBucketCount(10.0f, 20.0f), Long.valueOf(3L));
    assertEquals(y1Hist.getOverflow(), Long.valueOf(15L));
  }

  @Test
  public void testArbitraryBuckets() {
      ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
      Output output = new Output(outBuffer);
      output.writeShort(4);
      output.writeFloat(1.0f);
      output.writeFloat(6.0f);
      output.writeLong(5, true);
      output.writeFloat(6.0f);
      output.writeFloat(10.0f);
      output.writeLong(10, true);
      output.writeFloat(10.0f);
      output.writeFloat(20.0f);
      output.writeLong(1, true);
      output.writeFloat(20.0f);
      output.writeFloat(40.0f);
      output.writeLong(0, true);
      output.writeLong(0, true);
      output.writeLong(5, true);
      output.close();

      SimpleHistogram y1Hist = new SimpleHistogram();
      y1Hist.fromHistogram(outBuffer.toByteArray());

      assertEquals(y1Hist.getBucketCount(6.0f, 10.0f), Long.valueOf(10L));
      assertEquals(y1Hist.getBucketCount(5.0f, 6.0f), Long.valueOf(0L));
      assertEquals(y1Hist.getBucketCount(5.0f, 12.0f), Long.valueOf(0L));
      assertEquals(y1Hist.getBucketCount(1.0f, 10.0f), Long.valueOf(0L));
  }

  @Test
  public void testAddBuckets() {
    SimpleHistogram y1Hist = new SimpleHistogram();

    y1Hist.addBucket(5.0f, 7.0f, 3L);
    assertEquals(y1Hist.getBucketCount(5.0f, 7.0f), Long.valueOf(3L));;

    y1Hist.addBucket(5.0f, 7.0f, 5L);
    assertEquals(y1Hist.getBucketCount(5.0f, 7.0f), Long.valueOf(5L));
  }

  @Test
  public void testNullBucketVal() {
    SimpleHistogram y1Hist = new SimpleHistogram();

    y1Hist.addBucket(5.0f, 7.0f, null);
    assertEquals(y1Hist.getBucketCount(5.0f, 7.0f), Long.valueOf(0L));

    y1Hist.addBucket(5.0f, 7.0f, 5L);
  }
  
  @Test
  public void testJsonSerialization() {
    SimpleHistogram y1Hist = new SimpleHistogram();

    y1Hist.addBucket(5.0f, 7.0f, 3L);
    y1Hist.addBucket(7.0f, 10.0f, 5L);
    y1Hist.setOverflow(1L);

    String jsonOut = new String();
    ObjectMapper mapper = new ObjectMapper();
    try {
        StringWriter output = new StringWriter();
        mapper.writeValue(output, y1Hist);
        jsonOut = output.toString();
    } catch (IOException e) {
        e.printStackTrace();
    }
    assertTrue(jsonOut.contains("\"buckets\":{"));
    assertTrue(jsonOut.contains("\"5.0-7.0\":3"));
    assertTrue(jsonOut.contains("\"7.0-10.0\":5"));
    assertTrue(jsonOut.contains("\"underflow\":0"));
    assertTrue(jsonOut.contains("\"overflow\":1"));
    
    SimpleHistogram y1Hist1 = new SimpleHistogram();

    y1Hist1.addBucket(Float.NEGATIVE_INFINITY, 5.0f, 3L);
    y1Hist1.addBucket(7.0f, 10.0f, 5L);
    y1Hist1.addBucket(10.0f, null, 1L);
    try {
        StringWriter output = new StringWriter();
        mapper.writeValue(output, y1Hist1);
        jsonOut = output.toString();
    } catch (IOException e) {
        e.printStackTrace();
    }
    assertTrue(jsonOut.contains("\"buckets\":{"));
    assertTrue(jsonOut.contains("\"-Infinity-5.0\":3"));
    assertTrue(jsonOut.contains("\"7.0-10.0\":5"));
    assertTrue(jsonOut.contains("\"underflow\":0"));
    assertTrue(jsonOut.contains("\"overflow\":0"));
  }

  @Test
  public void testImmutableGetHistogram() {
    SimpleHistogram y1Hist = new SimpleHistogram();

    y1Hist.addBucket(5.0f, 7.0f, 3L);
    y1Hist.addBucket(7.0f, 10.0f, 5L);
    y1Hist.addBucket(10.0f, null, 1L);

    Map<HistogramBucket, Long> histMap = y1Hist.getHistogram();
    boolean histModBlocked = false;
    try {
      histMap.put(new HistogramBucket(BucketType.REGULAR, 1.0f, 5.0f), 3L);
    } catch (UnsupportedOperationException e) {
      histModBlocked = true;
    }

    assertTrue(histModBlocked);
  }

  @Test
  public void testMissingBucketsHistogramAggregation() {
    SimpleHistogram hist1 = new SimpleHistogram();
    hist1.addBucket(5.0f, 7.0f, 3L);
    hist1.addBucket(7.0f, 10.0f, 5L);
    hist1.addBucket(15.0f, 20.0f, 2L);

    SimpleHistogram hist2 = new SimpleHistogram();
    hist2.addBucket(5.0f, 7.0f, 3L);
    hist2.addBucket(7.0f, 10.0f, 5L);
    hist2.addBucket(10.0f, 15.0f, 2L);
    hist2.addBucket(15.0f, 20.0f, 1L);

    hist1.aggregate(hist2, HistogramAggregation.SUM);
    assertEquals(hist1.getBucketCount(10.0f, 15.0f).longValue(), 2L);
  }
  
  @Test
  public void testInit() {
  double[] dynaHist = SimpleHistogram.initializeHistogram(1.0f, 6000.0f, 
      100.0f, 2000.0f, 0.05f);
    // TODO - proper bucket testing
//    System.out.println("No. of buckets: " + dynaHist.length);
//    for (int i = 0; i < dynaHist.length; i++) {
//        System.out.print(dynaHist[i] + ", ");
//    }
//    System.out.println();
    assertEquals(33, dynaHist.length);
  }

  @Test
  public void testWholeRangeFocus() {
    double[] dynaHist = SimpleHistogram.initializeHistogram(100.0f, 2000.0f, 
        100.0f, 2000.0f, 0.05f);
      // TODO - proper bucket testing
//      System.out.println("No. of buckets: " + dynaHist.length);
//      for (int i = 0; i < dynaHist.length; i++) {
//          System.out.print(dynaHist[i] + ", ");
//      }
//      System.out.println();
    assertEquals(31, dynaHist.length);
  }

  @Test
  public void testStartEqualtoFocusStart() {
    double[] dynaHist = SimpleHistogram.initializeHistogram(100.0f, 6000.0f, 
        100.0f, 2000.0f, 0.05f);
    // TODO - proper bucket testing
//      System.out.println("No. of buckets: " + dynaHist.length);
//      for (int i = 0; i < dynaHist.length; i++) {
//          System.out.print(dynaHist[i] + ", ");
//      }
//      System.out.println();
    assertEquals(32, dynaHist.length);
  }

  @Test
  public void testEndEqualtoFocusEnd() {
    double[] dynaHist = SimpleHistogram.initializeHistogram(1.0f, 2000.0f, 
        100.0f, 2000.0f, 0.05f);
   // TODO - proper bucket testing
//      System.out.println("No. of buckets: " + dynaHist.length);
//      for (int i = 0; i < dynaHist.length; i++) {
//          System.out.print(dynaHist[i] + ", ");
//      }      
    assertEquals(32, dynaHist.length);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testErrorStartGreaterThanEnd() {
    SimpleHistogram.initializeHistogram(10000.0f, 6000.0f, 100.0f, 2000.0f, 0.05f);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testErrorFocusStartGreaterThanFocusEnd() {
    SimpleHistogram.initializeHistogram(1.0f, 6000.0f, 3000.0f, 2000.0f, 0.05f);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testErrorRateLessThanZero() {
    SimpleHistogram.initializeHistogram(1.0f, 6000.0f, 100.0f, 2000.0f, -0.05f);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFocusEndGreaterThanEnd() {
    SimpleHistogram.initializeHistogram(1.0f, 1000.0f, 100.0f, 2000.0f, 0.05f);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFocusStartLessThanStart() {
    SimpleHistogram.initializeHistogram(200.0f, 100.0f, 1500.0f, 2000.0f, 0.05f);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testExcessiveBuckets() {
    SimpleHistogram.initializeHistogram(100.0f, 6000.0f, 100.0f, 6000.0f, 0.01f);
  }
}