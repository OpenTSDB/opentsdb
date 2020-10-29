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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.junit.Test;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import net.opentsdb.common.Const;

public class TestParsing {
  
  @Test
  public void parseLong() throws Exception {
    byte[] buf = "foo-123445678blah".getBytes(Const.ASCII_US_CHARSET);
    long[] result = new long[1];
    assertTrue(Parsing.parseLong(buf, 3, 13, result));
    assertEquals(-123445678, result[0]);
    
    assertFalse(Parsing.parseLong(buf, 0, 13, result));
    
    buf = "0".getBytes(Const.ASCII_US_CHARSET);
    assertTrue(Parsing.parseLong(buf, 0, buf.length, result));
    assertEquals(0, result[0]);
  }
  
  @Test
  public void parseDouble() throws Exception {
    byte[] buf = "foo-1.234456e+78dblah".getBytes(Const.ASCII_US_CHARSET);
    double[] result = new double[1];
    assertTrue(Parsing.parseDouble(buf, 3, 17, result));
    assertEquals(-1.234456e78, result[0], 0.000001);
    
    assertFalse(Parsing.parseDouble(buf, 0, 17, result));
    
    buf = "42".getBytes(Const.ASCII_US_CHARSET);
    assertTrue(Parsing.parseDouble(buf, 0, buf.length, result));
    assertEquals(42, result[0], 0.000001);
    
    buf = "0".getBytes(Const.ASCII_US_CHARSET);
    assertTrue(Parsing.parseDouble(buf, 0, buf.length, result));
    assertEquals(0, result[0], 0.000001);
    
    buf = "0.0".getBytes(Const.ASCII_US_CHARSET);
    assertTrue(Parsing.parseDouble(buf, 0, buf.length, result));
    assertEquals(0.0, result[0], 0.000001);
    
    buf = "NaN".getBytes(Const.ASCII_US_CHARSET);
    assertTrue(Parsing.parseDouble(buf, 0, buf.length, result));
    assertTrue(Double.isNaN(result[0]));
    
    buf = "INFINITY".getBytes(Const.ASCII_US_CHARSET);
    assertTrue(Parsing.parseDouble(buf, 0, buf.length, result));
    assertTrue(Double.isInfinite(result[0]));
    
    buf = ".05".getBytes(Const.ASCII_US_CHARSET);
    assertTrue(Parsing.parseDouble(buf, 0, buf.length, result));
    assertEquals(0.05, result[0], 0.000001);
    
    buf = "err=.05 ".getBytes(Const.ASCII_US_CHARSET);
    assertTrue(Parsing.parseDouble(buf, 4, buf.length - 1, result));
    assertEquals(0.05, result[0], 0.000001);

    buf = "0.05".getBytes(Const.ASCII_US_CHARSET);
    assertTrue(Parsing.parseDouble(buf, 0, buf.length, result));
    assertEquals(0.05, result[0], 0.000001);
    
    buf = "12.899999999999636".getBytes(Const.ASCII_US_CHARSET);
    assertTrue(Parsing.parseDouble(buf, 0, buf.length, result));
    assertEquals(12.899999999999636, result[0], 0.0000000000000001);
  }
  
  @Test
  public void parseDoubleThreadTest() throws Exception {
    Random rnd = new Random(System.currentTimeMillis());
    double[] to_parse = new double[4096];
    for (int i = 0; i < to_parse.length; i++) {
      to_parse[i] = rnd.nextDouble() * rnd.nextInt();
    }
    ListeningExecutorService pool = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(16));
    class Job implements Runnable {
      final int i;
      
      Job(final int i) {
        this.i = i;
      }
      
      @Override
      public void run() {
        byte[] buf = Double.toString(to_parse[i]).getBytes(Const.ASCII_US_CHARSET);
        double[] output = new double[0];
        assertTrue(Parsing.parseDouble(buf, 0, buf.length, output));
        assertEquals(to_parse[i], output[0], 0.00001);
      }
    }
    
    CountDownLatch latch = new CountDownLatch(to_parse.length);
    for (int i = 0; i < to_parse.length; i++) {
      pool.submit(new Job(i)).addListener(new Runnable() {
        @Override
        public void run() {
          latch.countDown();
        }
      }, pool);
    }
    
    latch.await();
    pool.shutdown();
  }
  
  @Test
  public void parseDoubleRandom() {
    final Random rndm = new Random();
    double[] parsed = new double[1];
    for(int i = 0; i < 100_000; i++) {
      double[] doubles = new double[] {
        rndm.nextLong(),
        rndm.nextGaussian(),
        rndm.nextDouble() * Double.MAX_VALUE
      };
      for(int x = 0; x < doubles.length; x++) {
        double val = doubles[x];
        // TODO - it'd be better to randomly add junk before and after as well.
        String string = "myval=" + Double.toString(val) + "extra";
        assertTrue(Parsing.parseDouble(string.getBytes(), 6, string.length() - 5, parsed));
        assertEquals(doubles[x], parsed[0], 0.000000001);
      }
    }
  }
}