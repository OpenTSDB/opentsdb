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
package net.opentsdb.stats;

import junit.framework.TestCase;

public final class TestHistogram extends TestCase {

  public void test_percentile_empty_histogram() {
    final Histogram histo = new Histogram(16000, (short) 2, 100);
    assertEquals(0, histo.percentile(1));
    assertEquals(0, histo.percentile(50));
    assertEquals(0, histo.percentile(99));
  }

  public void test_16Max_1Interval_5Cutoff() {
    final Histogram histo = new Histogram(16, (short) 1, 5);
    assertEquals(10, histo.buckets());

    histo.add(4);
    assertBucketEquals(histo, 4, 1);

    histo.add(5);
    assertBucketEquals(histo, 5, 1);

    histo.add(5);
    assertBucketEquals(histo, 5, 2);

    histo.add(0);
    assertBucketEquals(histo, 0, 1);

    histo.add(42);
    assertBucketEquals(histo, 9, 1);

    histo.add(6);
    assertBucketEquals(histo, 5, 3);

    histo.add(9);
    assertBucketEquals(histo, 7, 1);

    histo.add(10);
    assertBucketEquals(histo, 7, 2);

    assertBucketEquals(histo, 0, 1);
    assertBucketEquals(histo, 1, 0);
    assertBucketEquals(histo, 2, 0);
    assertBucketEquals(histo, 3, 0);
    assertBucketEquals(histo, 4, 1);
    assertBucketEquals(histo, 5, 3);
    assertBucketEquals(histo, 6, 0);
    assertBucketEquals(histo, 7, 2);
    assertBucketEquals(histo, 8, 0);
    assertBucketEquals(histo, 9, 1);
  }

  public void test_16Max_2Interval_5Cutoff() {
    final Histogram histo = new Histogram(16, (short) 2, 5);
    assertEquals(6, histo.buckets());

    histo.add(4);
    assertBucketEquals(histo, 2, 1);

    histo.add(6);
    assertBucketEquals(histo, 2, 2);

    histo.add(7);
    assertBucketEquals(histo, 2, 3);

    histo.add(0);
    assertBucketEquals(histo, 0, 1);

    histo.add(42);
    assertBucketEquals(histo, 5, 1);

    histo.add(8);
    assertBucketEquals(histo, 3, 1);

    histo.add(9);
    assertBucketEquals(histo, 3, 2);

    histo.add(10);
    assertBucketEquals(histo, 3, 3);

    histo.add(11);
    assertBucketEquals(histo, 3, 4);

    histo.add(12);
    assertBucketEquals(histo, 5, 1);

    assertBucketEquals(histo, 0, 1);
    assertBucketEquals(histo, 1, 0);
    assertBucketEquals(histo, 2, 3);
    assertBucketEquals(histo, 3, 4);
    assertBucketEquals(histo, 4, 1);
    assertBucketEquals(histo, 5, 1);
  }

  public void test_160Max_20Interval_50Cutoff() {
    final Histogram histo = new Histogram(160, (short) 20, 50);
    assertEquals(6, histo.buckets());

    histo.add(0);
    assertBucketEquals(histo, 0, 1);

    histo.add(40);
    assertBucketEquals(histo, 2, 1);

    histo.add(50);
    assertBucketEquals(histo, 2, 2);

    histo.add(60);
    assertBucketEquals(histo, 2, 3);

    histo.add(71);
    assertBucketEquals(histo, 2, 4);

    histo.add(72);
    assertBucketEquals(histo, 3, 1);

    histo.add(103);
    assertBucketEquals(histo, 3, 2);

    histo.add(104);
    assertBucketEquals(histo, 4, 1);

    histo.add(130);
    assertBucketEquals(histo, 4, 2);

    histo.add(160);
    assertBucketEquals(histo, 4, 3);

    histo.add(167);
    assertBucketEquals(histo, 4, 4);

    histo.add(168);
    assertBucketEquals(histo, 5, 1);

    histo.add(420);
    assertBucketEquals(histo, 5, 2);

    assertBucketEquals(histo, 0, 1);
    assertBucketEquals(histo, 1, 0);
    assertBucketEquals(histo, 2, 4);
    assertBucketEquals(histo, 3, 2);
    assertBucketEquals(histo, 4, 4);
    assertBucketEquals(histo, 5, 2);
  }

  static void assertBucketEquals(final Histogram histo,
                                 final int bucket, final int expected) {
    int actual = histo.valueInBucket(bucket);
    if (actual != expected) {
      final StringBuilder buf = new StringBuilder();
      final int nbuckets = histo.buckets();
      for (int i = 0; i < nbuckets; i++) {
        histo.printAsciiBucket(buf, i);
        if (i == bucket) {
          buf.setCharAt(buf.length() - 1, ' ');
          buf.append(" <=== should have been ").append(expected).append('\n');
        }
      }
      fail("Bucket #" + bucket + " contains " + actual + " instead of "
           + expected + "\nHistogram:\n" + buf);
    }
  }

  static void printHisto(final Histogram histo) {
    final StringBuilder buf = new StringBuilder();
    histo.printAscii(buf);
    System.err.println(buf);
  }

}
