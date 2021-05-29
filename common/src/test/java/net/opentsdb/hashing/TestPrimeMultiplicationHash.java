// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.hashing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class TestPrimeMultiplicationHash {
  private HashFunction f = new PrimeMultiplicationHash();

  @Test
  public void testHashing() {

    HashSet<Long> collisionDetector = new HashSet<>();

    for (int i = 0; i < 1_000_000; i++) {
      ByteBuffer b = ByteBuffer.allocate(4);
      b.putInt(i);
      byte[] val = b.array();
      long hash = f.hash(val);
      assertEquals(hash, f.hash(val, 0, val.length));

      // Note: no collisions with the current prime value in the first 10M integers (coincidence)
      assertFalse(collisionDetector.contains(hash));
      collisionDetector.add(hash);
    }
  }

  @Test
  public void updateHash() {
    byte[] namespace = "test_namespace".getBytes();
    byte[] app = "test_application".getBytes();
    long nHash = f.hash(namespace);
    long naHash = f.update(nHash, app);
    long expected = f.update(nHash, app, 0, app.length);
    assertEquals(expected, naHash);
  }

  @Test
  public void testNull() {

    try {
      f.hash(null);
      fail("Should not hash a Null");
    } catch (NullPointerException expected) {
      // expected
    }
  }

  @Test
  public void updateTwoHashValuesIsNotSupported() {
    try {
      f.update(123, 345);
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) { }
  }

  @Test
  public void testEdgeCases() {
    assertEquals(0, f.hash(new byte[0]));
    assertEquals(0, f.hash(new byte[1]));
    assertEquals(1, f.hash(new byte[] {1}));
    assertEquals(12290, f.hash(new byte[] {1, 1}));
    assertEquals(-128, f.hash(new byte[] {-128}));
    assertEquals(127, f.hash(new byte[] {127}));
    assertEquals(235715800523260L, f.hash(new byte[] {127, 127, 127, 127}));
  }

}
