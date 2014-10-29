package net.opentsdb.core;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class RowKeyTest {
  @Test
  public void getTSUIDFromKey() {
    final byte[] tsuid = RowKey.tsuid(
            new byte[] {0, 0, 1, 1, 1, 1, 1, 0, 0, 2, 0, 0, 3});
    assertArrayEquals(new byte[] { 0, 0, 1, 0, 0, 2, 0, 0, 3 }, tsuid);
  }

  @Test
  public void getTSUIDFromKeyMissingTags() {
    final byte[] tsuid = RowKey.tsuid(
            new byte[] {0, 0, 1, 1, 1, 1, 1});
    assertArrayEquals(new byte[] { 0, 0, 1 }, tsuid);
  }
}