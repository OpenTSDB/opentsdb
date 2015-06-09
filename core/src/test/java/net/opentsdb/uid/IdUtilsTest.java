package net.opentsdb.uid;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import java.util.List;

public class IdUtilsTest {
  @Test
  public void uidToString() {
    assertEquals("01", IdUtils.uidToString(new byte[]{1}));
  }

  @Test
  public void uidToString2() {
    assertEquals("0A0B", IdUtils.uidToString(new byte[]{10, 11}));
  }

  @Test
  public void uidToString3() {
    assertEquals("1A1B", IdUtils.uidToString(new byte[]{26, 27}));
  }

  @Test
  public void uidToStringZeros() {
    assertEquals("00", IdUtils.uidToString(new byte[]{0}));
  }

  @Test
  public void uidToString255() {
    assertEquals("FF", IdUtils.uidToString(new byte[]{(byte) 255}));
  }

  @Test(expected = NullPointerException.class)
  public void uidToStringNull() {
    IdUtils.uidToString(null);
  }

  @Test
  public void stringToUid() {
    assertArrayEquals(new byte[]{0x0a, 0x0b}, IdUtils.stringToUid("0A0B"));
  }

  @Test
  public void stringToUidNormalize() {
    assertArrayEquals(new byte[]{(byte) 171}, IdUtils.stringToUid("AB"));
  }

  @Test
  public void stringToUidCase() {
    assertArrayEquals(new byte[]{(byte) 11}, IdUtils.stringToUid("B"));
  }

  @Test
  public void stringToUidWidth() {
    assertArrayEquals(new byte[]{(byte) 0, (byte) 42, (byte) 12},
        IdUtils.stringToUid("2A0C", (short) 3));
  }

  @Test
  public void stringToUidWidth2() {
    assertArrayEquals(new byte[]{(byte) 0, (byte) 0, (byte) 0},
        IdUtils.stringToUid("0", (short) 3));
  }

  @Test(expected = IllegalArgumentException.class)
  public void stringToUidNull() {
    IdUtils.stringToUid(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void stringToUidEmpty() {
    IdUtils.stringToUid("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void stringToUidNotHex() {
    IdUtils.stringToUid("HelloWorld");
  }

  @Test(expected = IllegalArgumentException.class)
  public void stringToUidNotHex2() {
    IdUtils.stringToUid(" ");
  }

  @Test
  public void getTagPairsFromTSUIDBytes() {
    List<byte[]> tags = IdUtils.getTagPairsFromTSUID(
        new byte[]{0, 0, 0, 0, 0, 1, 0, 0, 2, 0, 0, 3, 0, 0, 4});
    assertNotNull(tags);
    assertEquals(2, tags.size());
    assertArrayEquals(new byte[]{0, 0, 1, 0, 0, 2}, tags.get(0));
    assertArrayEquals(new byte[]{0, 0, 3, 0, 0, 4}, tags.get(1));
  }


  @Test(expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesNonStandardWidth() {
    List<byte[]> tags = IdUtils.getTagPairsFromTSUID(
        new byte[]{0, 0, 0, 0, 0, 0, 1, 0, 0, 2, 0, 0, 0, 3, 0, 0, 4});
    assertNotNull(tags);
    assertEquals(2, tags.size());
    assertArrayEquals(new byte[]{0, 0, 0, 1, 0, 0, 2}, tags.get(0));
    assertArrayEquals(new byte[]{0, 0, 0, 3, 0, 0, 4}, tags.get(1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesMissingTags() {
    IdUtils.getTagPairsFromTSUID(new byte[]{0, 0, 1});
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesMissingMetric() {
    IdUtils.getTagPairsFromTSUID(new byte[]{0, 0, 1, 0, 0, 2});
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesMissingTagv() {
    IdUtils.getTagPairsFromTSUID(new byte[]{0, 0, 8, 0, 0, 2});
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesNull() {
    IdUtils.getTagPairsFromTSUID((byte[]) null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagPairsFromTSUIDBytesEmpty() {
    IdUtils.getTagPairsFromTSUID(new byte[0]);
  }

  @Test
  public void getTagFromTSUID() {
    List<byte[]> tags = IdUtils.getTagsFromTSUID(
        "000000000001000002000003000004");
    assertNotNull(tags);
    assertEquals(4, tags.size());
    assertArrayEquals(new byte[]{0, 0, 1}, tags.get(0));
    assertArrayEquals(new byte[]{0, 0, 2}, tags.get(1));
    assertArrayEquals(new byte[]{0, 0, 3}, tags.get(2));
    assertArrayEquals(new byte[]{0, 0, 4}, tags.get(3));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagFromTSUIDNonStandardWidth() {
    List<byte[]> tags = IdUtils.getTagsFromTSUID(
        "0000000000000100000200000003000004");
    assertNotNull(tags);
    assertEquals(4, tags.size());
    assertArrayEquals(new byte[]{0, 0, 0, 1}, tags.get(0));
    assertArrayEquals(new byte[]{0, 0, 2}, tags.get(1));
    assertArrayEquals(new byte[]{0, 0, 0, 3}, tags.get(2));
    assertArrayEquals(new byte[]{0, 0, 4}, tags.get(3));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagFromTSUIDMissingTags() {
    IdUtils.getTagsFromTSUID("123456");
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagFromTSUIDMissingMetric() {
    IdUtils.getTagsFromTSUID("000001000002");
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagFromTSUIDOddNumberOfCharacters() {
    IdUtils.getTagsFromTSUID("0000080000010000020");
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagFromTSUIDMissingTagv() {
    IdUtils.getTagsFromTSUID("000008000001");
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagFromTSUIDNull() {
    IdUtils.getTagsFromTSUID(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void getTagFromTSUIDEmpty() {
    IdUtils.getTagsFromTSUID("");
  }

  @Test
  public void uidToLong() {
    assertEquals(42, IdUtils.uidToLong(new byte[]{0, 0, 0x2A}, (short) 3));
  }

  @Test(expected = IllegalArgumentException.class)
  public void uidToLongTooLong() throws Exception {
    IdUtils.uidToLong(new byte[]{0, 0, 0, 0x2A}, (short) 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void uidToLongTooShort() throws Exception {
    IdUtils.uidToLong(new byte[]{0, 0x2A}, (short) 3);
  }

  @Test(expected = NullPointerException.class)
  public void uidToLongNull() throws Exception {
    IdUtils.uidToLong(null, (short) 3);
  }

  @Test
  public void longToUID() throws Exception {
    assertArrayEquals(new byte[]{0, 0, 0x2A},
        IdUtils.longToUID(42L, (short) 3));
  }

  @Test(expected = IllegalStateException.class)
  public void longToUIDTooBig() throws Exception {
    IdUtils.longToUID(257, (short) 1);
  }
}