package net.opentsdb.storage;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

/**
 * Generic test for {@link StoreDescriptor}s. Extend this class for tests of the implementations of
 * the {@link StoreDescriptor} class and set the {@link #storeDescriptor} variable in the before
 * block.
 */
public class StoreDescriptorTest {
  protected StoreDescriptor storeDescriptor;

  @Test
  public void testLabelIdSerializerNotNull() {
    assertNotNull(storeDescriptor.labelIdDeserializer());
  }

  @Test
  public void testLabelIdDeserializerNotNull() {
    assertNotNull(storeDescriptor.labelIdDeserializer());
  }
}