package net.opentsdb.storage.cassandra;

import net.opentsdb.storage.StoreDescriptorTest;

import org.junit.Before;

public class CassandraStoreDescriptorTest extends StoreDescriptorTest {
  @Before
  public void setUp() throws Exception {
    storeDescriptor = new CassandraStoreDescriptor();
  }
}
