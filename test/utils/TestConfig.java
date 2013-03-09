package net.opentsdb.utils;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.util.HashMap;

import org.junit.Test;

public final class TestConfig {

  @Test
  public void constructor() throws Exception {
    assertNotNull(new Config(false));
  }
  
  @Test
  public void constructorDefault() throws Exception {
    assertTrue(new Config(false).getString("tsd.network.bind").equals("0.0.0.0"));
  }
  
  @Test
  public void constructorChild() throws Exception {
    Config c = new Config(false);
    assertNotNull(c);
    assertNotNull(new Config(c));
  }
  
  @Test
  public void constructorChildCopy() throws Exception {
    Config c = new Config(false);
    assertNotNull(c);
    c.overrideConfig("MyProp", "Parent");
    Config ch = new Config(c);
    assertNotNull(ch);
    ch.overrideConfig("MyProp", "Child");
    assertTrue(c.getString("MyProp").equals("Parent"));
    assertTrue(ch.getString("MyProp").equals("Child"));
  }
  
  @Test (expected = FileNotFoundException.class)
  public void loadConfigNotFound() throws Exception {
    Config c = new Config(false);
    c.loadConfig("/tmp/filedoesnotexist.conf");
  }
  
  @Test
  public void overrideConfig() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.core.bind", "127.0.0.1");
    assertTrue(c.getString("tsd.core.bind").equals("127.0.0.1"));
  }
  
  @Test 
  public void getString() throws Exception {
    assertTrue(new Config(false).getString("tsd.storage.flush_interval").equals("1000"));
  }
  
  @Test (expected = NullPointerException.class)
  public void getStringNull() throws Exception {
    assertTrue(new Config(false).getString("tsd.blarg").equals("1000"));
  }
  
  @Test
  public void getInt() throws Exception {
    assertTrue(new Config(false).getInt("tsd.storage.flush_interval") == 1000);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getIntNull() throws Exception {
    assertTrue(new Config(false).getInt("tsd.blarg") == 1000);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getIntNFE() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.blarg", "this can't be parsed to int");
    assertTrue(c.getInt("tsd.blarg") == 1000);
  }
  
  @Test
  public void getShort() throws Exception {
    assertTrue(new Config(false).getShort("tsd.storage.flush_interval") == 1000);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getShortNull() throws Exception {
    assertTrue(new Config(false).getShort("tsd.blarg") == 1000);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getShortNFE() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.blarg", "this can't be parsed to short");
    assertTrue(c.getShort("tsd.blarg") == 1000);
  }
  
  @Test
  public void getLong() throws Exception {
    assertTrue(new Config(false).getLong("tsd.storage.flush_interval") == 1000);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getLongNull() throws Exception {
    assertTrue(new Config(false).getLong("tsd.blarg") == 1000);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getLongNullNFE() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.blarg", "this can't be parsed to long");
    assertTrue(c.getLong("tsd.blarg") == 1000);
  }
  
  @Test
  public void getFloat() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "42.5");
    assertTrue(c.getFloat("tsd.unitest") == 42.5);
  }
  
  @Test (expected = NullPointerException.class)
  public void getFloatNull() throws Exception {
    assertTrue(new Config(false).getFloat("tsd.blarg") == 42.5);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getFloatNFE() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "this can't be parsed to float");
    assertTrue(c.getFloat("tsd.unitest") == 42.5);
  }
  
  @Test
  public void getDouble() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "42.5");
    assertTrue(c.getDouble("tsd.unitest") == 42.5);
  }
  
  @Test (expected = NullPointerException.class)
  public void getDoubleNull() throws Exception {
    assertTrue(new Config(false).getDouble("tsd.blarg") == 42.5);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getDoubleNFE() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "this can't be parsed to double");
    assertTrue(c.getDouble("tsd.unitest") == 42.5);
  }
  
  @Test
  public void getBool1() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "1");
    assertTrue(c.getBoolean("tsd.unitest"));
  }
  
  @Test
  public void getBoolTrue1() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "True");
    assertTrue(c.getBoolean("tsd.unitest"));
  }
  
  @Test
  public void getBoolTrue2() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "true");
    assertTrue(c.getBoolean("tsd.unitest"));
  }
  
  @Test
  public void getBoolYes() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "yes");
    assertTrue(c.getBoolean("tsd.unitest"));
  }
  
  @Test
  public void getBoolFalseEmpty() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "");
    assertFalse(c.getBoolean("tsd.unitest"));
  }
  
  @Test (expected = NullPointerException.class)
  public void getBoolFalseNull() throws Exception {
    Config c = new Config(false);
    assertFalse(c.getBoolean("tsd.unitest"));
  }
  
  @Test
  public void getBoolFalseOther() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "blarg");
    assertFalse(c.getBoolean("tsd.unitest"));
  }
}
