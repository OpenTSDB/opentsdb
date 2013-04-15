// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;

import org.junit.Before;
import org.junit.Test;

public final class TestConfig {
  private Config config;
  
  @Before
  public void before() throws Exception {
    config = new Config(false);
  }
  
  @Test
  public void constructor() throws Exception {
    assertNotNull(new Config(false));
  }
  
  @Test
  public void constructorDefault() throws Exception {
    assertEquals("0.0.0.0", config.getString("tsd.network.bind"));
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
    assertEquals("Parent", c.getString("MyProp"));
    assertEquals("Child", ch.getString("MyProp"));
  }
  
  @Test (expected = FileNotFoundException.class)
  public void loadConfigNotFound() throws Exception {
    Config c = new Config(false);
    c.loadConfig("/tmp/filedoesnotexist.conf");
  }
  
  @Test
  public void overrideConfig() throws Exception {
    config.overrideConfig("tsd.core.bind", "127.0.0.1");
    assertEquals("127.0.0.1", config.getString("tsd.core.bind"));
  }
  
  @Test 
  public void getString() throws Exception {
    assertEquals("1000", config.getString("tsd.storage.flush_interval"));
  }
  
  @Test
  public void getStringNull() throws Exception {
    assertNull(config.getString("tsd.blarg"));
  }
  
  @Test
  public void getInt() throws Exception {
    assertEquals(1000, config.getInt("tsd.storage.flush_interval"));
  }
  
  @Test (expected = NumberFormatException.class)
  public void getIntNull() throws Exception {
    config.getInt("tsd.blarg");
  }
  
  @Test (expected = NumberFormatException.class)
  public void getIntNFE() throws Exception {
    config.overrideConfig("tsd.blarg", "this can't be parsed to int");
    config.getInt("tsd.blarg");
  }
  
  @Test
  public void getShort() throws Exception {
    assertEquals(1000, config.getShort("tsd.storage.flush_interval"));
  }
  
  @Test (expected = NumberFormatException.class)
  public void getShortNull() throws Exception {
    assertEquals(1000, config.getShort("tsd.blarg"));
  }
  
  @Test (expected = NumberFormatException.class)
  public void getShortNFE() throws Exception {
    config.overrideConfig("tsd.blarg", "this can't be parsed to short");
    config.getShort("tsd.blarg");
  }
  
  @Test
  public void getLong() throws Exception {
    assertEquals(1000, config.getLong("tsd.storage.flush_interval"));
  }
  
  @Test (expected = NumberFormatException.class)
  public void getLongNull() throws Exception {
    config.getLong("tsd.blarg");
  }
  
  @Test (expected = NumberFormatException.class)
  public void getLongNullNFE() throws Exception {
    config.overrideConfig("tsd.blarg", "this can't be parsed to long");
    config.getLong("tsd.blarg");
  }
  
  @Test
  public void getFloat() throws Exception {
    config.overrideConfig("tsd.unitest", "42.5");
    assertEquals(42.5, config.getFloat("tsd.unitest"), 0.000001);
  }
  
  @Test (expected = NullPointerException.class)
  public void getFloatNull() throws Exception {
    config.getFloat("tsd.blarg");
  }
  
  @Test (expected = NumberFormatException.class)
  public void getFloatNFE() throws Exception {
    config.overrideConfig("tsd.unitest", "this can't be parsed to float");
    config.getFloat("tsd.unitest");
  }
  
  @Test
  public void getDouble() throws Exception {
    config.overrideConfig("tsd.unitest", "42.5");
    assertEquals(42.5, config.getDouble("tsd.unitest"), 0.000001);
  }
  
  @Test (expected = NullPointerException.class)
  public void getDoubleNull() throws Exception {
    config.getDouble("tsd.blarg");
  }
  
  @Test (expected = NumberFormatException.class)
  public void getDoubleNFE() throws Exception {
    config.overrideConfig("tsd.unitest", "this can't be parsed to double");
    config.getDouble("tsd.unitest");
  }
  
  @Test
  public void getBool1() throws Exception {
    config.overrideConfig("tsd.unitest", "1");
    assertTrue(config.getBoolean("tsd.unitest"));
  }
  
  @Test
  public void getBoolTrue1() throws Exception {
    config.overrideConfig("tsd.unitest", "True");
    assertTrue(config.getBoolean("tsd.unitest"));
  }
  
  @Test
  public void getBoolTrue2() throws Exception {
    config.overrideConfig("tsd.unitest", "true");
    assertTrue(config.getBoolean("tsd.unitest"));
  }
  
  @Test
  public void getBoolYes() throws Exception {
    config.overrideConfig("tsd.unitest", "yes");
    assertTrue(config.getBoolean("tsd.unitest"));
  }
  
  @Test
  public void getBoolFalseEmpty() throws Exception {
    config.overrideConfig("tsd.unitest", "");
    assertFalse(config.getBoolean("tsd.unitest"));
  }
  
  @Test (expected = NullPointerException.class)
  public void getBoolFalseNull() throws Exception {
    config.getBoolean("tsd.unitest");
  }
  
  @Test
  public void getBoolFalseOther() throws Exception {
    config.overrideConfig("tsd.unitest", "blarg");
    assertFalse(config.getBoolean("tsd.unitest"));
  }
}
