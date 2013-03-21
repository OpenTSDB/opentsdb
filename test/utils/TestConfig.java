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
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;

import org.junit.Test;

public final class TestConfig {

  @Test
  public void constructor() throws Exception {
    assertNotNull(new Config(false));
  }
  
  @Test
  public void constructorDefault() throws Exception {
    assertEquals(new Config(false).getString("tsd.network.bind"), "0.0.0.0");
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
    assertEquals(c.getString("MyProp"), "Parent");
    assertEquals(ch.getString("MyProp"), "Child");
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
    c.getString("tsd.core.bind").equals("127.0.0.1");
  }
  
  @Test 
  public void getString() throws Exception {
    assertEquals(new Config(false).getString("tsd.storage.flush_interval"), 
        "1000");
  }
  
  @Test (expected = NullPointerException.class)
  public void getStringNull() throws Exception {
    // assertEquals fails this test
    assertTrue(new Config(false).getString("tsd.blarg").equals("1000"));
  }
  
  @Test
  public void getInt() throws Exception {
    assertEquals(new Config(false).getInt("tsd.storage.flush_interval"), 1000);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getIntNull() throws Exception {
    new Config(false).getInt("tsd.blarg");
  }
  
  @Test (expected = NumberFormatException.class)
  public void getIntNFE() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.blarg", "this can't be parsed to int");
    c.getInt("tsd.blarg");
  }
  
  @Test
  public void getShort() throws Exception {
    assertEquals(new Config(false).getShort("tsd.storage.flush_interval"), 1000);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getShortNull() throws Exception {
    assertEquals(new Config(false).getShort("tsd.blarg"), 1000);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getShortNFE() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.blarg", "this can't be parsed to short");
    c.getShort("tsd.blarg");
  }
  
  @Test
  public void getLong() throws Exception {
    assertEquals(new Config(false).getLong("tsd.storage.flush_interval"), 1000);
  }
  
  @Test (expected = NumberFormatException.class)
  public void getLongNull() throws Exception {
    new Config(false).getLong("tsd.blarg");
  }
  
  @Test (expected = NumberFormatException.class)
  public void getLongNullNFE() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.blarg", "this can't be parsed to long");
    c.getLong("tsd.blarg");
  }
  
  @Test
  public void getFloat() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "42.5");
    // assertEquals is deprecated for floats/doubles
    assertEquals(c.getFloat("tsd.unitest"), 42.5, 0.000001);
  }
  
  @Test (expected = NullPointerException.class)
  public void getFloatNull() throws Exception {
    new Config(false).getFloat("tsd.blarg");
  }
  
  @Test (expected = NumberFormatException.class)
  public void getFloatNFE() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "this can't be parsed to float");
    c.getFloat("tsd.unitest");
  }
  
  @Test
  public void getDouble() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "42.5");
    assertEquals(c.getDouble("tsd.unitest"),  42.5, 0.000001);
  }
  
  @Test (expected = NullPointerException.class)
  public void getDoubleNull() throws Exception {
    new Config(false).getDouble("tsd.blarg");
  }
  
  @Test (expected = NumberFormatException.class)
  public void getDoubleNFE() throws Exception {
    Config c = new Config(false);
    c.overrideConfig("tsd.unitest", "this can't be parsed to double");
    c.getDouble("tsd.unitest");
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
