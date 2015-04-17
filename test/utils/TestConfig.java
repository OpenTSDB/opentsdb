// This file is part of OpenTSDB.
// Copyright (C) 2013-2014  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Config.class, FileInputStream.class })
public final class TestConfig {

  @Test
  public void constructor() throws Exception {
    assertNotNull(new Config(false));
  }
  
  @Test
  public void constructorDefault() throws Exception {
    assertEquals("0.0.0.0", new Config(false).getString("tsd.network.bind"));
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
  
  @Test(expected = NullPointerException.class)
  public void constructorNullChild() throws Exception {
    new Config((Config) null);
  }

  @Test
  public void constructorWithFile() throws Exception {
    PowerMockito.whenNew(FileInputStream.class).withAnyArguments()
      .thenReturn(mock(FileInputStream.class));
    final Properties props = new Properties();
    props.setProperty("tsd.test", "val1");
    PowerMockito.whenNew(Properties.class).withNoArguments().thenReturn(props);
    
    final Config config = new Config("/tmp/config.file");
    assertNotNull(config);
    assertEquals("/tmp/config.file", config.config_location);
    assertEquals("val1", config.getString("tsd.test"));
  }

  @Test(expected = FileNotFoundException.class)
  public void constructorFileNotFound() throws Exception {
    new Config("/tmp/filedoesnotexist.conf");
  }

  @Test(expected = NullPointerException.class)
  public void constructorNullFile() throws Exception {
    new Config((String) null);
  }

  @Test(expected = FileNotFoundException.class)
  public void constructorEmptyFile() throws Exception {
    new Config("");
  }

  @Test (expected = FileNotFoundException.class)
  public void loadConfigNotFound() throws Exception {
    Config c = new Config(false);
    c.loadConfig("/tmp/filedoesnotexist.conf");
  }

  @Test(expected = NullPointerException.class)
  public void loadConfigNull() throws Exception {
    final Config config = new Config(false);
    config.loadConfig(null);
  }

  @Test(expected = FileNotFoundException.class)
  public void loadConfigEmpty() throws Exception {
    final Config config = new Config(false);
    config.loadConfig("");
  }

  @Test
  public void overrideConfig() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.core.bind", "127.0.0.1");
    assertEquals("127.0.0.1", config.getString("tsd.core.bind"));
  }
  
  @Test 
  public void getString() throws Exception {
    final Config config = new Config(false);
    assertEquals("1000", config.getString("tsd.storage.flush_interval"));
  }
  
  @Test
  public void getStringNull() throws Exception {
    final Config config = new Config(false);
    assertNull(config.getString("tsd.blarg"));
  }
  
  @Test
  public void getInt() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.int", 
        Integer.toString(Integer.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, 
        config.getInt("tsd.int"));
  }

  @Test
  public void getIntWithSpaces() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.int", 
        " " + Integer.toString(Integer.MAX_VALUE) + " ");
    assertEquals(Integer.MAX_VALUE, 
        config.getInt("tsd.int"));
  }

  @Test
  public void getIntNegative() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.int", 
        Integer.toString(Integer.MIN_VALUE));
    assertEquals(Integer.MIN_VALUE, 
        config.getInt("tsd.int"));
  }

  @Test(expected = NumberFormatException.class)
  public void getIntNull() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.null", null);
    config.getInt("tsd.null");
  }

  @Test(expected = NumberFormatException.class)
  public void getIntDoesNotExist() throws Exception {
    final Config config = new Config(false);
    config.getInt("tsd.nosuchkey");
  }

  @Test(expected = NumberFormatException.class)
  public void getIntNFE() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.int", 
        "this can't be parsed to int");
    config.getInt("tsd.int");
  }

  @Test
  public void getShort() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.short", 
        Short.toString(Short.MAX_VALUE));
    assertEquals(Short.MAX_VALUE, 
        config.getShort("tsd.short"));
  }

  @Test
  public void getShortWithSpaces() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.short", 
        " " + Short.toString(Short.MAX_VALUE) + " ");
    assertEquals(Short.MAX_VALUE, 
        config.getShort("tsd.short"));
  }

  @Test
  public void getShortNegative() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.short", 
        Short.toString(Short.MIN_VALUE));
    assertEquals(Short.MIN_VALUE, 
        config.getShort("tsd.short"));
  }

  @Test(expected = NumberFormatException.class)
  public void getShortNull() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.null", null);
    config.getShort("tsd.null");
  }

  @Test(expected = NumberFormatException.class)
  public void getShortDoesNotExist() throws Exception {
    final Config config = new Config(false);
    config.getShort("tsd.nosuchkey");
  }

  @Test(expected = NumberFormatException.class)
  public void getShortNFE() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.short", 
        "this can't be parsed to short");
    config.getShort("tsd.short");
  }

  @Test
  public void getLong() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.long", Long.toString(Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, config.getLong("tsd.long"));
  }

  @Test
  public void getLongWithSpaces() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.long", " " + Long.toString(Long.MAX_VALUE) + " ");
    assertEquals(Long.MAX_VALUE, config.getLong("tsd.long"));
  }

  @Test
  public void getLongNegative() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.long", Long.toString(Long.MIN_VALUE));
    assertEquals(Long.MIN_VALUE, 
        config.getLong("tsd.long"));
  }

  @Test(expected = NumberFormatException.class)
  public void getLongNull() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.null", null);
    config.getLong("tsd.null");
  }

  @Test(expected = NumberFormatException.class)
  public void getLongDoesNotExist() throws Exception {
    final Config config = new Config(false);
    config.getLong("tsd.nosuchkey");
  }

  @Test(expected = NumberFormatException.class)
  public void getLongNullNFE() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.long", "this can't be parsed to long");
    config.getLong("tsd.long");
  }

  @Test
  public void getFloat() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.float", Float.toString(Float.MAX_VALUE));
    assertEquals(Float.MAX_VALUE, 
        config.getFloat("tsd.float"), 0.000001);
  }

  @Test
  public void getFloatWithSpaces() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.float", " " + Float.toString(Float.MAX_VALUE) + " ");
    assertEquals(Float.MAX_VALUE, 
        config.getFloat("tsd.float"), 0.000001);
  }

  @Test
  public void getFloatNegative() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.float", Float.toString(Float.MIN_VALUE));
    assertEquals(Float.MIN_VALUE, 
        config.getFloat("tsd.float"), 0.000001);
  }

  @Test
  public void getFloatNaN() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.float", "NaN");
    assertEquals(Float.NaN, 
        config.getDouble("tsd.float"), 0.000001);
  }

  @Test(expected = NumberFormatException.class)
  public void getFloatNaNBadCase() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.float", "nan");
    assertEquals(Float.NaN, 
        config.getDouble("tsd.float"), 0.000001);
  }

  @Test
  public void getFloatPIfinity() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.float", "Infinity");
    assertEquals(Float.POSITIVE_INFINITY, 
        config.getDouble("tsd.float"), 0.000001);
  }

  @Test
  public void getFloatNIfinity() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.float", "-Infinity");
    assertEquals(Float.NEGATIVE_INFINITY, 
        config.getDouble("tsd.float"), 0.000001);
  }

  @Test(expected = NullPointerException.class)
  public void getFloatNull() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.null", null);
    config.getFloat("tsd.null");
  }

  @Test(expected = NullPointerException.class)
  public void getFloatDoesNotExist() throws Exception {
    final Config config = new Config(false);
    config.getFloat("tsd.nosuchkey");
  }

  @Test(expected = NumberFormatException.class)
  public void getFloatNFE() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.float", "this can't be parsed to float");
    config.getFloat("tsd.float");
  }

  @Test
  public void getDouble() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.double", Double.toString(Double.MAX_VALUE));
    assertEquals(Double.MAX_VALUE, 
        config.getDouble("tsd.double"), 0.000001);
  }

  @Test
  public void getDoubleWithSpaces() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.double", " " + Double.toString(Double.MAX_VALUE) + " ");
    assertEquals(Double.MAX_VALUE, 
        config.getDouble("tsd.double"), 0.000001);
  }

  @Test
  public void getDoubleNegative() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.double", Double.toString(Double.MIN_VALUE));
    assertEquals(Double.MIN_VALUE, 
        config.getDouble("tsd.double"), 0.000001);
  }

  @Test
  public void getDoubleNaN() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.double", "NaN");
    assertEquals(Double.NaN, 
        config.getDouble("tsd.double"), 0.000001);
  }

  @Test(expected = NumberFormatException.class)
  public void getDoubleNaNBadCase() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.double", "nan");
    assertEquals(Double.NaN, 
        config.getDouble("tsd.double"), 0.000001);
  }

  @Test
  public void getDoublePIfinity() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.double", "Infinity");
    assertEquals(Double.POSITIVE_INFINITY,
        config.getDouble("tsd.double"), 0.000001);
  }

  @Test
  public void getDoubleNIfinity() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.double", "-Infinity");
    assertEquals(Double.NEGATIVE_INFINITY,
        config.getDouble("tsd.double"), 0.000001);
  }

  @Test(expected = NullPointerException.class)
  public void getDoubleNull() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.null", null);
    config.getDouble("tsd.null");
  }

  @Test(expected = NullPointerException.class)
  public void getDoubleDoesNotExist() throws Exception {
    final Config config = new Config(false);
    config.getDouble("tsd.nosuchkey");
  }

  @Test(expected = NumberFormatException.class)
  public void getDoubleNFE() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.double", 
        "this can't be parsed to double");
    config.getDouble("tsd.double");
  }

  @Test
  public void getBool() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "true");
    assertTrue(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBool1() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "1");
    assertTrue(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBool1WithSpaces() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", " 1  ");
    assertTrue(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBoolTrueCaseInsensitive() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "TrUe");
    assertTrue(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBoolTrueWithSpaces() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "TrUe ");
    assertTrue(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBoolYes() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "yes");
    assertTrue(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBoolYesWithSpaces() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", " yes  ");
    assertTrue(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBoolYesCaseInsensitive() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "YeS");
    assertTrue(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBoolFalse() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "false");
    assertFalse(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBoolFalse0() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "0");
    assertFalse(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBoolFalse2() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "2");
    assertFalse(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBoolFalseNo() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "no");
    assertFalse(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getBoolFalseEmpty() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "");
    assertFalse(config.getBoolean("tsd.bool"));
  }

  @Test(expected = NullPointerException.class)
  public void getBoolFalseNull() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.null", null);
    config.getBoolean("tsd.null");
  }

  @Test (expected = NullPointerException.class)
  public void getBoolFalseDoesNotExist() throws Exception {
    final Config config = new Config(false);
    assertFalse(config.getBoolean("tsd.nosuchkey"));
  }

  @Test
  public void getBoolFalseOther() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.bool", "blarg");
    assertFalse(config.getBoolean("tsd.bool"));
  }

  @Test
  public void getDirectoryNameAddSlash() throws Exception {
    // same for Windows && Unix
    final Config config = new Config(false);
    config.overrideConfig("tsd.unitest", "/my/dir");
    assertEquals("/my/dir/", config.getDirectoryName("tsd.unitest"));
  }
  
  @Test
  public void getDirectoryNameHasSlash() throws Exception {
    // same for Windows && Unix
    final Config config = new Config(false);
    config.overrideConfig("tsd.unitest", "/my/dir/");
    assertEquals("/my/dir/", config.getDirectoryName("tsd.unitest"));
  }
  
  @Test
  public void getDirectoryNameWindowsAddSlash() throws Exception {
    if (Config.IS_WINDOWS) {
      final Config config = new Config(false);
      config.overrideConfig("tsd.unitest", "C:\\my\\dir");
      assertEquals("C:\\my\\dir\\", config.getDirectoryName("tsd.unitest"));
    } else {
      assertTrue(true);
    }
  }
  
  @Test
  public void getDirectoryNameWindowsHasSlash() throws Exception {
    if (Config.IS_WINDOWS) {
      final Config config = new Config(false);
      config.overrideConfig("tsd.unitest", "C:\\my\\dir\\");
      assertEquals("C:\\my\\dir\\", config.getDirectoryName("tsd.unitest"));
    } else {
      assertTrue(true);
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getDirectoryNameWindowsOnLinuxException() throws Exception {
    if (Config.IS_WINDOWS) {
      throw new IllegalArgumentException("Can't run this on Windows");
    } else {
      final Config config = new Config(false);
      config.overrideConfig("tsd.unitest", "C:\\my\\dir");
      config.getDirectoryName("tsd.unitest");
    }
  }
  
  @Test (expected = NullPointerException.class)
  public void getDirectoryNameNull() throws Exception {
    final Config config = new Config(false);
    assertNull(config.getDirectoryName("tsd.unitest"));
  }
  
  @Test
  public void getDirectoryNameEmpty() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.unitest", "");
    assertNull(config.getDirectoryName("tsd.unitest"));
  }
  
  @Test
  public void getDirectoryNameNoslash() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.unitest", "relative");
    assertEquals("relative/", config.getDirectoryName("tsd.unitest"));
  }

  @Test
  public void hasProperty() throws Exception {
    final Config config = new Config(false);
    assertTrue(config.hasProperty("tsd.network.bind"));
  }

  @Test
  public void hasPropertyNull() throws Exception {
    final Config config = new Config(false);
    config.overrideConfig("tsd.null", null);
    assertFalse(config.hasProperty("tsd.null"));
  }

  @Test
  public void hasPropertyNot() throws Exception {
    final Config config = new Config(false);
    assertFalse(config.hasProperty("tsd.nosuchkey"));
  }
}
