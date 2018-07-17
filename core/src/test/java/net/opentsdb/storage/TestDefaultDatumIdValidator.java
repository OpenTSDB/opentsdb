// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.BaseTimeSeriesDatumStringId;
import net.opentsdb.data.TimeSeriesDatumId;
import net.opentsdb.storage.DefaultDatumIdValidator.Type;

public class TestDefaultDatumIdValidator {

  private MockTSDB tsdb;
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
  }
  
  @Test
  public void init() throws Exception {
    DefaultDatumIdValidator validator = new DefaultDatumIdValidator();
    validator.initialize(tsdb);
    
    assertFalse(validator.metric_utf8);
    assertFalse(validator.tagk_utf8);
    assertTrue(validator.tagv_utf8);
    
    assertFalse(validator.metric_allow_whitespace);
    assertFalse(validator.tagk_allow_whitespace);
    assertTrue(validator.tagv_allow_whitespace);
    
    assertEquals(256, validator.metric_length);
    assertEquals(256, validator.tagk_length);
    assertEquals(1024, validator.tagv_length);
    
    assertNull(validator.metric_special_chars);
    assertNull(validator.tagk_special_chars);
    assertNull(validator.tagv_special_chars);
    
    assertTrue(validator.require_at_least_one_tag);
  }
  
  @Test
  public void initOverrides() throws Exception {
    DefaultDatumIdValidator validator = new DefaultDatumIdValidator();
    validator.registerConfigs(tsdb);
    
    tsdb.config.override(DefaultDatumIdValidator.METRIC_UTF_KEY, "true");
    tsdb.config.override(DefaultDatumIdValidator.TAGK_UTF_KEY, "true");
    tsdb.config.override(DefaultDatumIdValidator.TAGV_UTF_KEY, "false");
    
    tsdb.config.override(DefaultDatumIdValidator.METRIC_WHITESPACE_KEY, "true");
    tsdb.config.override(DefaultDatumIdValidator.TAGK_WHITESPACE_KEY, "true");
    tsdb.config.override(DefaultDatumIdValidator.TAGV_WHITESPACE_KEY, "false");
    
    tsdb.config.override(DefaultDatumIdValidator.METRIC_LENGTH_KEY, "128");
    tsdb.config.override(DefaultDatumIdValidator.TAGK_LENGTH_KEY, "128");
    tsdb.config.override(DefaultDatumIdValidator.TAGV_LENGTH_KEY, "256");
    
    tsdb.config.override(DefaultDatumIdValidator.METRIC_SPECIAL_KEY, "!@#");
    tsdb.config.override(DefaultDatumIdValidator.TAGK_SPECIAL_KEY, "$%");
    tsdb.config.override(DefaultDatumIdValidator.TAGV_SPECIAL_KEY, "^&*");
    
    validator.initialize(tsdb);
    
    assertTrue(validator.metric_utf8);
    assertTrue(validator.tagk_utf8);
    assertFalse(validator.tagv_utf8);
    
    assertTrue(validator.metric_allow_whitespace);
    assertTrue(validator.tagk_allow_whitespace);
    assertFalse(validator.tagv_allow_whitespace);
    
    assertEquals(128, validator.metric_length);
    assertEquals(128, validator.tagk_length);
    assertEquals(256, validator.tagv_length);
    
    assertEquals("!@#", validator.metric_special_chars);
    assertEquals("$%", validator.tagk_special_chars);
    assertEquals("^&*", validator.tagv_special_chars);
    
    assertTrue(validator.require_at_least_one_tag);
  }

  @Test
  public void validateASCIIString() throws Exception {
    DefaultDatumIdValidator validator = new DefaultDatumIdValidator();
    validator.registerConfigs(tsdb);
    tsdb.config.override(DefaultDatumIdValidator.TAGV_SPECIAL_KEY, "(),");
    tsdb.config.override(DefaultDatumIdValidator.TAGV_UTF_KEY, "false");
    validator.initialize(tsdb);
    
    String string = "my.String_is_good/42";
    assertNull(validator.validateASCIIString(Type.METRIC, string));
    assertNull(validator.validateASCIIString(Type.TAGK, string));
    assertNull(validator.validateASCIIString(Type.TAGV, string));
    
    string = "0.99.couldbe.a.string";
    assertNull(validator.validateASCIIString(Type.METRIC, string));
    assertNull(validator.validateASCIIString(Type.TAGK, string));
    assertNull(validator.validateASCIIString(Type.TAGV, string));
    
    string = "this(willfail).except-for-tags";
    assertNotNull(validator.validateASCIIString(Type.METRIC, string));
    assertNotNull(validator.validateASCIIString(Type.TAGK, string));
    assertNull(validator.validateASCIIString(Type.TAGV, string));
    
    string = "white space only on tags";
    assertNotNull(validator.validateASCIIString(Type.METRIC, string));
    assertNotNull(validator.validateASCIIString(Type.TAGK, string));
    assertNull(validator.validateASCIIString(Type.TAGV, string));
    
    string = "newlines\narenotgood\r\n";
    assertNotNull(validator.validateASCIIString(Type.METRIC, string));
    assertNotNull(validator.validateASCIIString(Type.TAGK, string));
    assertNotNull(validator.validateASCIIString(Type.TAGV, string));
    
    string = "a"; // single chars ok
    assertNull(validator.validateASCIIString(Type.METRIC, string));
    assertNull(validator.validateASCIIString(Type.TAGK, string));
    assertNull(validator.validateASCIIString(Type.TAGV, string));
    
    string = "0"; // single chars ok
    assertNull(validator.validateASCIIString(Type.METRIC, string));
    assertNull(validator.validateASCIIString(Type.TAGK, string));
    assertNull(validator.validateASCIIString(Type.TAGV, string));
    
    string = "";
    assertNotNull(validator.validateASCIIString(Type.METRIC, string));
    assertNotNull(validator.validateASCIIString(Type.TAGK, string));
    assertNotNull(validator.validateASCIIString(Type.TAGV, string));
    
    string = null;
    assertNotNull(validator.validateASCIIString(Type.METRIC, string));
    assertNotNull(validator.validateASCIIString(Type.TAGK, string));
    assertNotNull(validator.validateASCIIString(Type.TAGV, string));
    
    // This actually matched in 2.x. Now it fails.
    string = "私はガラスを食べられまそれは私を傷つけません";
    assertNotNull(validator.validateASCIIString(Type.METRIC, string));
    assertNotNull(validator.validateASCIIString(Type.TAGK, string));
    assertNotNull(validator.validateASCIIString(Type.TAGV, string));
  }

  @Test
  public void validateUTFString() throws Exception {
    DefaultDatumIdValidator validator = new DefaultDatumIdValidator();
    validator.initialize(tsdb);
    
    // http://www.columbia.edu/~fdc/utf8/
    String string = "私はガラスを食べられます。それは私を傷つけません";
    assertNull(validator.validateUTFString(Type.METRIC, string));
    assertNull(validator.validateUTFString(Type.TAGK, string));
    assertNull(validator.validateUTFString(Type.TAGV, string));
    
    string = "Я можу їсти шкло, й воно мені не пошкодить.";
    assertNotNull(validator.validateUTFString(Type.METRIC, string));
    assertNotNull(validator.validateUTFString(Type.TAGK, string));
    assertNull(validator.validateUTFString(Type.TAGV, string));
    
    string = "ᚠᛇᚻ᛫ᛒᛦᚦ᛫ᚠᚱᚩᚠᚢᚱ᛫ᚠᛁᚱᚪ᛫ᚷᛖᚻᚹᛦᛚᚳᚢᛗ\n" + 
        "ᛋᚳᛖᚪᛚ᛫ᚦᛖᚪᚻ᛫ᛗᚪᚾᚾᚪ᛫ᚷᛖᚻᚹᛦᛚᚳ᛫ᛗᛁᚳᛚᚢᚾ᛫ᚻᛦᛏ᛫ᛞᚫᛚᚪᚾ\n" + 
        "ᚷᛁᚠ᛫ᚻᛖ᛫ᚹᛁᛚᛖ᛫ᚠᚩᚱ᛫ᛞᚱᛁᚻᛏᚾᛖ᛫ᛞᚩᛗᛖᛋ᛫ᚻᛚᛇᛏᚪᚾ᛬";
    assertNotNull(validator.validateUTFString(Type.METRIC, string));
    assertNotNull(validator.validateUTFString(Type.TAGK, string));
    assertNotNull(validator.validateUTFString(Type.TAGV, string));
  }

  @Test
  public void validate() throws Exception {
    DefaultDatumIdValidator validator = new DefaultDatumIdValidator();
    validator.registerConfigs(tsdb);
    tsdb.config.override(DefaultDatumIdValidator.METRIC_LENGTH_KEY, "16");
    tsdb.config.override(DefaultDatumIdValidator.TAGK_LENGTH_KEY, "16");
    tsdb.config.override(DefaultDatumIdValidator.TAGV_LENGTH_KEY, "128");
    validator.initialize(tsdb);
    
    TimeSeriesDatumId id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .build();
    assertNull(validator.validate(id));
    
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.user")
        .addTags("host", "web01")
        .addTags("sentence", "Sævör grét áðan því úlpan var ónýt.")
        .build();
    assertNull(validator.validate(id));
    
    // tag required
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.user")
        //.addTags("host", "web01")
        .build();
    assertNotNull(validator.validate(id));
    
    // metric is to long.
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.user.andmore.bits.and.bobs")
        .addTags("host", "web01")
        .build();
    assertNotNull(validator.validate(id));
    
    // No UTF in the metric
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("Blåbærsyltetøy")
        .addTags("host", "web01")
        .build();
    assertNotNull(validator.validate(id));
    
    // only strings for now.
    id = mock(TimeSeriesDatumId.class);
    when(id.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    assertNotNull(validator.validate(id));
    
    try {
      validator.validate(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // disable the length check now.
    validator = new DefaultDatumIdValidator();
    validator.registerConfigs(tsdb);
    tsdb.config.override(DefaultDatumIdValidator.METRIC_LENGTH_KEY, "0");
    tsdb.config.override(DefaultDatumIdValidator.TAGK_LENGTH_KEY, "16");
    tsdb.config.override(DefaultDatumIdValidator.TAGV_LENGTH_KEY, "128");
    validator.initialize(tsdb);
    
    id = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric("sys.cpu.user.andmore.bits.and.bobs")
        .addTags("host", "web01")
        .build();
    assertNull(validator.validate(id));
  }
}
