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
package net.opentsdb.query.serdes;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.PBufNumericSerdesFactory;
import net.opentsdb.data.PBufNumericSummarySerdesFactory;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;

public class TestPBufIteratorSerdesFactory {

  @Test
  public void ctor() throws Exception {
    PBufIteratorSerdesFactory factory = new PBufIteratorSerdesFactory();
    assertTrue(factory.serdesForType(NumericType.TYPE) 
        instanceof PBufNumericSerdesFactory);
    assertTrue(factory.serdesForType(NumericSummaryType.TYPE) 
        instanceof PBufNumericSummarySerdesFactory);
    assertNull(factory.serdesForType(TypeToken.of(String.class)));
  }
  
  @Test
  public void register() throws Exception {
    PBufIteratorSerdesFactory factory = new PBufIteratorSerdesFactory();
    TypeToken<?> string_type = TypeToken.of(String.class);
    PBufIteratorSerdes string_serdes = mock(PBufIteratorSerdes.class);
    when(string_serdes.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return string_type;
      }
    });
    
    factory.register(string_serdes);
    assertTrue(factory.serdesForType(NumericType.TYPE) 
        instanceof PBufNumericSerdesFactory);
    assertTrue(factory.serdesForType(NumericSummaryType.TYPE) 
        instanceof PBufNumericSummarySerdesFactory);
    assertSame(string_serdes, factory.serdesForType(string_type));
    
    // replace
    PBufIteratorSerdes new_numeric = mock(PBufIteratorSerdes.class);
    when(new_numeric.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return NumericType.TYPE;
      }
    });
    factory.register(new_numeric);
    assertSame(new_numeric, factory.serdesForType(NumericType.TYPE));
    assertTrue(factory.serdesForType(NumericSummaryType.TYPE) 
        instanceof PBufNumericSummarySerdesFactory);
    assertSame(string_serdes, factory.serdesForType(string_type));
    
    try {
      factory.register(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // null type
    new_numeric = mock(PBufIteratorSerdes.class);
    try {
      factory.register(new_numeric);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
