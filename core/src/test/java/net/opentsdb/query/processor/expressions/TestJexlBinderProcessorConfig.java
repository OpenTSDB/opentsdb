// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.expressions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.NumericFillPolicy;

public class TestJexlBinderProcessorConfig {

  @Test
  public void builder() throws Exception {
    JexlBinderProcessorConfig config = (JexlBinderProcessorConfig) 
        JexlBinderProcessorConfig.newBuilder()
          .setExpression(Expression.newBuilder()
            .setId("e1")
            .setExpression("a + b")
            .build())
        .build();
    assertEquals("e1", config.getExpression().getId());
    assertEquals("a + b", config.getExpression().getExpr());
    
    try {
      JexlBinderProcessorConfig.newBuilder()
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // invalid expression (missing ID)
    try {
      JexlBinderProcessorConfig.newBuilder()
        .setExpression(Expression.newBuilder()
          .setExpression("a + b")
          .build())
      .build();
      fail("Expected ParseException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final Map<String, NumericFillPolicy> fills = Maps.newHashMap();
    fills.put("a", NumericFillPolicy.newBuilder()
        .setPolicy(FillPolicy.ZERO).build());
    fills.put("b", NumericFillPolicy.newBuilder()
        .setPolicy(FillPolicy.SCALAR).setValue(-100).build());
    
    final JexlBinderProcessorConfig c1 = (JexlBinderProcessorConfig) 
        JexlBinderProcessorConfig.newBuilder()
          .setExpression(Expression.newBuilder()
            .setId("e1")
            .setExpression("a + b")
            .setFillPolicy(NumericFillPolicy.newBuilder()
                .setPolicy(FillPolicy.NOT_A_NUMBER).build())
            .build())
        .build();
    
    JexlBinderProcessorConfig c2 = (JexlBinderProcessorConfig) 
        JexlBinderProcessorConfig.newBuilder()
          .setExpression(Expression.newBuilder()
            .setId("e1")
            .setExpression("a + b")
            .setFillPolicy(NumericFillPolicy.newBuilder()
                .setPolicy(FillPolicy.NOT_A_NUMBER).build())
            .build())
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (JexlBinderProcessorConfig) 
        JexlBinderProcessorConfig.newBuilder()
          .setExpression(Expression.newBuilder()
            .setId("e2") // <-- Diff
            .setExpression("a + b")
            .setFillPolicy(NumericFillPolicy.newBuilder()
                .setPolicy(FillPolicy.NOT_A_NUMBER).build())
            .build())
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
  }
  
}
