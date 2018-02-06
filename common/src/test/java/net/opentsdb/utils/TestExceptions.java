// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

public class TestExceptions {
  private ArrayList<Deferred<Object>> deferreds;
  
  @Before
  public void before() {
    deferreds = new ArrayList<Deferred<Object>>(1);
  }
  
  @Test
  public void oneLevel() throws Exception {
    final RuntimeException ex = new RuntimeException("Boo!");
    deferreds.add(Deferred.fromError(ex));
    try {
      Deferred.group(deferreds).join();
      fail("Expected a DeferredGroupException");
    } catch (DeferredGroupException dge) {
      assertSame(ex, Exceptions.getCause(dge));
    }
  }
  
  @Test
  public void nested() throws Exception {
    final RuntimeException ex = new RuntimeException("Boo!");
    deferreds.add(Deferred.fromError(ex));
    
    final ArrayList<Deferred<Object>> deferreds2 = 
        new ArrayList<Deferred<Object>>(1);
    deferreds2.add(Deferred.fromResult(null));
    
    class LOne implements 
      Callback<Deferred<ArrayList<Object>>, ArrayList<Object>> {
      @Override
      public Deferred<ArrayList<Object>> call(final ArrayList<Object> piff) 
          throws Exception {
        return Deferred.group(deferreds);
      }
    }
    
    try {
      Deferred.group(deferreds2).addCallbackDeferring(new LOne()).join();
      fail("Expected a DeferredGroupException");
    } catch (DeferredGroupException dge) {
      assertSame(ex, Exceptions.getCause(dge));
    }
  }
  
}
