// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
