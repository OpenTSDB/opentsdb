// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data.iterators;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestIteratorStatus {

  @Test
  public void updateStatus() throws Exception {
    // HAS_DATA
    assertEquals(IteratorStatus.HAS_DATA, 
        IteratorStatus.updateStatus(IteratorStatus.END_OF_DATA, 
                                    IteratorStatus.HAS_DATA));
    
    assertEquals(IteratorStatus.HAS_DATA, 
        IteratorStatus.updateStatus(IteratorStatus.END_OF_CHUNK, 
                                    IteratorStatus.HAS_DATA));
    
    assertEquals(IteratorStatus.EXCEPTION, 
        IteratorStatus.updateStatus(IteratorStatus.EXCEPTION, 
                                    IteratorStatus.HAS_DATA));
    
    assertEquals(IteratorStatus.HAS_DATA, 
        IteratorStatus.updateStatus(IteratorStatus.HAS_DATA, 
                                    IteratorStatus.HAS_DATA));
    
    // END OF CHUNK
    assertEquals(IteratorStatus.END_OF_CHUNK, 
        IteratorStatus.updateStatus(IteratorStatus.END_OF_DATA, 
                                    IteratorStatus.END_OF_CHUNK));
    
    assertEquals(IteratorStatus.END_OF_CHUNK, 
        IteratorStatus.updateStatus(IteratorStatus.END_OF_CHUNK, 
                                    IteratorStatus.END_OF_CHUNK));
    
    assertEquals(IteratorStatus.HAS_DATA, 
        IteratorStatus.updateStatus(IteratorStatus.HAS_DATA, 
                                    IteratorStatus.END_OF_CHUNK));
    
    assertEquals(IteratorStatus.EXCEPTION, 
        IteratorStatus.updateStatus(IteratorStatus.EXCEPTION, 
                                    IteratorStatus.END_OF_CHUNK));
    
    // END OF DATA
    assertEquals(IteratorStatus.END_OF_DATA, 
        IteratorStatus.updateStatus(IteratorStatus.END_OF_DATA, 
                                    IteratorStatus.END_OF_DATA));
    
    assertEquals(IteratorStatus.END_OF_CHUNK, 
        IteratorStatus.updateStatus(IteratorStatus.END_OF_CHUNK, 
                                    IteratorStatus.END_OF_DATA));
    
    assertEquals(IteratorStatus.HAS_DATA, 
        IteratorStatus.updateStatus(IteratorStatus.HAS_DATA, 
                                    IteratorStatus.END_OF_DATA));
    
    assertEquals(IteratorStatus.EXCEPTION, 
        IteratorStatus.updateStatus(IteratorStatus.EXCEPTION, 
                                    IteratorStatus.END_OF_DATA));
    
    // EXCEPTION
    assertEquals(IteratorStatus.EXCEPTION, 
        IteratorStatus.updateStatus(IteratorStatus.END_OF_DATA, 
                                    IteratorStatus.EXCEPTION));
    
    assertEquals(IteratorStatus.EXCEPTION, 
        IteratorStatus.updateStatus(IteratorStatus.END_OF_CHUNK, 
                                    IteratorStatus.EXCEPTION));
    
    assertEquals(IteratorStatus.EXCEPTION, 
        IteratorStatus.updateStatus(IteratorStatus.HAS_DATA, 
                                    IteratorStatus.EXCEPTION));
    
    assertEquals(IteratorStatus.EXCEPTION, 
        IteratorStatus.updateStatus(IteratorStatus.EXCEPTION, 
                                    IteratorStatus.EXCEPTION));
  }
}
