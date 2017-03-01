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
