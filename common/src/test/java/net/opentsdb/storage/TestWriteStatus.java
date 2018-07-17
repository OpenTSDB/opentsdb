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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import net.opentsdb.storage.WriteStatus.WriteState;
import net.opentsdb.utils.UnitTestException;

public class TestWriteStatus {

  @Test
  public void ok() throws Exception {
    WriteStatus status = WriteStatus.ok();
    assertEquals(WriteState.OK, status.state());
    assertNull(status.message());
    assertNull(status.exception());
  }
  
  @Test
  public void retry() throws Exception {
    WriteStatus status = WriteStatus.retry(null);
    assertEquals(WriteState.RETRY, status.state());
    assertNull(status.message());
    assertNull(status.exception());
    
    status = WriteStatus.retry("");
    assertEquals(WriteState.RETRY, status.state());
    assertEquals("", status.message());
    assertNull(status.exception());
    
    status = WriteStatus.retry("oops");
    assertEquals(WriteState.RETRY, status.state());
    assertEquals("oops", status.message());
    assertNull(status.exception());
  }
  
  @Test
  public void rejected() throws Exception {
    WriteStatus status = WriteStatus.rejected(null);
    assertEquals(WriteState.REJECTED, status.state());
    assertNull(status.message());
    assertNull(status.exception());
    
    status = WriteStatus.rejected("");
    assertEquals(WriteState.REJECTED, status.state());
    assertEquals("", status.message());
    assertNull(status.exception());
    
    status = WriteStatus.rejected("oops");
    assertEquals(WriteState.REJECTED, status.state());
    assertEquals("oops", status.message());
    assertNull(status.exception());
  }
  
  @Test
  public void error() throws Exception {
    WriteStatus status = WriteStatus.error(null, null);
    assertEquals(WriteState.ERROR, status.state());
    assertNull(status.message());
    assertNull(status.exception());
    
    status = WriteStatus.error("", null);
    assertEquals(WriteState.ERROR, status.state());
    assertEquals("", status.message());
    assertNull(status.exception());
    
    status = WriteStatus.error("oops", null);
    assertEquals(WriteState.ERROR, status.state());
    assertEquals("oops", status.message());
    assertNull(status.exception());
    
    status = WriteStatus.error("oops", new UnitTestException());
    assertEquals(WriteState.ERROR, status.state());
    assertEquals("oops", status.message());
    assertTrue(status.exception() instanceof UnitTestException);
  }
}
