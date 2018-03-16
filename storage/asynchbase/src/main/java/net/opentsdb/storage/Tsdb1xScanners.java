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

import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.storage.schemas.tsdb1x.Schema;

/**
 * TODO - stub class
 */
public class Tsdb1xScanners {
  
  public static enum State {
    CONTINUE,
    COMPLETE,
    EXCEPTION
  }
  
  boolean hasException() {
    // TODO - implement
    return false;
  }
  
  boolean isFull() {
    // TODO - implement
    return false;
  }
  
  void scannerDone() {
    // TODO - implement
  }

  public Schema schema() {
    // TODO - implement
    return null;
  }
  
  public TimeStamp sequenceEnd() {
    // TODO - implement
    return null;
  }

  public Filter scannerFilter() {
    // TODO - implement
    return null;
  }
  
  public void exception(Throwable t) {
    // TODO - implement
    
  }

  Tsdb1xQueryNode node() {
    // TODO - implement
    return null;
  }
}
