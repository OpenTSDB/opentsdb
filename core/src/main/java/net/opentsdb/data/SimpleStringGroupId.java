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
package net.opentsdb.data;

import java.util.Comparator;

import com.google.common.base.Objects;

/**
 * TEMP - simple implementation of the group ID.
 * @since 3.0
 */
public class SimpleStringGroupId implements TimeSeriesGroupId, Comparator<TimeSeriesGroupId> {
  
  final String id;
  
  public SimpleStringGroupId(final String id) {
    this.id = id;
  }
  
  @Override
  public String id() {
    return id;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SimpleStringGroupId)) {
      return false;
    }
    return id.equals(((SimpleStringGroupId) o).id);
  }

  @Override
  public int compare(TimeSeriesGroupId o1, TimeSeriesGroupId o2) {
    return o1.id().compareTo(o2.id());
  }
}
