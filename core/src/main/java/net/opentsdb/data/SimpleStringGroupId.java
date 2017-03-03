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
package net.opentsdb.data;

import java.util.Comparator;
import java.util.Objects;

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
