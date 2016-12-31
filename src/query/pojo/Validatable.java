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
package net.opentsdb.query.pojo;

import java.util.Collection;
import java.util.Iterator;

/**
 * An interface for the pojos to implement to make sure all the bits of the 
 * expression queries are there
 * @since 2.3
 */
public abstract class Validatable {
  abstract public void validate();

  /**
   * Iterate through a field that is a collection of POJOs and validate each of
   * them. Inherit member POJO's error message.
   * @param collection the validatable POJO collection
   * @param name name of the field
   */
  <T extends Validatable> void validateCollection(final Collection<T> collection,
                                                  final String name) {
    Iterator<T> iterator = collection.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      try {
        iterator.next().validate();
      } catch (final IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid " + name + 
            " at index " + i, e);
      }
      i++;
    }
  }

  /**
   * Validate a single POJO validate
   * @param pojo The POJO object to validate
   * @param name name of the field
   */
  <T extends Validatable> void validatePOJO(final T pojo, final String name) {
    try {
      pojo.validate();
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid " + name, e);
    }
  }
}
