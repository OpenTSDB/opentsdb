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
package net.opentsdb.query.pojo;

import java.util.Collection;
import java.util.Iterator;

import net.opentsdb.core.TSDB;

/**
 * An interface for the pojos to implement to make sure all the bits of the 
 * expression queries are there
 * @since 2.3
 */
public abstract class Validatable {
  
  abstract public void validate(final TSDB tsdb);

  /**
   * Iterate through a field that is a collection of POJOs and validate each of
   * them. Inherit member POJO's error message.
   * @param TSDB The non-null TSDB for validation.
   * @param collection the validatable POJO collection
   * @param name name of the field
   */
  <T extends Validatable> void validateCollection(final TSDB tsdb,
                                                  final Collection<T> collection,
                                                  final String name) {
    Iterator<T> iterator = collection.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      try {
        iterator.next().validate(tsdb);
      } catch (final IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid " + name + 
            " at index " + i, e);
      }
      i++;
    }
  }

  /**
   * Validate a single POJO validate
   * @param TSDB The non-null TSDB for validation.
   * @param pojo The POJO object to validate
   * @param name name of the field
   */
  <T extends Validatable> void validatePOJO(final TSDB tsdb, 
                                            final T pojo, 
                                            final String name) {
    try {
      pojo.validate(tsdb);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid " + name, e);
    }
  }
}
