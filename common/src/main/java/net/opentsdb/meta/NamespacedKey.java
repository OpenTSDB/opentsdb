// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.meta;


import com.google.common.base.Objects;

/**
 * Key with Namespace and Id of the meta query
 *
 * @since 3.0
 */
public class NamespacedKey {

  /*** Namespace of the query. Pass through constructor, not modifiable */
  private final String namespace;

  /*** Id of the query. Default is -1. Passthrough constructor, not modifiable */
  private final String id;

  /***
   * Constructor
   * @param namespace namespace of the query
   * @param id id of the query. default is -1
   */
  public NamespacedKey(String namespace, String id) {
    this.namespace = namespace;
    this.id = id;
  }

  public String namespace() {
    return namespace;
  }

  public String id() {
    return id;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NamespacedKey that = (NamespacedKey) o;
    return Objects.equal(namespace, that.namespace) &&
        Objects.equal(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(namespace, id);
  }
}
