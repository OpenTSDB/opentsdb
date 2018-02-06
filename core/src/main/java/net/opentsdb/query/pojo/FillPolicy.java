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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Specification of how to deal with missing intervals when downsampling.
 * @since 2.2
 */
public enum FillPolicy {
  NONE("none"),
  ZERO("zero"),
  NOT_A_NUMBER("nan"),
  NULL("null"),
  SCALAR("scalar");

  // The user-friendly name of this policy.
  private final String name;

  FillPolicy(final String name) {
    this.name = name;
  }

  /**
   * Get this fill policy's user-friendly name.
   * @return this fill policy's user-friendly name.
   */
  @JsonValue
  public String getName() {
    return name;
  }

  /**
   * Get an instance of this enumeration from a user-friendly name.
   * @param name The user-friendly name of a fill policy.
   * @return an instance of {@link FillPolicy}, or {@code null} if the name
   * does not match any instance.
   * @throws IllegalArgumentException if the name doesn't match a policy
   */
  @JsonCreator
  public static FillPolicy fromString(final String name) {
    for (final FillPolicy policy : FillPolicy.values()) {
      if (policy.name.equalsIgnoreCase(name)) {
        return policy;
      }
    }

    throw new IllegalArgumentException("Unrecognized fill policy: " + name);
  }
}

