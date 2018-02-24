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
package net.opentsdb.config;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * A means of parsing a complex type based on the Jackson 
 * {@code TypeReference} class that we can't use here as we can't link
 * to Jackson from the common library.
 *
 * @param <T> The type to use for serdes.
 * 
 * @since 3.0
 */
public class ConfigTypeRef<T> {

  /** The generic type of this type ref. */
  private final Type type;
  
  /**
   * Protected ctor so folks can't mess with it. Parses the generic super 
   * arguments.
   */
  protected ConfigTypeRef() {
    final Type super_class = getClass().getGenericSuperclass();
    if (super_class == null || 
        ((ParameterizedType) super_class).getActualTypeArguments() == null ||
        ((ParameterizedType) super_class).getActualTypeArguments().length < 1) {
      throw new IllegalStateException("Typereference did not have a super class.");
    }
    type = ((ParameterizedType) super_class).getActualTypeArguments()[0];
  }
  
  /** @return The generic type of the parent class. */
  public Type type() {
    return type;
  }
  
  @Override
  public String toString() {
    return "TypeRef=" + type.toString();
  }
}
