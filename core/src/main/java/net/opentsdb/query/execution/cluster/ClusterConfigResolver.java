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
package net.opentsdb.query.execution.cluster;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.util.ClassUtil;
import com.google.common.reflect.TypeToken;

/**
 * A Jackson {@link TypeIdResolver} that allows for using just the 
 * {@link ClusterConfigPlugin}'s base name as the "implementation" field in a 
 * JSON config.
 * E.g.
 * {@code
 * {
 *   "implementation":"StaticClusterConfig",
 *   "clusters":[
 *     ...
 *   ]
 * }
 * }
 * 
 * Thanks: https://www.thomaskeller.biz/blog/2013/09/10/custom-polymorphic-type-handling-with-jackson/
 * 
 * @since 3.0
 */
public class ClusterConfigResolver extends TypeIdResolverBase {
  
  /** The package name (lets us move it around later) */
  final static String PACKAGE = 
      ClusterConfigPlugin.class.getPackage().getName();
  
  /** The super class, i.e. QueryExecutorConfig */
  private JavaType base_type;

  @Override
  public void init(final JavaType base_type) {
    if (base_type == null) {
      throw new IllegalArgumentException("Base type cannot be null.");
    }
    this.base_type = base_type;
  }

  @Override
  public Id getMechanism() {
    return Id.CUSTOM;
  }

  @Override
  public String idFromValue(final Object obj) {
    return idFromValueAndType(obj, obj.getClass());
  }

  @Override
  public String idFromBaseType() {
    return idFromValueAndType(null, base_type.getRawClass());
  }

  @Override
  public String idFromValueAndType(final Object obj, final Class<?> clazz) {
    if (clazz.getDeclaringClass() != null) {
      return clazz.getDeclaringClass().getSimpleName();
    } if (obj != null && obj.getClass().getDeclaringClass() != null) { 
      return obj.getClass().getDeclaringClass().getSimpleName();
    } else {
      return clazz.getSimpleName();
    }
  }

  @Override
  public JavaType typeFromId(final String type) {
    return TypeFactory.defaultInstance()
        .constructSpecializedType(base_type, typeOf(type).getRawType());
  }
  
  /**
   * Attempts to locate the given {@link ClusterConfigPlugin} Config class using
   * either the simple version of the {@link ClusterConfigPlugin} or the 
   * canonical name.
   * @param type A non-null and non-empty class name. 
   * @return A type token with the class if found.
   * @throws IllegalStateException if the class wasn't located.
   */
  public static TypeToken<?> typeOf(final String type) {
    final String name = type.contains(".") ? type : 
        PACKAGE + "." + type + "$Config";
    try {
      // TODO - search plugins as well as they likely won't be on our class
      // path.
      final Class<?> clazz = ClassUtil.findClass(name);
      return TypeToken.of(clazz);
    } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Unable to find Config "
            + "implementation: '" + name + "'");
    }
  }
}
