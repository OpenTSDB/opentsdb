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
package net.opentsdb.query.interpolation;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.reflect.TypeToken;

import net.opentsdb.query.interpolation.QueryInterpolatorConfig;

/**
 * A Jackson {@link TypeIdResolver} that allows for using just the 
 * {@link QueryInterpolatorConfig}'s base name as the "configType" field 
 * in a JSON config.
 * E.g.
 * {@code
 * {
 *   "configType":"NumericInterpolatorConfig",
 *   "parallelExecutors":8
 * }
 * }
 * 
 * Thanks: https://www.thomaskeller.biz/blog/2013/09/10/custom-polymorphic-type-handling-with-jackson/
 * 
 * @since 3.0
 */
public class QueryInterpolatorConfigResolver implements TypeIdResolver{
  
  /** The package name (lets us move it around later) */
  final static String PACKAGE = 
      QueryInterpolatorConfig.class.getPackage().getName();
  
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
  public JavaType typeFromId(final DatabindContext ctx, final String id)
      throws IOException {
    return TypeFactory.defaultInstance()
      .constructSpecializedType(base_type, typeOf(id).getRawType());
  }
  /**
   * Attempts to locate the given {@link QueryExecutorConfig} Config class using
   * either the simple version of a {@link QueryExecutorConfig} or the 
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
      // TODO - try other locations too.
      //final Class<?> clazz = ClassUtil.findClass(name); // aw where'd it go?
      final Class<?> clazz = Class.forName(name);
      return TypeToken.of(clazz);
    } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Unable to find Config "
            + "implementation: '" + name + "'");
    }
  }
  
  @Override
  public String getDescForKnownTypeIds() {
    // TODO Auto-generated method stub
    return null;
  }
  
}