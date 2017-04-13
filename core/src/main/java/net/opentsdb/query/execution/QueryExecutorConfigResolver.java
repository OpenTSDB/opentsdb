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
package net.opentsdb.query.execution;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.util.ClassUtil;
import com.google.common.reflect.TypeToken;

/**
 * A Jackson {@link TypeIdResolver} that allows for using just the 
 * {@link QueryExecutor}'s base name as the "executor" field in a JSON config.
 * E.g.
 * {@code
 * {
 *   "executor":"MetricShardingExecutor",
 *   "parallelExecutors":8
 * }
 * }
 * 
 * Thanks: https://www.thomaskeller.biz/blog/2013/09/10/custom-polymorphic-type-handling-with-jackson/
 * 
 * @since 3.0
 */
public class QueryExecutorConfigResolver implements TypeIdResolver{
  
  /** The package name (lets us move it around later) */
  final static String PACKAGE = 
      QueryExecutor.class.getPackage().getName();
  
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
      final Class<?> clazz = ClassUtil.findClass(name);
      return TypeToken.of(clazz);
    } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Unable to find Config "
            + "implementation: '" + name + "'");
    }
  }
}
