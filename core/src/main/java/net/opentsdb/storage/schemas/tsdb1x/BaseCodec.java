// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.storage.schemas.tsdb1x;

/**
 * Base codec class for re-usable arrays.
 *
 * @since 3.0
 */
public abstract class BaseCodec implements Codec{
  protected byte[][] qualifiers;
  protected byte[][] values;
  protected int[] qualifierLengths;
  protected int[] valueLengths;

  /** The index into the arrays. */
  protected int encodedValues;

  @Override
  public void reset() {
    encodedValues = 0;
  }

  @Override
  public int encodedValues() {
    return encodedValues;
  }

  @Override
  public byte[][] qualifiers() {
    return qualifiers;
  }

  @Override
  public byte[][] values() {
    return values;
  }

  @Override
  public int[] qualifierLengths() {
    return qualifierLengths;
  }

  @Override
  public int[] valueLengths() {
    return valueLengths;
  }
}
