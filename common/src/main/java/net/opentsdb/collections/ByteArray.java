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
package net.opentsdb.collections;

public interface ByteArray {

  void setByte(final int index, final byte value);

  void setShort(final int index, final short value);

  void setInt(final int index, final int value);

  void setLong(final int index, final long value);

  byte getByte(final int index);

  short getShort(final int index);

  int getInt(final int index);

  long getLong(final int index);
}
