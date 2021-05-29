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

/**
 * An off heap implementation of the long array. It uses the {@link DirectByteArray} that in turn
 * stores the data off heap. It uses 0 based indexing, where index of long array is the byte offset
 * from the start address of the underlying byte array.
 *
 * @see DirectByteArray
 * @see DirectIntArray
 */
public class DirectLongArray implements DirectArray, LongArray {

  /** length of the long array */
  private int capacity;

  /** Underlying byte array */
  private final DirectByteArray byteArray;

  /**
   * Creates a {@link DirectLongArray} with the given <code>capacity</code>.
   *
   * @param capacity length of the array
   */
  public DirectLongArray(final int capacity) {
    this.capacity = capacity;
    this.byteArray = new DirectByteArray(capacity * Long.BYTES);
  }

  /**
   * Creates a {@link DirectLongArray} with the given <code>capacity</code>.
   *
   * @param capacity length of the array
   * @param encodeLength if <code>true</code>, stores the length in bytes in an extra 4 byte at the
   *     beginning of the allocated memory.
   */
  public DirectLongArray(final int capacity, final boolean encodeLength) {
    this.capacity = capacity;
    this.byteArray = new DirectByteArray(capacity * Long.BYTES, encodeLength);
  }

  /**
   * Creates a {@link DirectLongArray} from a previously allocated memory. It reads the first 4
   * bytes from the start address and store that value in {@link DirectLongArray#capacity}
   *
   * @param address {@code address} to the allocated memory.
   */
  public DirectLongArray(final long address) {
    this.byteArray = new DirectByteArray(address);
    this.capacity = byteArray.getCapacity() / Long.BYTES;
  }

  /**
   * Creates a {@link DirectLongArray} from a previously allocated memory. Optionally reads the
   * first 4 bytes from the start address and store that value in {@link DirectLongArray#capacity}
   *
   * @param address {@code address} to the allocated memory.
   * @param lengthEncoded if <code>true</code>, reads the encoded length in bytes from an extra 4
   *     byte at the beginning of the allocated memory.
   * @param defaultCapacity used as {@link DirectLongArray#capacity}, if {@code lengthEncoded} is
   *     {@code false}
   */
  public DirectLongArray(
          final long address, final boolean lengthEncoded, final int defaultCapacity) {
    this.byteArray = new DirectByteArray(address, lengthEncoded, defaultCapacity * Long.BYTES);
    this.capacity = byteArray.getCapacity() / Long.BYTES;
  }

  /**
   * Resets the array with the new <code>capacity</code>. It allocates a new set of bytes off heap
   * but reuses the Object instance on heap. This api is provided to reuse the object instance of
   * {@link DirectLongArray} and minimize the garbage collection pressure under high load. This
   * provides and option to reuse the object reference of {@link DirectLongArray} to minimize
   * garbage collection pressure.
   *
   * <p>Note: It returns the previous <code>startAddress</code> and it's the clients responsibility
   * to free the old memory. Unless it will cause memory leak. User need to check if the previous
   * address was 0 and don't try to free that. Unless it will crash the JVM.
   *
   * @param capacity length of the array
   * @return the address to the previously allocated memory.
   */
  public long init(final int capacity) {
    this.capacity = capacity;
    return this.byteArray.init(capacity * Long.BYTES);
  }

  /**
   * Resets the array with the new <code>capacity</code>. It allocates a new set of bytes off heap
   * but reuses the Object instance on heap. This api is provided to reuse the object instance of
   * {@link DirectLongArray} and minimize the garbage collection pressure under high load. This
   * provides and option to reuse the object reference of {@link DirectLongArray} to minimize
   * garbage collection pressure.
   *
   * <p>Note: It returns the previous <code>startAddress</code> and it's the clients responsibility
   * to free the old memory. Unless it will cause memory leak. User need to check if the previous
   * address was 0 and don't try to free that. Unless it will crash the JVM.
   *
   * @param capacity length of the array
   * @param encodeLength if <code>true</code>, stores the length in bytes in an extra 4 byte at the
   *     beginning of the allocated memory.
   * @return the address to the previously allocated memory.
   */
  public long init(final int capacity, final boolean encodeLength) {
    this.capacity = capacity;
    return this.byteArray.init(capacity * Long.BYTES, encodeLength);
  }

  /**
   * Resets the array with a previously allocated memory. Resets the {@link
   * DirectLongArray#capacity} field by reading the first 4 bytes from the <code>startAddress</code>
   * . This api is provided to reuse the object instance of {@link DirectLongArray} and minimize the
   * garbage collection pressure under high load. This provides and option to reuse the object
   * reference of {@link DirectLongArray} to minimize garbage collection pressure.
   *
   * <p>Note: It returns the previous <code>startAddress</code> and it's the clients responsibility
   * to free the old memory. Unless it will cause memory leak. User need to check if the previous
   * address was 0 and don't try to free that. Unless it will crash the JVM.
   *
   * @param address address to the allocated memory.
   * @return the address to the previously allocated memory.
   */
  public long init(final long address) {
    long oldAddress = byteArray.init(address);
    this.capacity = byteArray.getCapacity() / Long.BYTES;
    return oldAddress;
  }

  /**
   * Resets the array with a previously allocated memory. Resets the {@link
   * DirectLongArray#capacity} field by reading the first 4 bytes from the <code>startAddress</code>
   * This api is provided to reuse the object instance of {@link DirectLongArray} and minimize the
   * garbage collection pressure under high load. This provides and option to reuse the object
   * reference of {@link DirectLongArray} to minimize garbage collection pressure.
   *
   * <p>Note: It returns the previous <code>startAddress</code> and it's the clients responsibility
   * to free the old memory. Unless it will cause memory leak. User need to check if the previous
   * address was 0 and don't try to free that. Unless it will crash the JVM.
   *
   * @param address address to the allocated memory.
   * @param lengthEncoded if <code>true</code>, reads the encoded length in bytes from an extra 4
   *     byte at the beginning of the allocated memory.
   * @param defaultCapacity used as {@link DirectLongArray#capacity}, if {@code lengthEncoded} is
   *     {@code false}
   * @return the address to the previously allocated memory.
   */
  public long init(final long address, final boolean lengthEncoded, final int defaultCapacity) {
    long oldAddress = byteArray.init(address, lengthEncoded, defaultCapacity * Long.BYTES);
    this.capacity = byteArray.getCapacity() / Long.BYTES;
    return oldAddress;
  }

  @Override
  public int getCapacity() {
    return capacity;
  }

  @Override
  public long getAddress() {
    return byteArray.getAddress();
  }

  @Override
  public void free() {
    this.byteArray.free();
    this.capacity = 0;
  }

  @Override
  public void set(final int index, final long value) {
    byteArray.setLong(byteIndex(index), value);
  }

  @Override
  public long get(final int index) {
    return byteArray.getLong(byteIndex(index));
  }

  private int byteIndex(final int index) {
    if ((index < 0) || (index >= capacity)) {
      throw new IndexOutOfBoundsException(index + ", length: " + capacity);
    }
    return index * Long.BYTES;
  }
}
