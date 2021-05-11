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
 * An off heap implementation of the int array. It uses the {@link DirectByteArray} that in turn
 * stores the data off heap. It uses 0 based indexing, where index of int array is the byte offset
 * from the start address of the underlying byte array.
 *
 * @see DirectByteArray
 * @see DirectLongArray
 */
public class DirectIntArray implements DirectArray {

  /** length of the int array */
  private int capacity;

  /** Underlying byte array */
  private final DirectByteArray byteArray;

  /**
   * Creates a {@link DirectIntArray} with the given <code>capacity</code>.
   *
   * @param capacity length of the array
   */
  public DirectIntArray(final int capacity) {
    this.capacity = capacity;
    this.byteArray = new DirectByteArray(capacity * Integer.BYTES);
  }

  /**
   * Creates a {@link DirectIntArray} with the given <code>capacity</code>.
   *
   * @param capacity length of the array
   * @param encodeLength if <code>true</code>, stores the length in bytes in an extra 4 byte at the
   *     beginning of the allocated memory.
   */
  public DirectIntArray(final int capacity, final boolean encodeLength) {
    this.capacity = capacity;
    this.byteArray = new DirectByteArray(capacity * Integer.BYTES, encodeLength);
  }

  /**
   * Creates a {@link DirectIntArray} from a previously allocated memory. It reads the first 4 bytes
   * from the start address and store that value in {@link DirectIntArray#capacity}
   *
   * @param address {@code address} to the allocated memory.
   */
  public DirectIntArray(final long address) {
    this.byteArray = new DirectByteArray(address);
    this.capacity = byteArray.getCapacity() / Integer.BYTES;
  }

  /**
   * Creates a {@link DirectIntArray} from a previously allocated memory. Optionally reads the first
   * 4 bytes from the start address and store that value in {@link DirectIntArray#capacity}
   *
   * @param address {@code address} to the allocated memory.
   * @param lengthEncoded if <code>true</code>, reads the encoded length in bytes from an extra 4
   *     byte at the beginning of the allocated memory.
   * @param defaultCapacity used as {@link DirectIntArray#capacity}, if {@code lengthEncoded} is
   *     {@code false}
   */
  public DirectIntArray(
          final long address, final boolean lengthEncoded, final int defaultCapacity) {
    this.byteArray = new DirectByteArray(address, lengthEncoded, defaultCapacity * Integer.BYTES);
    this.capacity = byteArray.getCapacity() / Integer.BYTES;
  }

  /**
   * Resets the array with the new <code>capacity</code>. It allocates a new set of bytes off heap
   * but reuses the Object instance on heap. This api is provided to reuse the object instance of
   * {@link DirectIntArray} and minimize the garbage collection pressure under high load. This
   * provides and option to reuse the object reference of {@link DirectIntArray} to minimize garbage
   * collection pressure.
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
    return this.byteArray.init(capacity * Integer.BYTES);
  }

  /**
   * Resets the array with the new <code>capacity</code>. It allocates a new set of bytes off heap
   * but reuses the Object instance on heap. This api is provided to reuse the object instance of
   * {@link DirectIntArray} and minimize the garbage collection pressure under high load. This
   * provides and option to reuse the object reference of {@link DirectIntArray} to minimize garbage
   * collection pressure.
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
    return this.byteArray.init(capacity * Integer.BYTES, encodeLength);
  }

  /**
   * Resets the array with a previously allocated memory. Resets the {@link DirectIntArray#capacity}
   * field by reading the first 4 bytes from the <code>startAddress</code>. This api is provided to
   * reuse the object instance of {@link DirectIntArray} and minimize the garbage collection
   * pressure under high load. This provides and option to reuse the object reference of {@link
   * DirectIntArray} to minimize garbage collection pressure.
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
    this.capacity = byteArray.getCapacity() / Integer.BYTES;
    return oldAddress;
  }

  /**
   * Resets the array with a previously allocated memory. Resets the {@link DirectIntArray#capacity}
   * field by reading the first 4 bytes from the <code>startAddress</code>. This api is provided to
   * reuse the object instance of {@link DirectIntArray} and minimize the garbage collection
   * pressure under high load. This provides and option to reuse the object reference of {@link
   * DirectIntArray} to minimize garbage collection pressure.
   *
   * <p>Note: It returns the previous <code>startAddress</code> and it's the clients responsibility
   * to free the old memory. Unless it will cause memory leak. User need to check if the previous
   * address was 0 and don't try to free that. Unless it will crash the JVM.
   *
   * @param address address to the allocated memory.
   * @param lengthEncoded if <code>true</code>, reads the encoded length in bytes from an extra 4
   *     byte at the beginning of the allocated memory.
   * @param defaultCapacity used as {@link DirectIntArray#capacity}, if {@code lengthEncoded} is
   *     {@code false}
   * @return the address to the previously allocated memory.
   */
  public long init(final long address, final boolean lengthEncoded, final int defaultCapacity) {
    long oldAddress = byteArray.init(address, lengthEncoded, defaultCapacity * Integer.BYTES);
    this.capacity = byteArray.getCapacity() / Integer.BYTES;
    return oldAddress;
  }

  public void set(final int index, final int value) {
    byteArray.setInt(byteIndex(index), value);
  }

  public int get(final int index) {
    return byteArray.getInt(byteIndex(index));
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

  /**
   * Does the boundary check and offsets the index from the {@code startAddress}
   *
   * @param index <code>index</code> of the int array being accessed.
   * @return index of the byte array being accessed.
   * @throws IndexOutOfBoundsException if {@code index < 0} or if it's accessing memory beyond the
   *     allocated length
   */
  private int byteIndex(int index) {
    if ((index < 0) || (index >= capacity)) {
      throw new IndexOutOfBoundsException(index + ", length: " + capacity);
    }
    return index * Integer.BYTES;
  }
}
