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
 * An off heap implementation of the byte array. It uses the {@link sun.misc.Unsafe} native api to
 * write and read values off heap.
 *
 * <p>Upon initialization, it allocates contiguous bytes of memory off the JVM heap and stores the
 * address to the allocated memory as a field. It reserves 4 extra bytes at the beginning of the
 * allocated memory and stores the total off heap length in bytes as an int value in it. It's
 * helpful to bootstrap a new instance of the array just from the address. It uses 0-based indexing,
 * where index is the byte offset from the start address of the array.
 *
 * <p>For example, if you create an array of size n, it will allocate, n+4 bytes of memory, and
 * store the value n+4 in the first 4 bytes of it.
 *
 * @see DirectIntArray
 * @see DirectLongArray
 */
public class DirectByteArray implements DirectArray, ByteArray {

  /** length of the byte array */
  private int capacity;

  /** address of the allocated memory block */
  private long address;

  /**
   * 4 extra bytes reserved at the beginning of the allocated memory. It stores the total allocated
   * bytes for this array,
   */
  private int reservedBytes;

  /**
   * Creates a {@link DirectByteArray} with the given <code>capacity</code>. If the <code>capacity
   * </code> is greater than zero, it allocates a memory of size (<code>capacity</code> + 4) bytes
   * and assigns the address to {@link DirectByteArray#address}. Otherwise, it doesn't do any
   * allocation and the value {@link DirectByteArray#address} would be zero.
   *
   * @param capacity length of the array in bytes
   */
  public DirectByteArray(final int capacity) {
    this.init(capacity, true);
  }

  /**
   * Creates a {@link DirectByteArray} with the given <code>capacity</code>. If the <code>capacity
   * </code> is greater than zero, it allocates a memory of size (<code>capacity</code> + 4
   * (optional)) bytes and assigns the address to {@link DirectByteArray#address}. Otherwise, it
   * doesn't do any allocation and the value {@link DirectByteArray#address} would be zero.
   *
   * @param capacity length of the array in bytes
   * @param encodeLength if <code>true</code>, stores the length in bytes in an extra 4 byte at the
   *     beginning of the allocated memory.
   */
  public DirectByteArray(final int capacity, final boolean encodeLength) {
    this.init(capacity, encodeLength);
  }

  /**
   * Creates a {@link DirectByteArray} from a previously allocated memory. It reads the first 4
   * bytes from the start address and store that value in {@link DirectByteArray#capacity}
   *
   * @param address address to the allocated memory.
   */
  public DirectByteArray(final long address) {
    this.init(address, true, 0);
  }

  /**
   * Creates a {@link DirectByteArray} from a previously allocated memory. Optionally reads the
   * encoded length from the first 4 bytes from the start address and store that value in {@link
   * DirectByteArray#capacity}
   *
   * @param address address to the allocated memory.
   * @param lengthEncoded if <code>true</code>, reads the encoded length in bytes from an extra 4
   *     byte at the beginning of the allocated memory.
   * @param defaultCapacity used as {@link DirectByteArray#capacity}, if {@code lengthEncoded} is
   *     {@code false}
   */
  public DirectByteArray(
          final long address, final boolean lengthEncoded, final int defaultCapacity) {
    this.init(address, lengthEncoded, defaultCapacity);
  }

  /**
   * Initializes a new array with {@code capacity}. It allocates a new set of bytes off heap but
   * reuses the Object instance on heap. This api is provided to reuse the object instance of {@link
   * DirectByteArray} and minimize the garbage collection pressure under high load. This provides
   * and option to reuse the object reference of {@link DirectByteArray} to minimize garbage
   * collection pressure.
   *
   * <p>Note: It returns the previous <code>startAddress</code> and it's the clients responsibility
   * to free the old memory. Unless it will cause memory leak. User need to check if the previous
   * address was 0 and don't try to free that. Unless it will crash the JVM.
   *
   * @param capacity length of the array in bytes
   * @param encodeLength if <code>true</code>, stores the length in bytes in an extra 4 byte at the
   *     beginning of the allocated memory.
   * @return the address to the previously allocated memory.
   */
  public long init(final int capacity, final boolean encodeLength) {
    long oldAddress = address;
    this.capacity = capacity;
    if (encodeLength) {
      reservedBytes = Integer.BYTES;
    }
    if (capacity > 0) {
      int allocatedBytes = reservedBytes + capacity;
      this.address = DirectArray.malloc(allocatedBytes);
      if (encodeLength) {
        unsafe.putInt(address, allocatedBytes);
      }
    } else {
      this.address = 0;
    }
    return oldAddress;
  }

  /**
   * Initializes a new array with {@code capacity}. It allocates a new set of bytes off heap but
   * reuses the Object instance on heap. This api is provided to reuse the object instance of {@link
   * DirectByteArray} and minimize the garbage collection pressure under high load. This provides
   * and option to reuse the object reference of {@link DirectByteArray} to minimize garbage
   * collection pressure.
   *
   * <p>Note: It returns the previous <code>startAddress</code> and it's the clients responsibility
   * to free the old memory. Unless it will cause memory leak. User need to check if the previous
   * address was 0 and don't try to free that. Unless it will crash the JVM.
   *
   * @param capacity length of the array in bytes
   * @return the address to the previously allocated memory.
   */
  public long init(final int capacity) {
    return init(capacity, true);
  }

  /**
   * Resets this array reference to a previously allocated memory address. Resets the {@link
   * DirectByteArray#capacity} field by reading the first 4 bytes from the <code>startAddress</code>
   * This provides and option to reuse the object reference of {@link DirectByteArray} to minimize
   * garbage collection pressure.
   *
   * <p>Note: It returns the previous <code>startAddress</code> and it's the clients responsibility
   * to free the old memory. Unless it will cause memory leak. User need to check if the previous
   * address was 0 and don't try to free that. Unless it will crash the JVM
   *
   * @param startAddress address to the allocated memory.
   * @return the address to the previously allocated memory.
   */
  public long init(final long startAddress) {
    return init(startAddress, true, 0);
  }

  /**
   * Resets this reference to a previously allocated memory. Resets the {@link
   * DirectByteArray#capacity} field by reading the first 4 bytes from the <code>startAddress</code>
   * This provides and option to reuse the object reference of {@link DirectByteArray} to minimize
   * garbage collection pressure.
   *
   * <p>Note: It returns the previous <code>startAddress</code> and it's the clients responsibility
   * to free the old memory. Unless it will cause memory leak. User need to check if the previous
   * address was 0 and don't try to free that. Unless it will crash the JVM
   *
   * @param startAddress address to the allocated memory.
   * @param lengthEncoded if <code>true</code>, reads the encoded length in bytes from an extra 4
   *     byte at the beginning of the allocated memory.
   * @param defaultCapacity used as {@link DirectByteArray#capacity}, if {@code lengthEncoded} is
   *     {@code false}
   * @return the address to the previously allocated memory.
   */
  public long init(
          final long startAddress, final boolean lengthEncoded, final int defaultCapacity) {
    long oldAddress = this.address;
    this.address = startAddress;
    if (lengthEncoded) {
      reservedBytes = Integer.BYTES;
      this.capacity = startAddress > 0 ? unsafe.getInt(startAddress) - reservedBytes : 0;
    } else {
      this.capacity = startAddress > 0 ? defaultCapacity : 0;
    }
    return oldAddress;
  }

  @Override
  public int getCapacity() {
    return capacity;
  }

  @Override
  public long getAddress() {
    return address;
  }

  @Override
  public void setByte(final int index, final byte value) {
    unsafe.putByte(offset(index, 1), value);
  }

  @Override
  public void setShort(final int index, final short value) {
    unsafe.putShort(offset(index, 2), value);
  }

  @Override
  public void setInt(final int index, final int value) {
    unsafe.putInt(offset(index, 4), value);
  }

  @Override
  public void setLong(final int index, final long value) {
    unsafe.putLong(offset(index, 8), value);
  }

  @Override
  public byte getByte(final int index) {
    return unsafe.getByte(offset(index, 1));
  }

  @Override
  public short getShort(final int index) {
    return unsafe.getShort(offset(index, 2));
  }

  @Override
  public int getInt(final int index) {
    return unsafe.getInt(offset(index, 4));
  }

  @Override
  public long getLong(final int index) {
    return unsafe.getLong(offset(index, 8));
  }

  /**
   * Does the boundary check and offsets the index from the {@code startAddress}
   *
   * @param index array <code>index</code> being accessed.
   * @param bytesAccessed number if bytes being accessed.
   * @return address of the byte location being accessed.
   * @throws IndexOutOfBoundsException if {@code index < 0} or if it's accessing memory beyond the
   *     allocated length
   */
  private long offset(final int index, final int bytesAccessed) {
    if ((index < 0) || (index + bytesAccessed > capacity)) {
      throw new IndexOutOfBoundsException(index + ", length: " + capacity);
    }
    return address + reservedBytes + index;
  }

  @Override
  public void free() {
    if (address > 0) {
      unsafe.freeMemory(address);
      address = 0;
      capacity = 0;
    }
  }

}

