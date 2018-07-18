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
package net.opentsdb.uid;

import com.google.common.base.Strings;

import net.opentsdb.storage.WriteStatus.WriteState;

/**
 * A response from an assignment that contains either a non-null UID
 * with a null error, or a null UID with a null error. This replaces
 * the old exception throwing code so it's quite a bit faster.
 * 
 * @since 3.0
 */
public interface IdOrError {

  /** @return The UID if successfully assigned, null if there was an 
   * an error, in which case {@link #error()} must not be null. */
  public byte[] id();
  
  /** @return An error if assignment failed, null if assignment 
   * succeeded in which case {@link #id()} must not be null. */
  public String error();
  
  /** @return The write state used to determine how to handle datum 
   * that do not return an ID. */
  public WriteState state();
  
  /** @return An optional exception for errors. */
  public Throwable exception();
  
  /**
   * Wraps the ID in the interface.
   * @param id A non-null and non-empty ID.
   * @return The wrapped ID.
   * @throws IllegalArgumentException if the ID was null or empty.
   */
  public static IdOrError wrapId(final byte[] id) {
    if (id == null || id.length < 1) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    
    return new IdOrError() {
      
      @Override
      public byte[] id() {
        return id;
      }

      @Override
      public String error() {
        return null;
      }
      
      @Override
      public WriteState state() {
        return WriteState.OK;
      }
      
      @Override
      public Throwable exception() { 
        return null; 
      }
      
    };
  }
  
  /**
   * Wraps the retry in the interface.
   * @param message An optional message string.
   * @return The wrapped ID.
   */
  public static IdOrError wrapRetry(final String message) {
    return new IdOrError() {

      @Override
      public byte[] id() {
        return null;
      }

      @Override
      public String error() {
        return message;
      }
      
      @Override
      public WriteState state() {
        return WriteState.RETRY;
      }
      
      @Override
      public Throwable exception() { 
        return null; 
      }
      
    };
  }
  
  /**
   * Wraps the rejection in the interface.
   * @param message A non-null and non-empty error string.
   * @param t An optional exception.
   * @return The wrapped ID.
   * @throws IllegalArgumentException if the string was null or empty.
   */
  public static IdOrError wrapRejected(final String message) {
    if (Strings.isNullOrEmpty(message)) {
      throw new IllegalArgumentException("Error string cannot be null "
          + "or empty.");
    }
    
    return new IdOrError() {

      @Override
      public byte[] id() {
        return null;
      }

      @Override
      public String error() {
        return message;
      }
      
      @Override
      public WriteState state() {
        return WriteState.REJECTED;
      }
      
      @Override
      public Throwable exception() { 
        return null; 
      }
      
    };
  }
  
  /**
   * Wraps the error in the interface.
   * @param error A non-null and non-empty error string.
   * @return The wrapped ID.
   * @throws IllegalArgumentException if the string was null or empty.
   */
  public static IdOrError wrapError(final String error) {
    return wrapError(error, null);
  }
  
  /**
   * Wraps the error in the interface.
   * @param error A non-null and non-empty error string.
   * @param t An optional exception.
   * @return The wrapped ID.
   * @throws IllegalArgumentException if the string was null or empty.
   */
  public static IdOrError wrapError(final String error, final Throwable t) {
    if (Strings.isNullOrEmpty(error)) {
      throw new IllegalArgumentException("Error string cannot be null "
          + "or empty.");
    }
    
    return new IdOrError() {

      @Override
      public byte[] id() {
        return null;
      }

      @Override
      public String error() {
        return error;
      }
      
      @Override
      public WriteState state() {
        return WriteState.ERROR;
      }
      
      @Override
      public Throwable exception() { 
        return t; 
      }
      
    };
  }
}
