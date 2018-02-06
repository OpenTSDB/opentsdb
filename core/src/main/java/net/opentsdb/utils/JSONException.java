// This file is part of OpenTSDB.
// Copyright (C) 2013-2017  The OpenTSDB Authors.
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
package net.opentsdb.utils;

/**
 * Exception class used to wrap the myriad of typed exceptions thrown by 
 * Jackson.
 * @since 2.0
 */
public final class JSONException extends RuntimeException {

  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   */
  public JSONException(final String msg) {
    super(msg);
  }
  
  /**
   * Constructor.
   * @param cause The exception that caused this one to be thrown.
   */
  public JSONException(final Throwable cause) {
    super(cause);
  }
  
  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   * @param cause The exception that caused this one to be thrown.
   */
  public JSONException(final String msg, final Throwable cause) {
    super(msg, cause);
  }
  
  private static final long serialVersionUID = 1365518940;
}
