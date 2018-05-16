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
package net.opentsdb.exceptions;

/**
 * An exception thrown during serialization or deserialization.
 * 
 * @since 3.0
 */
public class SerdesException extends RuntimeException {
  private static final long serialVersionUID = 7578119134399029514L;

  /**
   * Default ctor.
   * @param msg A descriptive message.
   */
  public SerdesException(final String msg) {
    super(msg);
  }
  
  /**
   * Ctor with a cause.
   * @param msg A descriptive message.
   * @param cause A non-null cause of the exception.
   */
  public SerdesException(final String msg, final Exception cause) {
    super(msg, cause);
  }
}
