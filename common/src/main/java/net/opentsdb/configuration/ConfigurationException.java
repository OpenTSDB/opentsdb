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
package net.opentsdb.configuration;

public class ConfigurationException extends RuntimeException {
  private static final long serialVersionUID = 7199353656058656724L;

  /**
   * Ctor setting the message.
   * @param message A non-null and non-empty message.
   */
  public ConfigurationException(final String message) {
    super(message);
  }
  
  /**
   * Ctor setting the message and cause.
   * @param message A non-null and non-empty message.
   * @param cause A non-null cause.
   */
  public ConfigurationException(final String message, final Throwable cause) {
    super(message, cause);
  }
  
}
