// This file is part of OpenTSDB.
// Copyright (C) 2018 The OpenTSDB Authors.
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
package net.opentsdb.servlet.filter;

import javax.servlet.Filter;

import net.opentsdb.auth.Authentication;

/**
 * An implementation of the TSDB Authentication class with a servlet
 * filter.
 * 
 * @since 3.0
 */
public interface AuthFilter extends Filter, Authentication {

  /** The key used to lookup the AuthState object in the request attributes. */
  public static String AUTH_STATE_KEY = "tsdb.authstate";
  
}
