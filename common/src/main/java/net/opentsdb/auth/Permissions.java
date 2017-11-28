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
package net.opentsdb.auth;

/**
 * The Permissions used within the OpenTSDB Code.
 * Any authorization plugins need to be able to respond to inquiries on these permissions. Plugins and other
 * third-party code may implement additional permissions.
 *
 * @author jonathan.creasy
 * @since 2.4.0
 *
 */
public enum Permissions {
    TELNET_PUT, HTTP_PUT, HTTP_QUERY,
    CREATE_TAGK,CREATE_TAGV, CREATE_METRIC;
}
