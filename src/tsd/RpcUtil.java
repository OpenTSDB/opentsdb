// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RpcUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RpcUtil.class);

    public static void allowedMethods(HttpMethod requestMethod, String... allowedMethods) {
        for(String method : allowedMethods) {
            LOG.debug(String.format("Trying Method: %s", method));
            if (requestMethod.getName() == method) {
                LOG.debug(String.format("Method Allowed: %s", method));
                return;
            }
        }
        throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED,
                "Method not allowed", "The HTTP method [" + requestMethod.getName() + "] is not permitted for this endpoint");
    }
}
