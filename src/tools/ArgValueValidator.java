// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
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
package net.opentsdb.tools;

import net.opentsdb.tools.ConfigArgP.ConfigurationItem;

/**
 * <p>Title: ArgValueValidator</p>
 * <p>Description: Defines a class that can validate a value instance of a declared ConfigMetaType</p> 
 */
public interface ArgValueValidator {
  /**
   * Validates the passed configuration item
   * @param citem The item to validate
   */
  public void validate(ConfigurationItem citem);
  
  /** A static exception that needs no stack trace or whatever */
  public static final Exception EX = new Exception();
}
