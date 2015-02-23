// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Exception thrown by the HTTP handlers when presented with a bad request such
 * as missing data, invalid requests, etc.
 * <p>
 * This has been extended for 2.0 to include the HTTP status code and an 
 * optional detailed response. The default "message" field is still used for
 * short error descriptions, typically one sentence long.
 */
final class BadRequestException extends RuntimeException {

  /** The HTTP status code to return to the user 
   * @since 2.0 */
  private final HttpResponseStatus status;

  /** An optional, detailed error message 
   * @since 2.0 */
  private final String details;
  
  /** 
   * Backwards compatible constructor, sets the status code to 400, leaves 
   * the details field empty
   * @param message A brief, descriptive error message
   */
  public BadRequestException(final String message) {
    this(HttpResponseStatus.BAD_REQUEST, message, "");
  }
  
  /**
   * Constructor to wrap a source exception in a BadRequestException
   * @param cause The source exception
   * @since 2.0
   */
  public BadRequestException(final Throwable cause) {
    this(cause.getMessage(), cause);
  }
  
  /**
   * Constructor with caller supplied message and source exception
   * <b>Note:</b> This constructor will store the message from the source 
   * exception in the "details" field of the local exception.
   * @param message A brief, descriptive error message
   * @param cause The source exception if applicable
   * @since 2.0
   */
  public BadRequestException(final String message, final Throwable cause) {
    this(HttpResponseStatus.BAD_REQUEST, message, cause.getMessage(), cause);
  }
  
  /**
   * Constructor allowing the caller to supply a status code and message
   * @param status HTTP status code
   * @param message A brief, descriptive error message
   * @since 2.0
   */
  public BadRequestException(final HttpResponseStatus status, 
      final String message) {
    this(status, message, "");
  }
  
  /**
   * Constructor with caller supplied status, message and source exception
   * <b>Note:</b> This constructor will store the message from the source 
   * exception in the "details" field of the local exception.
   * @param status HTTP status code
   * @param message A brief, descriptive error message
   * @param cause The source exception if applicable
   * @since 2.0
   */
  public BadRequestException(final HttpResponseStatus status, 
      final String message, final Throwable cause) {
    this(status, message, cause.getMessage(), cause);
  }
  
  /**
   * Constructor with caller supplied status, message and details
   * @param status HTTP status code
   * @param message A brief, descriptive error message
   * @param details Details about what caused the error. Do not copy the stack
   * trace in this message, it will be included with the exception. Use this
   * for suggestions on what to fix or more error details.
   * @since 2.0
   */
  public BadRequestException(final HttpResponseStatus status, 
      final String message, final String details) {
    super(message);
    this.status = status;
    this.details = details;
  }
  
  /**
   * Constructor with caller supplied status, message, details and source
   * @param status HTTP status code
   * @param message A brief, descriptive error message
   * @param details Details about what caused the error. Do not copy the stack
   * trace in this message, it will be included with the exception. Use this
   * for suggestions on what to fix or more error details.
   * @param cause The source exception if applicable
   * @since 2.0
   */
  public BadRequestException(final HttpResponseStatus status, 
      final String message, final String details, final Throwable cause) {
    super(message, cause);
    this.status = status;
    this.details = details;
  }

  /**
   * Static helper that returns a 400 exception with the template: 
   * Missing parameter &lt;code&gt;parameter&lt;/code&gt;
   * @param paramname Name of the missing parameter
   * @return A BadRequestException
   */
  public static BadRequestException missingParameter(final String paramname) {
    return new BadRequestException("Missing parameter <code>" + paramname
                                   + "</code>");
  }

  /** @return the HTTP status code */
  public final HttpResponseStatus getStatus() {
    return this.status;
  }
  
  /** @return the details, may be an empty string */
  public final String getDetails() { 
    return this.details;
  }
  
  static final long serialVersionUID = 1365109233;
}
