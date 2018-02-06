// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.servlet.resources;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;
import javax.servlet.ServletConfig;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;

import net.opentsdb.utils.JSON;

/**
 * NOTE: This is modified from Hadoop's JMXJsonServlet to run as a JAX-RS
 * resource instead of a full servlet. It uses TSD's JSON instance for
 * serialization and SLFJ for logging.
 * 
 * This servlet is based off of the JMXProxyServlet from Tomcat 7.0.14. It has
 * been rewritten to be read only and to output in a JSON format so it is not
 * really that close to the original.
 * 
 * Provides Read only web access to JMX.
 * <p>
 * This servlet generally will be placed under the /jmx URL for each
 * HttpServer.  It provides read only
 * access to JMX metrics.  The optional <code>query</code> parameter
 * may be used to query only a subset of the JMX Beans.  This query
 * functionality is provided through the
 * {@link MBeanServer#queryNames(ObjectName, javax.management.QueryExp)}
 * method.
 * <p>
 * For example <code>http://.../jmx?qry=Hadoop:*</code> will return
 * all hadoop metrics exposed through JMX.
 * <p>
 * If the <code>query</code> parameter is not formatted correctly then a
 * 400 BAD REQUEST http response code will be returned. 
 * <p>
 * The return format is JSON and in the form
 * <p>
 *  <code>
 *  {
 *    "beans" : [
 *      {
 *        "name":"bean-name"
 *        ...
 *      }
 *    ]
 *  }
 *  </code>
 *  <p>
 *  The servlet attempts to convert the the JMXBeans into JSON. Each
 *  bean's attributes will be converted to a JSON object member.
 *  
 *  If the attribute is a boolean, a number, a string, or an array
 *  it will be converted to the JSON equivalent. 
 *  
 *  If the value is a {@link CompositeData} then it will be converted
 *  to a JSON object with the keys as the name of the JSON member and
 *  the value is converted following these same rules.
 *  
 *  If the value is a {@link TabularData} then it will be converted
 *  to an array of the {@link CompositeData} elements that it contains.
 *  
 *  All other objects will be converted to a string and output as such.
 *  
 *  The bean's name and modelerType will be returned for all beans.
 *  
 *  @since 3.0
 */
@Path("api/stats/jmx")
public class JMXResource {
  private static final Logger LOG = LoggerFactory.getLogger(JMXResource.class);

  /** The mbean server instance */
  protected transient MBeanServer mbean_server = null;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public String get(final @Context ServletConfig servletConfig, 
      final @Context HttpServletRequest request) {
    try {
      if (mbean_server == null) {
        synchronized (this) {
          if (mbean_server == null) {
            mbean_server = ManagementFactory.getPlatformMBeanServer();
          }
        }
      }
      final ByteArrayOutputStream output = new ByteArrayOutputStream();
      final JsonGenerator json = JSON.getFactory().createGenerator(output);
      
      json.writeStartObject();
      if (mbean_server == null) {
        throw new WebApplicationException("No MBeanServer could be found",
            Response.Status.INTERNAL_SERVER_ERROR);
      }
      String qry = request.getParameter("query");
      if (qry == null) {
        qry = "*:*";
      }
      listBeans(json, new ObjectName(qry));
      json.close();
      return output.toString();
    } catch (IOException e) {
      LOG.error("Caught an exception while processing JMX request", e);
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    } catch (MalformedObjectNameException e) {
      LOG.error("Caught an exception while processing JMX request", e);
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Iterates through the MBeans matching the query parameter and prints the
   * details to the JSON generator.
   * @param json A non-null generator to write to.
   * @param query A non-null object name query. Use "*:*" to fetch everything.
   * @throws IOException If something goes pear shaped.
   */
  private void listBeans(final JsonGenerator json, final ObjectName query) 
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Listing beans for " + query);
    }
    final Set<ObjectName> names = mbean_server.queryNames(query, null);

    json.writeArrayFieldStart("beans");
    final Iterator<ObjectName> it = names.iterator();
    while (it.hasNext()) {
      final ObjectName oname = it.next();
      MBeanInfo minfo;
      String code;
      try {
        minfo = mbean_server.getMBeanInfo(oname);
        code = minfo.getClassName();
        try {
          if ("org.apache.commons.modeler.BaseModelMBean".equals(code)) {
            code = (String) mbean_server.getAttribute(oname, "modelerType");
          }
        } catch (AttributeNotFoundException e) {
          //Ignored the modelerType attribute was not found, so use the class name instead.
        } catch (MBeanException e) {
          //The code inside the attribute getter threw an exception so log it, and
          // fall back on the class name
          LOG.warn("Getting attribute modelerType of " + oname + " threw an exception", e);
        } catch (RuntimeException e) {
          //For some reason even with an MBeanException available to them Runtime exceptions
          //can still find their way through, so treat them the same as MBeanException
          LOG.warn("Getting attribute modelerType of " + oname + " threw an exception", e);
        } catch (ReflectionException e) {
          //This happens when the code inside the JMX bean (setter?? from the java docs)
          //threw an exception, so log it and fall back on the class name
          LOG.warn("Getting attribute modelerType of " + oname + " threw an exception", e);
        }
      } catch (InstanceNotFoundException e) {
        //Ignored for some reason the bean was not found so don't output it
        continue;
      } catch (IntrospectionException e) {
        //This is an internal error, something odd happened with reflection so log it and
        //don't output the bean.
        LOG.error("Problem while trying to process JMX query: " + query 
            + " with MBean " + oname, e); 
        continue;
      } catch (ReflectionException e) {
        //This happens when the code inside the JMX bean threw an exception, so log it and
        //don't output the bean.
        LOG.error("Problem while trying to process JMX query: " + query 
            + " with MBean " + oname, e);
        continue;
      }

      json.writeStartObject();
      json.writeStringField("domain", oname.getDomain());
      json.writeObjectField("properties", oname.getKeyPropertyList());
      //System.out.println(JSON.serializeToString(oname));
      // can't be null - I think

      json.writeStringField("modelerType", code);

      final MBeanAttributeInfo[] attrs = minfo.getAttributes();
      for (int i = 0; i < attrs.length; i++) {
        writeAttribute(json, oname, attrs[i]);
      }
      json.writeEndObject();
    }
    json.writeEndArray();
  }
  
  /**
   * Fetches and writes the given attribute for the given MBean object.
   * @param json A non-null JSON generator to write to.
   * @param oname A non-null MBean object name. Must be an explicit MBean.
   * @param attr A non-null attribute belonging to the given MBean object name.
   * @throws IOException If something goes pear shaped.
   */
  private void writeAttribute(final JsonGenerator json, 
                              final ObjectName oname, 
                              final MBeanAttributeInfo attr) throws IOException {
    if (!attr.isReadable()) {
      return;
    }
    final String attribute_name = attr.getName();
    if ("modelerType".equals(attribute_name)) {
      return;
    }
    if (attribute_name.indexOf("=") >= 0 || attribute_name.indexOf(":") >= 0
        || attribute_name.indexOf(" ") >= 0) {
      return;
    }
    Object value = null;
    try {
      value = mbean_server.getAttribute(oname, attribute_name);
    } catch (RuntimeMBeanException e) {
      // UnsupportedOperationExceptions happen in the normal course of business,
      // so no need to log them as errors all the time.
      if (e.getCause() instanceof UnsupportedOperationException) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("getting attribute " + attribute_name + " of " + oname 
              + " threw an exception: " + e.getMessage());
        }
      } else {
        LOG.warn("Getting attribute " + attribute_name + " of " + oname 
            + " threw an exception", e);
      }
      return;
    } catch (AttributeNotFoundException e) {
      //Ignored the attribute was not found, which should never happen because the bean
      //just told us that it has this attribute, but if this happens just don't output
      //the attribute.
      return;
    } catch (MBeanException e) {
      //The code inside the attribute getter threw an exception so log it, and
      // skip outputting the attribute
      LOG.warn("Getting attribute " + attribute_name + " of " + oname + " threw an exception", e);
      return;
    } catch (RuntimeException e) {
      //For some reason even with an MBeanException available to them Runtime exceptions
      //can still find their way through, so treat them the same as MBeanException
      LOG.warn("Getting attribute " + attribute_name + " of " + oname + " threw an exception", e);
      return;
    } catch (ReflectionException e) {
      //This happens when the code inside the JMX bean (setter?? from the java docs)
      //threw an exception, so log it and skip outputting the attribute
      LOG.warn("Getting attribute " + attribute_name + " of " + oname + " threw an exception", e);
      return;
    } catch (InstanceNotFoundException e) {
      //Ignored the mbean itself was not found, which should never happen because we
      //just accessed it (perhaps something unregistered in-between) but if this
      //happens just don't output the attribute.
      return;
    }

    writeAttribute(json, attribute_name, value);
  }
  
  /**
   * Helper that writes an attribute name and the given object to the JSON 
   * generator.
   * @param json A non-null JSON generator to write to.
   * @param attName A non-null string to use as a field key.
   * @param value An object (may be null) to write as a JSON field object.
   * @throws IOException If something goes pear shaped.
   */
  private void writeAttribute(final JsonGenerator json, 
                              final String attName, 
                              final Object value) throws IOException {
    json.writeFieldName(attName);
    writeObject(json, value);
  }
  
  /**
   * Parses the attribute value based on it's class type and emits a useful
   * object that can be serialized in a JSON representation. In particular this
   * class has handling for {@link TabularData} and {@link CompositeData}.
   * <p>
   * Also converts floating point non-finite values to Strings instead of 
   * unquoted values to avoid problems with standards based parsers (thanks
   * Jackson!). 
   * @param json A non-null JSON generator to write to.
   * @param value A non-null attribute object returned from 
   * {@code mBeanServer.getAttribute(oname, attName);}
   * @throws IOException If something goes pear shaped.
   */
  private void writeObject(final JsonGenerator json, final Object value) 
      throws IOException {
    if (value == null) {
      json.writeNull();
    } else {
      final Class<?> c = value.getClass();
      if (c.isArray()) {
        json.writeStartArray();
        int len = Array.getLength(value);
        for (int j = 0; j < len; j++) {
          Object item = Array.get(value, j);
          writeObject(json, item);
        }
        json.writeEndArray();
      } else if (value instanceof Number) {
        Number n = (Number) value;
        if ((n instanceof Double && !Double.isFinite((double) n)) ||
            n instanceof Float && !Float.isFinite(((float) n))) {
          json.writeString(n.toString());
        } else {
          json.writeNumber(n.toString());
        }
      } else if (value instanceof Boolean) {
        Boolean b = (Boolean) value;
        json.writeBoolean(b);
      } else if (value instanceof CompositeData) {
        CompositeData cds = (CompositeData) value;
        CompositeType comp = cds.getCompositeType();
        Set<String> keys = comp.keySet();
        json.writeStartObject();
        for (String key: keys) {
          writeAttribute(json, key, cds.get(key));
        }
        json.writeEndObject();
      } else if (value instanceof TabularData) {
        TabularData tds = (TabularData) value;
        json.writeStartArray();
        for (Object entry : tds.values()) {
          writeObject(json, entry);
        }
        json.writeEndArray();
      } else {
        json.writeString(value.toString());
      }
    }
  }
}
