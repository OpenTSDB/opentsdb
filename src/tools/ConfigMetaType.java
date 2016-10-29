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
package net.opentsdb.tools;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.loading.MLet;

import net.opentsdb.tools.ConfigArgP.ConfigurationItem;

/**
 * <p>Title: ConfigMetaType</p>
 * <p>Description: Defines the recognized meta types for command line argument values</p> 
 */

public enum ConfigMetaType implements ArgValueValidator {
  /** An HBase table name */
  TABLE("An HBase table name", new StringValidator("TABLE")), 
  /** A zookeeper quorum spec */
  SPEC("A zookeeper quorum spec", new StringValidator("SPEC")), 
  /** A class name */
  CLASS("A class name", new StringValidator("CLASS")),
  /** A class name of a class loadable at boot time */
  BCLASS("A class name of a class loadable at boot time", new BootLoadableClass("BCLASS")),   
  /** A comma separated list of values */
  LIST("A comma separated list of values", new StringValidator("List of comma sep values")), 
  /** An existing file */
  EFILE("An existing file", new FileSystemValidator(false, true)),
  /** A list of existing files or URLs */
  FILELIST("A list of existing files or accessible URLs", new FileListValidator()),
  /** A list of existing files or URLs that comprise a classpath, and when this option is loaded, a ClassLoader MBean will be registered */
  CLASSPATH("A list of comma separated existing files or accessible URLs", new ClassPathValidator()), 
  /** A file, optionally existing */
  FILE("A file, optionally existing", new FileSystemValidator(false, false)), 
  /** An existing directory */
  EDIR("An existing directory", new FileSystemValidator(true, true)),
  /** A fully qualified file name where the parent directory must exist, but the file is optional */
  EDIRFILE("An existing directory", new DirFileSystemValidator()),
  /** A valid URL or readable file */
  URLORFILE("A valid URL or readable file", new URLOrFileValidator()),  
  /** A directory, optionally existing */
  DIR("A directory, optionally existing", new FileSystemValidator(true, false)),  
  /** A host name or address */
  ADDR("A host name or address", new StringValidator("ADDR")),  // we could use InetAddress.getByName('') but it might be really slow.
  /** An integer value */
  INT("An integer value", new IntegerValidator(Integer.MAX_VALUE)),
  /** A positive integer value */
  POSINT("A positive integer value", new IntegerValidator(0)),
  /** A non zero positive integer value */
  GTZEROINT("A non zero positive integer value", new IntegerValidator(1)),  
  /** A boolean value (true|false) */
  BOOL("A boolean value (true|false)", new BooleanValidator()),
  /** A znode path name */
  ZPATH("A znode path name", new StringValidator("ZPATH")),
  /** The read write mode */
  RWMODE("Read/Write mode specification", new ReadWriteModeValidator()),
  /** A time zone. e.g. "America/Los_Angeles"*/
  TIMEZONE("A time zone name", new TimeZoneValidator()),
  /** A list of comma separated class names that should be loadable with (or without) the assistance of a {@link #CLASSPATH} configured classloader */
  BCLASSLIST("A comma separated list of loadable classes", new ClasspathConfiguredClassList());
  
  /** The platform MBeanServer */
  public static final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
  /** Comma splitter */
  public static final Pattern COMMA_SPLITTER = Pattern.compile(",");
  
  
  /**
   * Decodes the passed name with trim and upper
   * @param name The name to decode
   * @param key The key of the item (for error reporting)
   * @return the decoded value
   */
  public static ConfigMetaType byName(CharSequence name, String key) {
    if(name==null) throw new IllegalArgumentException("Null ConfigMetaType");
    String cname = name.toString().toUpperCase().trim();
    try {
      return ConfigMetaType.valueOf(cname);
    } catch (Exception ex) {
      throw new IllegalArgumentException("Invalid ConfigMetaType [" + cname + "]. Key: [" + key + "]");
    }
  }
    
  private ConfigMetaType(String description, ArgValueValidator validator) {
    this.description = description;
    this.validator = validator;
  }
  
  /** A short description of the meta type */
  public final String description;
  /** A validator instance for the meta type  */
  public final ArgValueValidator validator;
  

  /**
   * {@inheritDoc}
   * @see net.opentsdb.tools.ArgValueValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
   */
  @Override
  public void validate(final ConfigurationItem citem) {
    validator.validate(citem);    
  }

  /**
   * <p>Title: ReadWriteModeValidator</p>
   * <p>Description: Validator for ReadWrite Modes</p> 
   */
  public static class ReadWriteModeValidator implements ArgValueValidator {
    /** The supported mode codes */
    private static final Set<String> MODES = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
        "rw",             // READ AND WRITE
        "wo",       // WRITE ONLY
        "ro"        // READ ONLY
    )));
    /**
     * Creates a new ReadWriteModeValidator
     */
    public ReadWriteModeValidator() {
      
    }   
    
    /**
     * {@inheritDoc}
     * @see net.opentsdb.tools.ArgValueValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
     */
    @Override
    public void validate(final ConfigurationItem citem) {     
      try {
        final String _mode = citem.getValue();
        if(!MODES.contains(_mode)) throw new Exception();
      } catch (Exception ex) {
        throw new IllegalArgumentException("Invalid ReadWrite Mode [" + citem.getValue() + "] for " + citem.getName());
      }
    }
  }
  
  /**
   * <p>Title: ClassListValidator</p>
   * <p>Description: Validator for loadable class lists</p> 
   */
  public static class ClassListValidator implements ArgValueValidator {
    /**
     * Creates a new ClassListValidator
     */
    public ClassListValidator() {
      
    }   
    
    /**
     * {@inheritDoc}
     * @see net.opentsdb.tools.ArgValueValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
     */
    @Override
    public void validate(final ConfigurationItem citem) {     
      final String val = citem.getValue();
      if(val.trim().isEmpty()) return;
      final String[] classNames = COMMA_SPLITTER.split(val.trim());
      for(String className: classNames) {
        className = className.trim();
        if(className.isEmpty()) continue;
        try {
          Class.forName(className);
        } catch (Exception ex) {
          throw new IllegalArgumentException("Failed to load class [" + className + "] defined in configuration item [" + citem.getName() + "]", ex);
        }
      }
    }
  }
  
  
  /**
   * <p>Title: IntegerValidator</p>
   * <p>Description: Validator for integers</p> 
   */
  public static class IntegerValidator implements ArgValueValidator {
    private final int min;    
    /**
     * Creates a new IntegerValidator
     * @param min The minumum value of the integer
     */
    public IntegerValidator(final int min) {
      this.min = min;
    }   
    /**
     * {@inheritDoc}
     * @see net.opentsdb.tools.ArgValueValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
     */
    @Override
    public void validate(final ConfigurationItem citem) {     
      try {
        int i = Integer.parseInt(citem.getValue());
        if(i < min) throw EX; 
      } catch (Exception ex) {
        throw new IllegalArgumentException("Invalid Integer value [" + citem.getValue() + "] for " + citem.getName());
      }
    }
  }
  
  /**
   * <p>Title: BooleanValidator</p>
   * <p>Description: Validator for booleans (true|false)</p> 
   */
  public static class BooleanValidator implements ArgValueValidator {
    /**
     * {@inheritDoc}
     * @see net.opentsdb.tools.ArgValueValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
     */
    @Override
    public void validate(final ConfigurationItem citem) {
      if(!"true".equalsIgnoreCase(citem.getValue()) && !"false".equalsIgnoreCase(citem.getValue())) {
        throw new IllegalArgumentException("Invalid Boolean value [" + citem.getValue() + "] for " + citem.getName());        
      }
    }
  }
  
  /**
   * <p>Title: URLOrFileValidator</p>
   * <p>Description: Validator for URLs or Files</p> 
   */
  public static class URLOrFileValidator implements ArgValueValidator {
    /**
     * {@inheritDoc}
     * @see net.opentsdb.tools.ArgValueValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
     */
    @Override
    public void validate(final ConfigurationItem citem) {
      try {
        new URL(citem.getValue());
      } catch (Exception ex) {
        File f = new File(citem.getValue());
        if(!f.exists() || !f.isFile() || !f.canRead()) {
          throw new IllegalArgumentException("Invalid URL or File [" + citem.getValue() + "]", ex);
        }
      }
    }
  }
  

  /**
   * <p>Title: StringValidator</p>
   * <p>Description: Empty validator for generic string values, used when we don't have a decent or practical validation routine</p> 
   */
  public static class StringValidator implements ArgValueValidator {
    /** Informational name */
    protected final String name;
    
    /**
     * Creates a new StringValidator
     * @param name The name of the type
     */
    public StringValidator(String name) {   
      this.name = name;
    }
    /**
     * {@inheritDoc}
     * @see net.opentsdb.tools.ArgValueValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
     */
    @Override
    public void validate(final ConfigurationItem citem) {
      if(citem.getValue()==null || citem.getValue().trim().isEmpty()) {
        throw new IllegalArgumentException("Null or empty " + name + " value for " + citem.getName());
      }
    }
  }
  
  /**
   * <p>Title: BootLoadableClassValidator</p>
   * <p>Description: A validator for boot time loadable class names</p> 
   */
  public static class BootLoadableClass extends StringValidator {
    /**
     * Creates a new BootLoadableClass
     * @param name The name of the type
     */
    public BootLoadableClass(String name) {   
      super(name);
    }
    /**
     * {@inheritDoc}
     * @see net.opentsdb.tools.ConfigMetaType.StringValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
     */
    @Override
    public void validate(final ConfigurationItem citem) {
      super.validate(citem);
      try {
        Class.forName(citem.getValue().trim(), true, ConfigMetaType.class.getClassLoader());
      } catch (Exception ex) {
        throw new IllegalArgumentException("Failed to load boot time class [" + citem.getValue() + "] for " + citem.getName());
      }
    }
  }
  
  /**
   * <p>Title: BootLoadableClassValidator</p>
   * <p>Description: A validator for a comma separated list of classnames for which a supplementary
   * classpath has been specified in a {@link #FILELIST} configuration item named the same as this one
   * but with <b><code>.classpath</code></b> appended.</p> 
   */
  public static class ClasspathConfiguredClassList implements ArgValueValidator {
    /** The template for the classloader MBean's ObjectName */
    public static final String CLASSLOADER_OBJECTNAME = "net.opentsdb.classpath:type=ClassLoader,name=%s.classpath";
    
    /**
     * Creates a new ClasspathConfiguredClassList
     */
    public ClasspathConfiguredClassList() {   
      
    }
    /**
     * {@inheritDoc}
     * @see net.opentsdb.tools.ConfigMetaType.StringValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
     */
    @Override
    public void validate(final ConfigurationItem citem) {
      Set<String> classNames = new LinkedHashSet<String>(Arrays.asList(COMMA_SPLITTER.split(citem.getValue())));
      if(classNames.isEmpty()) return;
      for(final Iterator<String> iter = classNames.iterator(); iter.hasNext();) {
        String fileName = iter.next().trim();
        if(fileName.isEmpty()) iter.remove();       
      }
      if(classNames.isEmpty()) return;
      String className = null;
      try {
        final ClassLoader CL;
        final ObjectName on = new ObjectName(String.format(CLASSLOADER_OBJECTNAME, citem.getKey()));        
        if(server.isRegistered(on)) {
          CL = server.getClassLoader(on);
        } else {
          CL = Thread.currentThread().getContextClassLoader();
        }
        for(String cl: classNames) {
          className = cl.trim();
          Class.forName(className, true, CL);
        }
        Class.forName(citem.getValue().trim(), true, ConfigMetaType.class.getClassLoader());
      } catch (Exception ex) {
        throw new IllegalArgumentException("Failed to load boot time class [" + className + "] for " + citem.getName());
      }
    }
  }
  
  /**
   * <p>Title: DirFileSystemValidator</p>
   * <p>Description: Validator for fully qualified file names where the parent directory
   * must exist but the file is optional</p> 
   */
  public static class DirFileSystemValidator implements ArgValueValidator {

    @Override
    public void validate(final ConfigurationItem citem) {
      final String name = citem.getValue();
      if(name==null || name.trim().isEmpty()) return;
      final File f = new File(citem.getValue()).getAbsoluteFile();
      final File dir = f.getParentFile();
      if(dir.exists() && dir.isDirectory()) return;
      if(!dir.exists()) throw new IllegalArgumentException("No directory named [" + dir + "] exists. Invalid value for config item:" + citem.getKey());
      if(!dir.isDirectory()) throw new IllegalArgumentException("Specified parent directory [" + dir + "] is not a directory. Invalid value for config item:" + citem.getKey());
    }
    
  }
  

  /**
   * <p>Title: FileSystemValidator</p>
   * <p>Description: Validator for files and directories</p> 
   */
  public static class FileSystemValidator implements ArgValueValidator {
    private final boolean dir;
    private final boolean mustExist;
    
    /**
     * Creates a new FileSystemValidator
     * @param dir true for validating directories, false for files
     * @param mustExist true if the target must exist, false if it can be created if it does not exist
     */
    public FileSystemValidator(boolean dir, boolean mustExist) {
      this.dir = dir;
      this.mustExist = mustExist;
    }

    /**
     * {@inheritDoc}
     * @see net.opentsdb.tools.ArgValueValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
     */
    @Override
    public void validate(final ConfigurationItem citem) {
      String value = citem.getValue();
      File target = new File(value);
      if(mustExist) {
        if(!target.exists()) throw new IllegalArgumentException((dir ? "Directory [" : " File [") + value + "] does not exist for " + citem.getName());
        if(dir) {
          if(!target.isDirectory()) throw new IllegalArgumentException(("[") + value + "] is a file not a directory for " + citem.getName());
        } else {
          if(!target.isFile()) throw new IllegalArgumentException(("[") + value + "] is a directory not a file for " + citem.getName());
        }
      } else {
        if(dir) {
          if(!target.exists()) {
            if(!target.mkdirs()) throw new IllegalArgumentException(("Could not create directory [") + value + "] for " + citem.getName());
          } else {
            if(!target.isDirectory()) throw new IllegalArgumentException(("[") + value + "] is a file not a directory for " + citem.getName());
          }
        } else {
          if(!target.getParentFile().exists()) {
            if(!target.getParentFile().mkdirs()) throw new IllegalArgumentException(("Could not create parent directory for file [") + value + "] for " + citem.getName());
          }
          
          if(!target.exists()) {
            try {           
              if(!target.createNewFile()) throw new IllegalArgumentException(("Could not create file [") + value + "] for " + citem.getName());
            } catch (IOException e) {
              throw new IllegalArgumentException(("Could not create file [") + value + "] for " + citem.getName());
            } finally {
              target.delete();
            }
          }
        }
      }
    }
  }
  
  /**
   * <p>Title: TimeZoneValidator</p>
   * <p>Description: Validates time zone typed configuration items.</p> 
   * <p><code>net.opentsdb.tools.TimeZoneValidator</code></p>
   */
  public static class TimeZoneValidator implements ArgValueValidator {
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");
    
    @Override
    public void validate(final ConfigurationItem citem) {
      final String tz = citem.getValue();
      if("DEFAULT".equalsIgnoreCase(tz)) {
        citem.setValue(TimeZone.getDefault().getDisplayName());
        return;
      }
      TimeZone timeZone = TimeZone.getTimeZone(tz);
      if("GMT".equals(timeZone.getID())) {
        if(!tz.toUpperCase().contains("GMT")) {
          throw new IllegalArgumentException("Unrecognized TimeZone [" + tz + "]");
        }
      }
    }
    
  }

  /**
   * <p>Title: FileListValidator</p>
   * <p>Description: A validator for a list of existing files or accessible URLs</p> 
   */
  public static class FileListValidator implements ArgValueValidator {
    /**
     * {@inheritDoc}
     * @see net.opentsdb.tools.ConfigMetaType.StringValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
     */
    @Override
    public void validate(final ConfigurationItem citem) {
      buildURLSet(citem);
    }
    
    /**
     * Builds a set of URLs from the configured file list and validates each one
     * @param citem The configuration item
     * @return A [possibly empty] set of URLs
     */
    protected Set<URL> buildURLSet(final ConfigurationItem citem) {
      final Set<URL> urls = new LinkedHashSet<URL>();
      String[] files = COMMA_SPLITTER.split(citem.getValue());
      Set<String> failed = new LinkedHashSet<String>();
      for(String file: files) {
        String name = file.trim();
        if(name.isEmpty()) continue;
        boolean isURL = false;
        URL url = null;
        try {
          url = new URL(name);
          isURL = true;
          urls.add(url);
        } catch (Exception ex) { /* No Op */ }
        if(isURL) continue;
        File f = new File(name);
        if(f.exists() && f.canRead()) {
//          if(f.isDirectory()) {
//            failed.add(name + ":Not a file");
//          }
          try {
            urls.add(f.toURI().toURL());
          } catch (Exception ex) {
            failed.add(name + ":Could not convert to URL");
          }
        } else {
          failed.add(name + ":Not found");
          continue;
        }
      }
      if(!failed.isEmpty()) {
        StringBuilder b = new StringBuilder("FileList Validation Failures:");
        for(String s: failed) {
          b.append("\n\t").append(s);
        }
        throw new IllegalArgumentException("Invalid Files or URLs in File List for " + citem.getName() + "\n" + b.toString());        
      }
      
      return urls;
    }
    
  }

  /**
   * <p>Title: ClassPathValidator</p>
   * <p>Description: A class path validator and ClassLoader factory</p> 
   * <p>Company: Helios Development Group LLC</p>
   */
  public static class ClassPathValidator extends FileListValidator {
    /** The template for the classloader MBean's ObjectName */
    public static final String CLASSLOADER_OBJECTNAME = "net.opentsdb.classpath:type=ClassLoader,name=%s";
    
    /**
     * {@inheritDoc}
     * @see net.opentsdb.tools.ConfigMetaType.FileListValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
     */
    @Override
    public void validate(final ConfigurationItem citem) {
      super.validate(citem);
      Set<URL> urls = buildURLSet(citem);
      final MLet classLoader = new MLet(urls.toArray(new URL[urls.size()]));
      try {
        final ObjectName on = new ObjectName(String.format(CLASSLOADER_OBJECTNAME, citem.getKey()));
        if(server.isRegistered(on)) {
          server.unregisterMBean(on);
        }
        server.registerMBean(classLoader, on);
      } catch (Exception ex) {
        throw new IllegalArgumentException("Failed to create class loader for [" + citem.getName() + "]", ex);  
      }
      
    }
  }
  
  
}
