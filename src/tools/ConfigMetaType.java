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
import java.net.URL;
import java.net.URLConnection;
import java.util.LinkedHashSet;
import java.util.Set;

import net.opentsdb.tools.ConfigArgP.ConfigurationItem;

/**
 * <p>Title: ConfigMetaType</p>
 * <p>Description: Defines the recognized meta types for command line argument values</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tools.ConfigMetaType</code></p>
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
	/** A file, optionally existing */
	FILE("A file, optionally existing", new FileSystemValidator(false, false)), 
	/** An existing directory */
	EDIR("An existing directory", new FileSystemValidator(true, true)),
	/** A directory, optionally existing */
	DIR("A directory, optionally existing", new FileSystemValidator(true, false)), 	
	/** A host name or address */
	ADDR("A host name or address", new StringValidator("ADDR")),	// we could use InetAddress.getByName('') but it might be really slow.
	/** An integer value */
	INT("An integer value", new IntegerValidator(false)),
	/** A positive integer value */
	POSINT("A positive integer value", new IntegerValidator(true)),
	/** A boolean value (true|false) */
	BOOL("A boolean value (true|false)", new BooleanValidator()),
	/** A znode path name */
	ZPATH("A znode path name", new StringValidator("ZPATH"));
	
	
	/**
	 * Decodes the passed name with trim and upper
	 * @param name The name to decode
	 * @return the decoded value
	 */
	public static ConfigMetaType byName(CharSequence name) {
		if(name==null) throw new IllegalArgumentException("Null ConfigMetaType");
		String cname = name.toString().toUpperCase().trim();
		try {
			return ConfigMetaType.valueOf(cname);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Invalid ConfigMetaType [" + name + "]");
		}
	}
		
	public static void main(String[] args) {
		System.out.println(ConfigMetaType.values().length);
		for(ConfigMetaType ct: ConfigMetaType.values()) {
			System.out.println(ct.description);
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
	public void validate(ConfigurationItem citem) {
		validator.validate(citem);		
	}

	
	/**
	 * <p>Title: IntegerValidator</p>
	 * <p>Description: Validator for integers</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.tools.ConfigMetaType.IntegerValidator</code></p>
	 */
	public static class IntegerValidator implements ArgValueValidator {
		private final boolean pos;		
		/**
		 * Creates a new IntegerValidator
		 * @param pos True if integer must be positive
		 */
		public IntegerValidator(boolean pos) {
			this.pos = pos;
		}		
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.tools.ArgValueValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
		 */
		@Override
		public void validate(ConfigurationItem citem) {			
			try {
				int i = Integer.parseInt(citem.getValue());
				if(pos && i < 0) throw EX;
			} catch (Exception ex) {
				throw new IllegalArgumentException("Invalid " + (pos ? "positive " : " ") + " Integer value [" + citem.getValue() + "] for " + citem.getName());
			}
		}
	}
	
	/**
	 * <p>Title: BooleanValidator</p>
	 * <p>Description: Validator for booleans (true|false)</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.tools.ConfigMetaType.BooleanValidator</code></p>
	 */
	public static class BooleanValidator implements ArgValueValidator {
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.tools.ArgValueValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
		 */
		@Override
		public void validate(ConfigurationItem citem) {
			if(!"true".equalsIgnoreCase(citem.getValue()) && !"false".equalsIgnoreCase(citem.getValue())) {
				throw new IllegalArgumentException("Invalid Boolean value [" + citem.getValue() + "] for " + citem.getName());				
			}
		}
	}

	/**
	 * <p>Title: StringValidator</p>
	 * <p>Description: Empty validator for generic string values, used when we don't have a decent or practical validation routine</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.tools.ConfigMetaType.StringValidator</code></p>
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
		public void validate(ConfigurationItem citem) {
			if(citem.getValue()==null || citem.getValue().trim().isEmpty()) {
				throw new IllegalArgumentException("Null or empty " + name + " value for " + citem.getName());
			}
		}
	}
	
	/**
	 * <p>Title: BootLoadableClassValidator</p>
	 * <p>Description: A validator for boot time loadable class names</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.tools.ConfigMetaType.BootLoadableClass</code></p>
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
		public void validate(ConfigurationItem citem) {
			super.validate(citem);
			try {
				Class.forName(citem.getValue().trim(), true, ConfigMetaType.class.getClassLoader());
			} catch (Exception ex) {
				throw new IllegalArgumentException("Failed to load boot time class [" + citem.getValue() + "] for " + citem.getName());
			}
		}
	}
	

	/**
	 * <p>Title: FileSystemValidator</p>
	 * <p>Description: Validator for files and directories</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.tools.ConfigMetaType.FileSystemValidator</code></p>
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
		public void validate(ConfigurationItem citem) {
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
	 * <p>Title: FileListValidator</p>
	 * <p>Description: A validator for a list of existing files or accessible URLs</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.tools.ConfigMetaType.BootLoadableClass</code></p>
	 */
	public static class FileListValidator implements ArgValueValidator {
		/**
		 * {@inheritDoc}
		 * @see net.opentsdb.tools.ConfigMetaType.StringValidator#validate(net.opentsdb.tools.ConfigArgP.ConfigurationItem)
		 */
		@Override
		public void validate(ConfigurationItem citem) {
			String[] files = citem.getValue().split(",");
			Set<String> failed = new LinkedHashSet<String>();
			for(String file: files) {
				String name = file.trim();
				if(name.isEmpty()) continue;
				boolean isURL = false;
				URL url = null;
				try {
					url = new URL(name);
					isURL = true;
				} catch (Exception ex) { /* No Op */ }
				if(isURL) continue;
				File f = new File(name);
				if(f.exists() && f.canRead()) {
					if(f.isDirectory()) {
						failed.add(name + ":Not a file");
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
		}
		
	}

	
	
}

