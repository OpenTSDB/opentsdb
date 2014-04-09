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

package net.opentsdb.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

/**
 * <p>Title: PluginJARFactory</p>
 * <p>Description: Creates an OpenTSDB plugin jar on the fly</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.utils.PluginJARFactory</code></p>
 */

public class PluginJARFactory {
	/** The stack of created files, in a thread local so we can run tests in parallel */
	private static final ThreadLocal<Stack<File>> jarFileStack = new ThreadLocal<Stack<File>>() {
		@Override
		protected Stack<File> initialValue() {			
			return new Stack<File>();
		}
	};
	
	/**
	 * Creates the plugin jar with the <b><code>Services</code></b> sub-folder
	 * @param jarFile The file the jar content will be written to
	 * @param classPairs A map of sets of impl classes keyed by the iface
	 * @param deleteOnExit If true, file will be marked to be deleted on exit
	 */
	private static void pluginJar(File jarFile, Map<String, Set<String>> classPairs, boolean deleteOnExit) {
		FileOutputStream fos = null;
		JarOutputStream jos = null;
		try {
			jarFileStack.get().push(jarFile);
			if(deleteOnExit) jarFile.deleteOnExit();		
			StringBuilder manifest = new StringBuilder();
			manifest.append("Manifest-Version: 1.0\n");
			ByteArrayInputStream bais = new ByteArrayInputStream(manifest.toString().getBytes());
			Manifest mf = new Manifest(bais);
			fos = new FileOutputStream(jarFile, false);
			jos = new JarOutputStream(fos, mf);
			for(Map.Entry<String, Set<String>> entry: classPairs.entrySet()) {
				String iface = entry.getKey();
				jos.putNextEntry(new ZipEntry("META-INF/services/" + iface));
				for(String impl: entry.getValue()) {
					jos.write((impl + "\n").getBytes());
				}
			}
			jos.flush();
			jos.closeEntry();				
			jos.close();
			fos.flush();
			fos.close();
		} catch (Exception e) {
			throw new RuntimeException("Failed to Plugin Jar for [" + jarFile.getAbsolutePath() + "]", e);
		} finally {
			if(jos!=null) try { jos.close(); } catch (Exception e) {}
			if(fos!=null) try { fos.close(); } catch (Exception e) {}
		}		
		
	}

	
	
	/**
	 * Pops the file off the top of the stack and deletes it
	 */
	public static void popAndDel() {
		if(jarFileStack.get().isEmpty()) return;
		try {
			jarFileStack.get().pop().delete();
		} catch (Exception ex) { /* No Op */ }
	}
	
	/**
	 * Pops the file off the top of the stack and deletes it if it's name is the same as the passed name
	 * @param name The expected name of the file on the top of the stack
	 */
	public static void popAndDel(String name) {
		if(jarFileStack.get().isEmpty()) return;
		if(jarFileStack.get().peek().getName().equals(name)) {
			jarFileStack.get().pop().delete();
		}
		if(jarFileStack.get().isEmpty()) {
			jarFileStack.remove();
		}
	}
	
	/**
	 * Clears the stack
	 */
	public static void purge() {
		while(!jarFileStack.get().isEmpty()) {
			popAndDel();
		}
		jarFileStack.remove();
	}
	
	/**
	 * Creates a new SpecBuilder
	 * @param fileName The file name to write the spec as
	 * @return a new SpecBuilder
	 */
	public static SpecBuilder newBuilder(String fileName) {
		return new SpecBuilder(fileName);
	}
	
	/**
	 * No Ctor
	 */
	private PluginJARFactory() {}
	
	/**
	 * <p>Title: SpecBuilder</p>
	 * <p>Description: A builder for prepping the creation of a service jar</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.utils.PluginJARFactory.SpecBuilder</code></p>
	 */
	public static class SpecBuilder {
		/** The array lists the builder stores the iface and impl when building */
		private final Map<String, Set<String>> classDefs = new HashMap<String, Set<String>>();
		/** The file name to write the spec as */
		private final File specFile;
		/** Indicates if we should mark the jar for delete-on-exit. Defaults to true. */
		private boolean deleteOnExit = true;
		/**
		 * Creates a new SpecBuilder
		 */
		private SpecBuilder(String fileName) {
			this.specFile = new File(fileName);
		}
		
		/**
		 * Builds and writes the configured jar file 
		 */
		public void build() {
			pluginJar(specFile, classDefs, deleteOnExit);
		}
		
		/**
		 * Sets the delete on exit for the jar file created from this builder
		 * @param del true to enable, false to disable
		 * @return this builder
		 */
		public SpecBuilder deleteOnExit(boolean del) {
			deleteOnExit = del;
			return this;
		}
		
		/**
		 * Adds an interface/parent and implementation pair to include in the Service spec
		 * @param iface The class name of the parent class or interface
		 * @param impl The class name[s] of the service implementation
		 * @return this builder
		 */
		public SpecBuilder service(String iface, String...impl) {
			if(iface==null || iface.trim().isEmpty()) throw new IllegalArgumentException("The passed iface was null or empty");
			
			Set<String> impls = classDefs.get(iface.trim());
			if(impls==null) {
				impls = new LinkedHashSet<String>();
				classDefs.put(iface.trim(), impls);
			}
			for(String s: impl) {
				if(s!=null) {
					impls.add(s.trim());
				}
			}
			return this;
		}
		
		/**
		 * Adds an interface/parent and implementation pair to include in the Service spec
		 * @param iface The class of the parent class or interface
		 * @param impl The class[es] of the service implementation
		 * @return this builder
		 */
		public SpecBuilder service(Class<?> iface, Class<?>...impl) {
			if(iface==null) throw new IllegalArgumentException("The passed iface was null");
			if(impl==null) throw new IllegalArgumentException("The passed impl was null");
			return service(iface.getClass().getName(), classNameArr(impl));
		}

		/**
		 * Adds an interface/parent and implementation pair to include in the Service spec
		 * @param iface The class name of the parent class or interface
		 * @param impl The class[es] of the service implementation
		 * @return this builder
		 */
		public SpecBuilder service(String iface, Class<?>...impl) {
			if(impl==null) throw new IllegalArgumentException("The passed impl was null");
			return service(iface, classNameArr(impl));
		}

		/**
		 * Adds an interface/parent and implementation pair to include in the Service spec
		 * @param iface The class of the parent class or interface
		 * @param impl The class name[s] of the service implementation
		 * @return this builder
		 */
		public SpecBuilder service(Class<?> iface, String...impl) {
			if(iface==null) throw new IllegalArgumentException("The passed iface was null");
			return service(iface.getClass().getName(), impl);
		}
		
		/**
		 * Converts an array of classes to an array of class names
		 * @param classes The classes
		 * @return the array of names
		 */
		private static String[] classNameArr(Class<?>...classes) {
			Set<String> names = new LinkedHashSet<String>();
			for(Class<?> cl: classes) {
				if(cl!=null) {
					names.add(cl.getName());
				}
			}
			return names.toArray(new String[names.size()]);
		}		
	}

}
