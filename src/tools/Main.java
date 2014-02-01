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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import net.opentsdb.BuildData;
import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.PipelineFactory;
import net.opentsdb.utils.Config;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: Main</p>
 * <p>Description: OpenTSDB fat-jar main entry point</p> 
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tools.Main</code></p>
 */

public class Main {
	/** The platform EOL string */
	public static final String EOL = System.getProperty("line.separator", "\n");
	/** Static class logger */
	private static final Logger log = LoggerFactory.getLogger(Main.class);
	/** The content prefix  */
	public static final String CONTENT_PREFIX = "queryui";

	/** The default pid file directory which is <b><code>${user.home}/.tsdb/</code></b> */
	public static final File DEFAULT_PID_DIR = new File(System.getProperty("user.home") + File.separator + ".tsdb");
	/** The default pid file which is <b><code>${user.home}/.tsdb/opentsdb.pid</code></b> */
	public static final File DEFAULT_PID_FILE = new File(DEFAULT_PID_DIR, "opentsdb.pid");

	/** The default flush interval */
	public static final short DEFAULT_FLUSH_INTERVAL = 1000;

	/** Boot classes keyed by the commands we recognize */
	public static final Map<String, Class<?>> COMMANDS;
	
	static {
		Map<String, Class<?>> tmp = new HashMap<String, Class<?>>();
		tmp.put("fsck", Fsck.class);
		tmp.put("import", TextImporter.class);
		tmp.put("mkmetric", UidManager.class); // -> shift --> set uid assign metrics "$@"
		tmp.put("query", CliQuery.class);
		tmp.put("tsd", TSDMain.class);
		tmp.put("scan", DumpSeries.class);
		tmp.put("uid", UidManager.class);
		tmp.put("exportui", UIContentExporter.class);
		COMMANDS = Collections.unmodifiableMap(tmp);
	}
	
	/**
	 * The OpenTSDB fat-jar main entry point
	 * @param args See usage banner {@link Main#mainUsage()}
	 */
	public static void main(String[] args) {
	    log.info("Starting.");
	    log.info(BuildData.revisionString());
	    log.info(BuildData.buildString());
	    try {
	      System.in.close();  // Release a FD we don't need.
	    } catch (Exception e) {
	      log.warn("Failed to close stdin", e);
	    }
	    if(args.length==0) {
	    	log.error("No command supplied");
	    	mainUsage(System.err);
	    	System.exit(-1);
	    }
	    String targetTool = args[0].toLowerCase();
	    if(!COMMANDS.containsKey(targetTool)) {
	    	log.error("Command not recognized: [" + targetTool + "]");
	    	mainUsage(System.err);
	    	System.exit(-1);	    	
	    }
	    process(targetTool, shift(args));
	    
	}
	
	
	
	private static void process(String targetTool, String[] args) {
		if("mkmetric".equals(targetTool)) {
			shift(args);
		} 
		if(!"tsd".equals(targetTool)) {			
			try {
				COMMANDS.get(targetTool).getDeclaredMethod("main", String[].class).invoke(null, new Object[] {args});
			} catch(Exception x) {
	    		log.error("Failed to call [" + targetTool + "].", x);
	    		System.exit(-1);
			}			
		} else {
			launchTSD(args);
		}
	}
	
	private static void launchTSD(String[] args) {
		ConfigArgP cap = new ConfigArgP(args);
		Config config = cap.getConfig();
		ArgP argp = cap.getArgp();
		
		    	// ==== New cl options and their config keys
		       // --pid-file: tsd.process.pid.file,
		      // --ignore-existing-pid: tsd.process.pid.ignore.existing
		      // --no-uiexport:tsd.ui.noexport
		      // --include-config: tsd.config.include

		    // All options are now correctly set in config
		try {
		    // Write the PID file
		    writePid(config.getString("tsd.process.pid.file"), config.getBoolean("tsd.process.pid.ignore.existing"));		    
		    // Export the UI content
		    if(!config.getBoolean("tsd.ui.noexport")) {
		    	loadContent(config.getString("tsd.http.staticroot"));
		    }
		    // Create the cache dir if it does not exist
		    File cacheDir = new File(config.getString("tsd.http.cachedir"));
		    if(cacheDir.exists()) {
		    	if(!cacheDir.isDirectory()) {
		    		throw new IllegalArgumentException("The http cache directory [" + cacheDir + "] is not a directory, but a file, which is bad");
		    	}
		    } else {
		    	if(!cacheDir.mkdirs()) {
		    		throw new IllegalArgumentException("Failed to create the http cache directory [" + cacheDir + "]");
		    	}
		    }
//		      StringBuilder allArgp = new StringBuilder("\n\t=================================================\n\tAll ArgP\n\t=================================================");	      
//		      for(Map.Entry<String, String> entry: argp.getParsed().entrySet()) {
//		    	  allArgp.append("\n\t").append(entry.getKey()).append("  :  [").append(entry.getValue()).append("]");
//		      }
//		      allArgp.append("\n\t").append(argp.toString());
//		      allArgp.append("\n\t=================================================\n");
//		      log.info(allArgp.toString());
		    
		} catch (Exception ex) {
			log.error("Failed to process tsd configuration", ex);
			System.exit(-1);
		}
		
		// =====================================================================
		//  Command line processing complete
		//  so the following is the TSDB startup from TSDMain
		//  after all the cl processing
		// =====================================================================
		
	    final ServerSocketChannelFactory factory;
	    if (config.getBoolean("tsd.network.async_io")) {
	      int workers = Runtime.getRuntime().availableProcessors() * 2;
	      if (config.hasProperty("tsd.network.worker_threads")) {
	        try {
	        workers = config.getInt("tsd.network.worker_threads");
	        } catch (NumberFormatException nfe) {
	          usage(argp, "Invalid worker thread count", 1);
	        }
	      }
	      factory = new NioServerSocketChannelFactory(
	          Executors.newCachedThreadPool(), Executors.newCachedThreadPool(),
	          workers);
	    } else {
	      factory = new OioServerSocketChannelFactory(
	          Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
	    }
	    
	    TSDB tsdb = null;
	    try {
	      tsdb = new TSDB(config);
	      tsdb.initializePlugins(true);
	      
	      // Make sure we don't even start if we can't find our tables.
	      tsdb.checkNecessaryTablesExist().joinUninterruptibly();

	      registerShutdownHook(tsdb);
	      final ServerBootstrap server = new ServerBootstrap(factory);

	      server.setPipelineFactory(new PipelineFactory(tsdb));
	      if (config.hasProperty("tsd.network.backlog")) {
	        server.setOption("backlog", config.getInt("tsd.network.backlog")); 
	      }
	      server.setOption("child.tcpNoDelay", 
	          config.getBoolean("tsd.network.tcp_no_delay"));
	      server.setOption("child.keepAlive", 
	          config.getBoolean("tsd.network.keep_alive"));
	      server.setOption("reuseAddress", 
	          config.getBoolean("tsd.network.reuse_address"));

	      // null is interpreted as the wildcard address.
	      InetAddress bindAddress = null;
	      if (config.hasProperty("tsd.network.bind")) {
	        bindAddress = InetAddress.getByName(config.getString("tsd.network.bind"));
	      }

	      // we validated the network port config earlier
	      final InetSocketAddress addr = new InetSocketAddress(bindAddress,
	          config.getInt("tsd.network.port"));
	      server.bind(addr);
	      log.info("Ready to serve on " + addr);
//	      if(log.isDebugEnabled()) {
		      StringBuilder allConfig = new StringBuilder("\n\t=================================================\n\tAll Config\n\t=================================================");
		      for(Map.Entry<String, String> entry: config.getMap().entrySet()) {
		    	  allConfig.append("\n\t").append(entry.getKey()).append("  :  [").append(entry.getValue()).append("]");
		      }
		      allConfig.append("\n\t=================================================\n");
		      log.info(allConfig.toString());
//	      }
	      
	    } catch (Throwable e) {
	      factory.releaseExternalResources();
	      try {
	        if (tsdb != null)
	          tsdb.shutdown().joinUninterruptibly();
	      } catch (Exception e2) {
	        log.error("Failed to shutdown HBase client", e2);
	      }
	      throw new RuntimeException("Initialization failed", e);
	    }
	    // The server is now running in separate threads, we can exit main.
		
	}

	/**
	 * Drops the first array item in the passed array.
	 * If the passed array is null or empty, returns an empty array
	 * @param args The array to shift
	 * @return the shifted array
	 */
	private static String[] shift(String[] args) {
		if(args==null || args.length==0 | args.length==1) return new String[0];
		String[] newArgs = new String[args.length-1];
		System.arraycopy(args, 1, newArgs, 0, newArgs.length);
		return newArgs;
	}
	
//	/**
//	 * Removes all entries equaling the passed arg value from the passed arg array and returns the reduced array.
//	 * @param arg The value to remove from the array
//	 * @param args The array to remove the value from
//	 * @return the possibly reduced array
//	 */
//	private static String[] remove(String arg, String[] args) {
//		List<String> keep = new ArrayList<String>();
//		for(String a: args) {
//			if(!a.equals(arg)) {
//				keep.add(a);
//			}
//		}
//		return keep.toArray(new String[keep.size()]);
//	}

	/**
	 * Prints the main usage banner
	 */
	public static void mainUsage(PrintStream ps) {
		StringBuilder b = new StringBuilder("\nUsage: java -jar [opentsdb.jar] [command] [args]\nValid commands:")
			.append("\n\tfsck: ")
			.append("\n\timport: ")
			.append("\n\tmkmetric: ")
			.append("\n\tquery: ")
			.append("\n\tscan: ")
			.append("\n\tuid: ")
			.append("\n\texportui: ");
		ps.println(b);
	}
	
	  /** Prints usage and exits with the given retval. */
	  static void usage(final ArgP argp, final String errmsg, final int retval) {
	    System.err.println(errmsg);
	    mainUsage(System.err);
	    if (argp != null) {
	      System.err.print(argp.usage());
	    }
	    System.exit(retval);
	  }
	  
	  private static void registerShutdownHook(final TSDB tsdb) {
		    final class TSDBShutdown extends Thread {
		      public TSDBShutdown() {
		        super("TSDBShutdown");
		      }
		      public void run() {
		        try {
		          tsdb.shutdown().join();
		        } catch (Exception e) {
		          LoggerFactory.getLogger(TSDBShutdown.class)
		            .error("Uncaught exception during shutdown", e);
		        }
		      }
		    }
		    Runtime.getRuntime().addShutdownHook(new TSDBShutdown());
		  }
	  
	

	/**
	 * <p>Title: UIContentExporter</p>
	 * <p>Description: Exports the queryui content from the jar to the specified directory</p> 
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.tools.Main.UIContentExporter</code></p>
	 */
	public static class UIContentExporter {
		/**
		 * Entry point for invoking the ui content exporter
		 * @param args see the ArgP
		 */
		public static void main(String[] args) {
			ArgP uiexOptions = new ArgP();
			uiexOptions.addOption("--d", "DIR", "The directory to export the UI content to");
			uiexOptions.addOption("--p", "Create the directory if it does not exist");			
			uiexOptions.parse(args);
			String dirName = uiexOptions.get("--d");
			boolean createIfNotExists = uiexOptions.has("--p");
			if(dirName==null) {
				log.error("Missing argument for target directory. Usage:  java -jar <opentsdb.jar> exportui --d <destination> [--p]");
				System.exit(1);
			}
			File f = new File(dirName);
			if(!f.exists()) {
				if(createIfNotExists) {
					if(f.mkdirs()) {
						log.info("Created exportui directory [{}]", f);
					} else {
						log.error("Failed to create target directory [{}]", f);
						System.exit(1);
					}
				} else {
					log.error("Specified target directory [{}] does not exist. You could use the --p option, or create the directory", f);
					System.exit(1);
				}
			} else {
				if(!f.isDirectory()) {
					log.error("Specified target [{}] is not a directory, but is a file. exportui cannot contine", f);
					System.exit(1);
				}
			}
			loadContent(f.getAbsolutePath());
			System.exit(0);			
		}
	}
	/**
	 * Loads the Static UI content files from the classpath JAR to the configured static root directory
	 * @param the name of the content directory to write the content to
	 */
	private static void loadContent(String contentDirectory) {	  
		File gpDir = new File(contentDirectory);
		final long startTime = System.currentTimeMillis();
		int filesLoaded = 0;
		int fileFailures = 0;
		int fileOlder = 0;
		long bytesLoaded = 0;
		String codeSourcePath = TSDMain.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		File file = new File(codeSourcePath);
		if( codeSourcePath.endsWith(".jar") && file.exists() && file.canRead() ) {
			JarFile jar = null;
			ChannelBuffer contentBuffer = ChannelBuffers.dynamicBuffer(300000);
			try {
				jar = new JarFile(file);
				final Enumeration<JarEntry> entries = jar.entries(); 
				while(entries.hasMoreElements()) {
					JarEntry entry = entries.nextElement();
					final String name = entry.getName();
					if (name.startsWith(CONTENT_PREFIX + "/")) { 
						final int contentSize = (int)entry.getSize();
						final long contentTime = entry.getTime();
						if(entry.isDirectory()) {
							new File(gpDir, name).mkdirs();
							continue;
						}
						File contentFile = new File(gpDir, name.replace(CONTENT_PREFIX + "/", ""));
						if( !contentFile.getParentFile().exists() ) {
							contentFile.getParentFile().mkdirs();
						}
						if( contentFile.exists() ) {
							if( contentFile.lastModified() < contentTime ) {
								log.debug("File in directory was newer [{}]", name);
								fileOlder++;
								continue;
							}
							contentFile.delete();
						}
						log.debug("Writing content file [{}]", contentFile );
						contentFile.createNewFile();
						if( !contentFile.canWrite() ) {
							log.warn("Content file [{}] not writable", contentFile);
							fileFailures++;
							continue;
						}
						FileOutputStream fos = null;
						InputStream jis = null;
						try {
							fos = new FileOutputStream(contentFile);
							jis = jar.getInputStream(entry);
							contentBuffer.writeBytes(jis, contentSize);
							contentBuffer.readBytes(fos, contentSize);
							fos.flush();
							jis.close(); jis = null;
							fos.close(); fos = null;
							filesLoaded++;
							bytesLoaded += contentSize;
							log.debug("Wrote content file [{}] + with size [{}]", contentFile, contentSize );
						} finally {
							if( jis!=null ) try { jis.close(); } catch (Exception ex) {}
							if( fos!=null ) try { fos.close(); } catch (Exception ex) {}
						}
					}  // not content
				} // end of while loop
				final long elapsed = System.currentTimeMillis()-startTime;
				StringBuilder b = new StringBuilder("\n\n\t===================================================\n\tStatic Root Directory:[").append(contentDirectory).append("]");
				b.append("\n\tTotal Files Written:").append(filesLoaded);
				b.append("\n\tTotal Bytes Written:").append(bytesLoaded);
				b.append("\n\tFile Write Failures:").append(fileFailures);
				b.append("\n\tFile Older Than Content:").append(fileOlder);
				b.append("\n\tElapsed (ms):").append(elapsed);
				b.append("\n\t===================================================\n");
				log.info(b.toString());
			} catch (Exception ex) {
				log.error("Failed to export ui content", ex);			  
			} finally {
				if( jar!=null ) try { jar.close(); } catch (Exception x) { /* No Op */}
			}
		}  else {	// end of was-not-a-jar
			log.warn("\n\tThe OpenTSDB classpath is not a jar file, so there is no content to unload.\n\tBuild the OpenTSDB jar and run 'java -jar <jar> --d <target>'.");
		}
	}
	
	  /**
	   * Writes the PID to the file at the passed location
	   * @param file The fully qualified pid file name
	   * @param ignorePidFile If true, an existing pid file will be ignored after a warning log
	   */
	  private static void writePid(String file, boolean ignorePidFile) {
		  File pidFile = new File(file);
		  if(pidFile.exists()) {
			  Long oldPid = getPid(pidFile);
			  if(oldPid==null) {
				  pidFile.delete();
			  } else {
				  log.warn("\n\t==================================\n\tThe OpenTSDB PID file [" + file + "] already exists for PID [" + oldPid + "]. \n\tOpenTSDB might already be running.\n\t==================================\n");
				  if(!ignorePidFile) {
					  log.warn("Exiting due to existing pid file. Start with option --ignore-existing-pid to overwrite"); 
					  System.exit(-1);
				  } else {
					  log.warn("Deleting existing pid file [" + file + "]");
					  pidFile.delete();
				  }
			  }
		  }
		  pidFile.deleteOnExit();
		  File pidDir = pidFile.getParentFile();
		  FileOutputStream fos = null;
		  try {
			  if(!pidDir.exists()) {
				  if(!pidDir.mkdirs()) {
					  throw new Exception("Failed to create PID directory [" + file + "]");
				  }
			  }		  
			  fos = new FileOutputStream(pidFile);		  
			  String PID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
			  fos.write(String.format("%s%s", PID, EOL).getBytes());
			  fos.flush();
			  fos.close();
			  fos = null;
			  log.info("PID [" + PID + "] written to pid file [" + file + "]");
		  } catch (Exception ex) {
			  log.error("Failed to write PID file to [" + file + "]", ex);
			  throw new IllegalArgumentException("Failed to write PID file to [" + file + "]", ex);
		  } finally {
			  if(fos!=null) try { fos.close(); } catch (Exception ex) { /* No Op */ }
		  }
	  }

	  /**
	   * Reads the pid from the specified pid file
	   * @param pidFile The pid file to read from
	   * @return The read pid or possibly null / blank if failed to read
	   */
	  private static Long getPid(File pidFile) {
	  	FileReader reader = null;
	  	BufferedReader lineReader = null;
	  	String pidLine  = null;
	  	try {
	  		reader = new FileReader(pidFile);
	  		lineReader = new BufferedReader(reader);
	  		pidLine = lineReader.readLine();
	  		if(pidLine!=null) {
	  			pidLine = pidLine.trim();			
	  		}		
	  	} catch (Exception ex) {
	  		log.error("Failed to read PID from file  [" + pidFile.getAbsolutePath() + "]", ex);
	  	} finally {
	  		if(reader!=null) try { reader.close(); } catch (Exception ex) { /* No Op */ }
	  	}
	  	try {
	  		return Long.parseLong(pidLine);
	  	} catch (Exception ex) {
	  		return null;
	  	}	  	
	  }

}
