package net.opentsdb.application;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import joptsimple.OptionParser;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class CommandLineOptionsTest {
  private CommandLineOptions cmdOptions;

  @Before
  public void setUp() throws Exception {
    final OptionParser optionParser = new OptionParser();
    cmdOptions = new CommandLineOptions(optionParser);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructorThrowsOnNullParser() throws Exception {
    new CommandLineOptions(null);
  }

  @Test
  public void testParsesShortHelp() throws Exception {
    cmdOptions.parseOptions(new String[] {"-h"});
    assertTrue(cmdOptions.shouldPrintHelp());
  }

  @Test
  public void testParsesLongHelp() throws Exception {
    cmdOptions.parseOptions(new String[] {"--help"});
    assertTrue(cmdOptions.shouldPrintHelp());
  }

  @Test(expected = IllegalStateException.class)
  public void testShouldPrintHelpThrowsOnNoParse() throws Exception {
    cmdOptions.shouldPrintHelp();
  }

  @Test(expected = IllegalStateException.class)
  public void testConfigFileThrowsOnNoParse() throws Exception {
    cmdOptions.configFile();
  }

  @Test
  public void testConfigFileDefault() throws Exception {
    cmdOptions.parseOptions(new String[] {});
    assertEquals(new File("config/opentsdb"), cmdOptions.configFile());
  }

  @Test
  public void testConfigFileParsed() throws Exception {
    cmdOptions.parseOptions(new String[] {"-c", "manualConfig.conf"});
    assertEquals(new File("manualConfig.conf"), cmdOptions.configFile());
  }
}
