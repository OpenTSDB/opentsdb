// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestArgP {
  private static final String NAME = "--tsd.config";
  private static final String META = "description";
  private static final String HELP = "help";
  
  @Test
  public void addOption() throws Exception {
    // null name
    try {
      new ArgP(true).addOption(null, HELP);
      fail("Expted IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty name
    try {
      new ArgP(true).addOption("", HELP);
      fail("Expted IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // name doesn't start with hashes
    try {
      new ArgP(true).addOption("tsd.config", HELP);
      fail("Expted IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty meta
    try {
      new ArgP(true).addOption(NAME, "", HELP);
      fail("Expted IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // null help
    try {
      new ArgP(true).addOption(NAME, null);
      fail("Expted IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty help
    try {
      new ArgP(true).addOption(NAME, "");
      fail("Expted IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // good
    ArgP args = new ArgP(true);
    args.addOption(NAME, META, HELP);
    assertTrue(args.optionExists(NAME));
    
    // dupe
    try {
      new ArgP(true).addOption(NAME, "");
      args.addOption(NAME, META, HELP);
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void optionExists() throws Exception {
    ArgP args = new ArgP(true);
    args.addOption(NAME, META, HELP);
    
    assertTrue(args.optionExists(NAME));
    assertFalse(args.optionExists("--tsd.foo"));
  }

  @Test
  public void parse() throws Exception {
    ArgP args = new ArgP(true);
    args.addOption(NAME, META, HELP);
    args.addOption("--tsd.other", "Try me");
    
    // null, don't do this!
    try {
      args.parse(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty args
    String[] parsed = args.parse(new String[0]);
    assertEquals(0, parsed.length);
    
    // key=value
    String[] cli = new String[] { "--tsd.config=foo", "--tsd.other=bar" };
    parsed = args.parse(cli);
    assertEquals(0, parsed.length);
    assertEquals("foo", args.get(NAME));
    assertEquals("bar", args.get("--tsd.other"));
    
    // key value TODO - This fails! If we really want to we could clean it
    // up. But... blah
    cli = new String[] { "--tsd.config", "foo", "--tsd.other", "bar" };
    parsed = args.parse(cli);
    assertEquals(1, parsed.length);
    assertEquals("foo", args.get(NAME));
    assertNull(args.get("--tsd.other"));
    
    // key="value"
    cli = new String[] { "--tsd.config=foo", "--tsd.other=\"some spaces "
        + "in here with an =\"" };
    parsed = args.parse(cli);
    assertEquals(0, parsed.length);
    assertEquals("foo", args.get(NAME));
    assertEquals("\"some spaces in here with an =\"", args.get("--tsd.other"));
    
    // flag key=value TODO - This fails! If we really want to we could clean it
    cli = new String[] { "--tsd.config", "--tsd.other=bar" };
    parsed = args.parse(cli);
    assertEquals(0, parsed.length);
    assertEquals("--tsd.other=bar", args.get(NAME));
    assertNull(args.get("--tsd.other"));
    
    // unkown flag
    cli = new String[] { "--tsd.config=foo", "--something.else=bar" };
    try {
      args.parse(cli);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // stop early
    cli = new String[] { "--tsd.config=foo", "--", "--tsd.other=bar" };
    parsed = args.parse(cli);
    assertEquals(1, parsed.length);
    assertEquals("--tsd.other=bar", parsed[0]);
    assertEquals("foo", args.get(NAME));
    
    // case sensitive
    cli = new String[] { "--Tsd.Config", "foo", "--tsd.other", "bar" };
    try {
      args.parse(cli);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // key= (dangling delimiter)
    cli = new String[] { "--tsd.config=", "--tsd.other=bar" };
    parsed = args.parse(cli);
    assertEquals(0, parsed.length);
    assertEquals("", args.get(NAME));
    assertEquals("bar", args.get("--tsd.other"));
  }
  
  @Test
  public void parseNotStrict() throws Exception {
    ArgP args = new ArgP(false);
    args.addOption(NAME, META, HELP);
    args.addOption("--tsd.other", "Try me");
    
    // null, don't do this!
    try {
      args.parse(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty args
    String[] parsed = args.parse(new String[0]);
    assertEquals(0, parsed.length);
    
    // key=value
    String[] cli = new String[] { "--tsd.config=foo", "--tsd.other=bar" };
    parsed = args.parse(cli);
    assertEquals(0, parsed.length);
    assertEquals("foo", args.get(NAME));
    assertEquals("bar", args.get("--tsd.other"));
    
    // key value TODO - This fails! If we really want to we could clean it
    // up. But... blah
    cli = new String[] { "--tsd.config", "foo", "--tsd.other", "bar" };
    parsed = args.parse(cli);
    assertEquals(1, parsed.length);
    assertEquals("foo", args.get(NAME));
    assertNull(args.get("--tsd.other"));
    
    // key="value"
    cli = new String[] { "--tsd.config=foo", "--tsd.other=\"some spaces "
        + "in here with an =\"" };
    parsed = args.parse(cli);
    assertEquals(0, parsed.length);
    assertEquals("foo", args.get(NAME));
    assertEquals("\"some spaces in here with an =\"", args.get("--tsd.other"));
    
    // flag key=value TODO - This fails! If we really want to we could clean it
    cli = new String[] { "--tsd.config", "--tsd.other=bar" };
    parsed = args.parse(cli);
    assertEquals(0, parsed.length);
    assertEquals("--tsd.other=bar", args.get(NAME));
    assertNull(args.get("--tsd.other"));
    
    // unkown flag
    cli = new String[] { "--tsd.config=foo", "--something.else=bar" };
    parsed = args.parse(cli);
    assertEquals(1, parsed.length);
    
    // stop early
    cli = new String[] { "--tsd.config=foo", "--", "--tsd.other=bar" };
    parsed = args.parse(cli);
    assertEquals(1, parsed.length);
    assertEquals("--tsd.other=bar", parsed[0]);
    assertEquals("foo", args.get(NAME));
    
    // case sensitive
    cli = new String[] { "--Tsd.Config", "foo", "--tsd.other", "bar" };
    parsed = args.parse(cli);
    
    // key= (dangling delimiter)
    cli = new String[] { "--tsd.config=", "--tsd.other=bar" };
    parsed = args.parse(cli);
    assertEquals(0, parsed.length);
    assertEquals("", args.get(NAME));
    assertEquals("bar", args.get("--tsd.other"));
  }
  
  @Test
  public void getAndHas() throws Exception {
    ArgP args = new ArgP(true);
    args.addOption(NAME, META, HELP);
    args.addOption("--tsd.other", "Try me");
    
    // not parsed yet
    try {
      args.get(NAME);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    try {
      args.has(NAME);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    
    String[] cli = new String[] { "--tsd.config=foo", "--tsd.other" };
    args.parse(cli);
    
    assertEquals("foo", args.get(NAME));
    assertNull(args.get("--tsd.other"));
    
    try {
      args.get("--no.such.name");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // default
    assertEquals("42", args.get("--tsd.other", "42"));
    
    assertTrue(args.has(NAME));
    assertTrue(args.has("--tsd.other"));
    try {
      args.has("--no.such.name");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // doesn't have
    args.addOption("--tsd.nope", "boo!");
    assertNull(args.get("--tsd.nope"));
    assertFalse(args.has("--tsd.nope"));
  }
  
}
