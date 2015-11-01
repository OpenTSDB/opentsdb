package net.opentsdb.query.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.NoSuchElementException;

import org.junit.Test;

public class TestExpressionReader {
  final static String EXP = "test(sys.cpu.user)";
  
  @Test
  public void ctor() throws Exception {
    final ExpressionReader reader = new ExpressionReader(EXP.toCharArray());
    assertEquals(EXP, reader.toString());
    assertEquals(0, reader.getMark());
    assertEquals('t', reader.peek());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNull() throws Exception {
    new ExpressionReader(null);
  }
  
  @Test
  public void ctorEmptyString() throws Exception {
    final ExpressionReader reader = new ExpressionReader(new char[] { });
    assertEquals(0, reader.getMark());
    assertTrue(reader.isEOF());
    try {
      reader.peek();
      fail("Expected a NoSuchElementException");
    } catch (NoSuchElementException e) { }
    try {
      reader.next();
      fail("Expected a NoSuchElementException");
    } catch (NoSuchElementException e) { }
    try {
      reader.isNextChar('o');
      fail("Expected a NoSuchElementException");
    } catch (NoSuchElementException e) { }
    try {
      reader.readFuncName();
      fail("Expected a NoSuchElementException");
    } catch (NoSuchElementException e) { }
    reader.readNextParameter();
    // always false
    assertFalse(reader.isNextSeq("laska"));
    // no-op
    reader.skipWhitespaces();
    // doesn't hurt anything
    reader.skip(52);
  }

  @Test
  public void peek() throws Exception {
    final ExpressionReader reader = new ExpressionReader(EXP.toCharArray());
    assertEquals(0, reader.getMark());
    assertEquals('t', reader.peek());
    reader.next();
    assertEquals(1, reader.getMark());
    assertEquals('e', reader.peek());
    assertEquals(1, reader.getMark());
  }
  
  @Test
  public void nextTillEOF() throws Exception {
    final char[] chars = EXP.toCharArray();
    final ExpressionReader reader = new ExpressionReader(EXP.toCharArray());
    for (final char c : chars) {
      assertEquals(c, reader.next());
    }
    assertTrue(reader.isEOF());
  }
  
  @Test
  public void skip() throws Exception {
    final ExpressionReader reader = new ExpressionReader(EXP.toCharArray());
    reader.skip(4);
    assertEquals(4, reader.getMark());
    assertEquals('(', reader.peek());
  }
  
  @Test
  public void skipOutOfBounds() throws Exception {
    final ExpressionReader reader = new ExpressionReader(EXP.toCharArray());
    reader.skip(EXP.length());
    assertEquals(EXP.length(), reader.getMark());
    assertTrue(reader.isEOF());
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void skipBackwards() throws Exception {
    final ExpressionReader reader = new ExpressionReader(EXP.toCharArray());
    reader.skip(-1);
  }
  
  @Test
  public void isNextChar() throws Exception {
    final ExpressionReader reader = new ExpressionReader(EXP.toCharArray());
    assertTrue(reader.isNextChar('t'));
    reader.skip(4);
    assertTrue(reader.isNextChar('('));
    assertFalse(reader.isNextChar('t'));
  }
  
  @Test
  public void isNextSeq() throws Exception {
    final ExpressionReader reader = new ExpressionReader(EXP.toCharArray());
    assertTrue(reader.isNextSeq("test("));
    assertFalse(reader.isNextSeq("est("));
    assertTrue(reader.isNextSeq(EXP));
    assertFalse(reader.isNextSeq(EXP + "morestuff"));
  }
  
  @Test
  public void readFuncName() {
    ExpressionReader reader = new ExpressionReader(EXP.toCharArray());
    assertEquals("test", reader.readFuncName());
    assertEquals("", reader.readFuncName());
    assertEquals("", reader.readFuncName());
    assertFalse(reader.isEOF());
    
    // space between method and parens
    reader = new ExpressionReader("test (foo)".toCharArray());
    assertEquals("test", reader.readFuncName());
    assertEquals("", reader.readFuncName());
    
    // consume initial whitespace
    reader = new ExpressionReader("  test(foo)".toCharArray());
    assertEquals("test", reader.readFuncName());
    assertEquals("", reader.readFuncName());
    
    // whitespace everywhere!!
    reader = new ExpressionReader("  test(foo)  ".toCharArray());
    assertEquals("test", reader.readFuncName());
    assertEquals("", reader.readFuncName());
    
    // nesting fails unless we consume the parens
    reader = new ExpressionReader("test(foo(bar()))".toCharArray());
    assertEquals("test", reader.readFuncName());
    assertEquals("", reader.readFuncName());
    assertEquals("", reader.readFuncName());
    
    reader = new ExpressionReader("test(foo(bar()))".toCharArray());
    assertEquals("test", reader.readFuncName());
    reader.next();
    assertEquals("foo", reader.readFuncName());
    reader.next();
    assertEquals("bar", reader.readFuncName());
    
    // parens with space
    reader = new ExpressionReader("test ( foo ( bar()))".toCharArray());
    assertEquals("test", reader.readFuncName());
    reader.next();
    assertEquals("foo", reader.readFuncName());
    reader.next();
    assertEquals("bar", reader.readFuncName());
    
    // TODO - Watch out for the following gotchas
    reader = new ExpressionReader("test ( foo  bar()))".toCharArray());
    assertEquals("test", reader.readFuncName());
    reader.next();
    assertEquals("foo", reader.readFuncName());
    reader.next();
    assertEquals("ar", reader.readFuncName());
    
    reader = new ExpressionReader("test ".toCharArray());
    assertEquals("test", reader.readFuncName());
  }

  // TODO - more UTs around this guy
  @Test
  public void readNextParameter() {
    // will read the whole thing, so watch out!
    ExpressionReader reader = new ExpressionReader(EXP.toCharArray());
    assertEquals(EXP, reader.readNextParameter());
    
    // TODO - ok?
    reader = new ExpressionReader(EXP.toCharArray());
    reader.readFuncName();
    assertEquals("(sys.cpu.user)", reader.readNextParameter());
    
    // TODO - ok?
    reader = new ExpressionReader("test(foo,1,2)".toCharArray());
    reader.readFuncName();
    reader.next();
    assertEquals("foo,1,2", reader.readNextParameter());
  }
}
