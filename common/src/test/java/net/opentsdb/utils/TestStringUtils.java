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
package net.opentsdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestStringUtils {

  @Test
  public void getRandomString() throws Exception {
    final Pattern pattern = Pattern.compile("[A-Za-z]{24}");
    for (int i = 0; i < 100; i++) {
      assertTrue(pattern.matcher(StringUtils.getRandomString(24)).matches());
    }
    
    try {
      StringUtils.getRandomString(0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      StringUtils.getRandomString(-42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void splitString() throws Exception {
    String[] splits = StringUtils.splitString("Space Separated Chars", ' ');
    assertEquals(3, splits.length);
    assertEquals("Space", splits[0]);
    assertEquals("Separated", splits[1]);
    assertEquals("Chars", splits[2]);
    
    splits = StringUtils.splitString("NoSpace", ' ');
    assertEquals(1, splits.length);
    assertEquals("NoSpace", splits[0]);
    
    splits = StringUtils.splitString("", ' ');
    assertEquals(1, splits.length);
    assertEquals("", splits[0]);
    
    try {
      StringUtils.splitString(null, ' ');
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
  }

  @Test
  public void splitStringWithBrackets() throws Exception {
    // repeat the case of SplitString to validate it still works
    String[] splits = StringUtils.splitStringWithBrackets("Space Separated Chars", ' ');
    assertEquals(3, splits.length);
    assertEquals("Space", splits[0]);
    assertEquals("Separated", splits[1]);
    assertEquals("Chars", splits[2]);

    splits = StringUtils.splitStringWithBrackets("NoSpace", ' ');
    assertEquals(1, splits.length);
    assertEquals("NoSpace", splits[0]);

    splits = StringUtils.splitStringWithBrackets("", ' ');
    assertEquals(1, splits.length);
    assertEquals("", splits[0]);

    try {
      StringUtils.splitStringWithBrackets(null, ' ');
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }

    // m-type query with brackets
    splits = StringUtils.splitStringWithBrackets("avg:1s-avg:metric.name{tagPre:tagPost=value,tag2=value2}", ':');
    assertEquals(3, splits.length);
    assertEquals("avg", splits[0]);
    assertEquals("1s-avg", splits[1]);
    assertEquals("metric.name{tagPre:tagPost=value,tag2=value2}", splits[2]);

    // test for nesting brackets
    splits = StringUtils.splitStringWithBrackets("a:b:c{(::)}{:}(:)[:]:d", ':');
    assertEquals(4, splits.length);
    assertEquals("a", splits[0]);
    assertEquals("b", splits[1]);
    assertEquals("c{(::)}{:}(:)[:]", splits[2]);
    assertEquals("d", splits[3]);

    // test for nesting exception
    try {
      splits = StringUtils.splitStringWithBrackets("a:b:c{(::]}{:}(:)[:]:d", ':');
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("bracket open/close mismatch", e.getMessage());
    }

    try {
      splits = StringUtils.splitStringWithBrackets("a:b:c{(::[)]}{:}(:)[:]:d", ':');
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("bracket open/close mismatch", e.getMessage());
    }

    try {
      splits = StringUtils.splitStringWithBrackets("a:b:c{(({[[[(({[::[)]}{:}(:)[:]:d", ':');
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("more than 10 nested brackets", e.getMessage());
    }

  }

  // some strings from https://www.w3.org/2001/06/utf-8-test/UTF-8-demo.html
  private static Stream<Arguments> utfStrings() {
    return Stream.of(
      Arguments.arguments("Hello world!"),
      Arguments.arguments("᾿Απ᾿ τὰ κόκκαλα βγαλμένη"),
      Arguments.arguments("๏ แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช"),
      Arguments.arguments("Οὐχὶ ταὐτὰ παρίσταταί μοι γιγνώσκειν, ὦ ἄνδρες ᾿Αθηναῖοι,"),
      Arguments.arguments("გთხოვთ ახლავე გაიაროთ რეგისტრაცია Unicode-ის"),
      Arguments.arguments("Интернета и Unicode, локализации"),
      Arguments.arguments("ሲተረጉሙ ይደረግሙ።"),
      Arguments.arguments("ᛚᚪᚾᛞᛖ ᚾᚩᚱᚦᚹᛖᚪᚱᛞᚢᛗ ᚹᛁᚦ ᚦᚪ ᚹᛖᛥᚫ"),
      Arguments.arguments("⡌⠁⠧⠑ ⠼⠁⠒  ⡍⠜⠇⠑⠹⠰⠎ ⡣⠕⠌"),
      Arguments.arguments("ABCDEFGHIJKLMNOPQRSTUVWXYZ /0123456789"),
      Arguments.arguments("abcdefghijklmnopqrstuvwxyz £©µÀÆÖÞßéöÿ"),
      Arguments.arguments("–—‘“”„†•…‰™œŠŸž€ ΑΒΓΔΩαβγδω АБВГДабвгд"),
      Arguments.arguments("∀∂∈ℝ∧∪≡∞ ↑↗↨↻⇣ ┐┼╔╘░►☺♀ ﬁ�⑀₂ἠḂӥẄɐː⍎אԱა"),
      Arguments.arguments("Καλημέρα κόσμε"),
      Arguments.arguments("コンニチハ"),
      Arguments.arguments("∮ E⋅da = Q,  n → ∞, ∑ f(i) = ∏ g(i), ∀x∈ℝ: ⌈x⌉ = −⌊−x⌋, α ∧ ¬β = ¬(¬α ∨ β),"),
      Arguments.arguments("ℕ ⊆ ℕ₀ ⊂ ℤ ⊂ ℚ ⊂ ℝ ⊂ ℂ, ⊥ < a ≠ b ≡ c ≤ d ≪ ⊤ ⇒ (A ⇔ B),"),
      Arguments.arguments("2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm"),
      Arguments.arguments("ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn"),
      Arguments.arguments("Y [ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]"),
      Arguments.arguments("((V⍳V)=⍳⍴V)/V←,V    ⌷←⍳→⍴∆∇⊃‾⍎⍕⌈"),
      Arguments.arguments("14.95 €")
    );
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("utfStrings")
  public void stringToUTF8Bytes(String string) {
    int encodedLength = StringUtils.stringToUTF8BytesLength(string);
    byte[] buf = new byte[encodedLength];
    int written = StringUtils.stringToUTF8Bytes(string, buf, 0);
    assertEquals(encodedLength, written);
    byte[] java = string.getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(buf, java);
    String backTo16 = new String(buf, StandardCharsets.UTF_8);
    assertEquals(string, backTo16);
  }

}
