package water.util;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

public class Check {
  private static final Pattern JSON_PATTERN = Pattern.compile("[a-z]+(_?[a-z0-9])*");
  private static final List<String> RESERVED_WORDS = Lists.newArrayList(
    // python reserved words
    "and", "assert", "break", "class", "continue", "def", "del", "elif",
    "else", "except", "exec", "finally", "for", "from", "global", "if",
    "import", "in", "is", "lambda", "not", "or", "pass", "print", "raise",
    "return", "try", "while",
    // java reserved words
    "public", "private", "protected", "static", "true", "false", "final",
    "volatile", "transient", "package", "catch"
  );

  public static boolean paramName(String s) {
    Matcher m = JSON_PATTERN.matcher(s);
    assert m.matches() : "Parameter " + s + " does not match convention: " + JSON_PATTERN;
    assert !RESERVED_WORDS.contains(s) : "Parameter " + s + " is a reserved word";
    return true;
  }

}
