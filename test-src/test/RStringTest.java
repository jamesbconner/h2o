package test;
import java.util.Arrays;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.exec.*;
import water.parser.ParseDataset;
import water.util.KeyUtil;
import water.web.RString;

public class RStringTest {

  static final String s1 = "hello %all and %$all and %go";
  
  @Test public void testReplacement() {
    RString r1 = new RString(s1);
    r1.replace("all"," +");
    assertEquals("hello  + and +%2B and ",r1.toString());
    r1.replace("go","why");
    assertEquals("hello  + and +%2B and why",r1.toString());
  }
  
}
