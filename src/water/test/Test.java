package water.test;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runner.notification.*;
import static org.junit.Assert.*;
import water.*;

public class Test {

  // A no-arg constructor for JUnit alone
  public Test() { }

  // Run some basic tests
  @org.junit.Test public void test0() {
    Key k1 = Key.make("key1");
    Value v0 = DKV.get(k1);
    assertNull(v0);
    Value v1 = new Value(k1,"bits for Value1");
    DKV.put(k1,v1);
    assertEquals(v1._key,k1);
    Value v2 = DKV.get(k1);
    assertEquals(v1,v2);
    DKV.remove(k1);
    Value v3 = DKV.get(k1);
    assertNull(v3);
  }
}
