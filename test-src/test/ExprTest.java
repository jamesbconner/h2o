package test;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.exec.*;
import water.parser.ParseDataset;
import water.util.KeyUtil;

public class ExprTest {
  private static int _initial_keycnt = 0;

  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] { });
    _initial_keycnt = H2O.store_size();
  }

  @AfterClass public static void checkLeakedKeys() {
    int leaked_keys = H2O.store_size() - _initial_keycnt;
    assertEquals("No keys leaked", 0, leaked_keys);
  }
  
  int i = 0;

  
  protected void testParseFail(String expr, int errorPos) {
    try {
      RLikeParser parser = new RLikeParser();
      parser.parse(expr);
      assertTrue("An exception should have been thrown.",false);
    } catch (ParserException e) {
      if (errorPos != -1)
        assertEquals(errorPos,e._pos);
    }
  }
  
  protected void testParseFail(String expr) {
    testParseFail(expr,-1);
  }
  
  protected void testExecFail(String expr, int errorPos) {
    DKV.write_barrier();
    int keys = H2O.store_size();
    try {
      System.err.println("result"+(new Integer(i).toString())+": "+expr);
      Key key = Exec.exec(expr, "result"+(new Integer(i).toString()));
      UKV.remove(key);
      assertTrue("An exception should have been thrown.",false);
    } catch (ParserException e) {
      assertTrue(false);
    } catch (EvaluationException e) {
      if (errorPos!=-1)
        assertEquals(errorPos,e._pos);
    }
    DKV.write_barrier();
    assertEquals("Keys were not properly deleted for expression "+expr,keys,H2O.store_size());
  }
  
  protected void testExecFail(String expr) {
    testExecFail(expr,-1);
  }
  
  @Test public void testParserFails() {
    testParseFail("4.5.6");
    testParseFail("4e4e4");
    testParseFail("a +* b");
    testParseFail("(a + b");
    testParseFail(" \"hello");
    testParseFail(" a $ 5");
  }
  
  @Test public void testExecFails() {
    testExecFail("a");
    testExecFail("a$hello");
    testExecFail("a[2]");
    testScalarExpression("a=5",5);
    testExecFail("a$hello");
    testExecFail("a[2]");
    UKV.remove(Key.make("a"));
  }
  
  @Test public void testDivByZero() {
    testScalarExpression("5/0", Double.POSITIVE_INFINITY);
    testScalarExpression("n = 6",6);
    testScalarExpression("g = 0",0);
    testScalarExpression("n/g",Double.POSITIVE_INFINITY);
    testScalarExpression("n/0",Double.POSITIVE_INFINITY);
    UKV.remove(Key.make("n"));
    UKV.remove(Key.make("g"));
  }
  
  protected Key executeExpression(String expr) {
    DKV.write_barrier();
    try {
      ++i;
      Key key = Exec.exec(expr, "result"+(new Integer(i).toString()));
      return key;
    } catch (PositionedException e) {
      System.out.println(e.report(expr));
      e.printStackTrace();
      assertTrue(false);
      return null;
    }
  }
  
  protected void testScalarExpression(String expr, double result) {
    Key key = executeExpression(expr);
    ValueArray va = (ValueArray) DKV.get(key);
    assertEquals(va.num_rows(), 1);
    assertEquals(va.num_cols(), 1);
    assertEquals(result,va.datad(0,0), 0.0);
    UKV.remove(key);
  }
  
  protected Key loadAndParseKey(String keyName, String path) {
    Key fkey = KeyUtil.load_test_file(path);
    Key okey = Key.make(keyName);
    ParseDataset.parse(okey,DKV.get(fkey));
    UKV.remove(fkey);
    return okey;
  }
  
  protected void testKeyValues(Key k, double n1, double n2, double n3, double nx3, double nx2, double nx1) {
    ValueArray v = (ValueArray) DKV.get(k);
    assertEquals(v.datad(0,0),n1,0.0);
    assertEquals(v.datad(1,0),n2,0.0);
    assertEquals(v.datad(2,0),n3,0.0);
    assertEquals(v.datad(v.num_rows()-3,0),nx3,0.0);
    assertEquals(v.datad(v.num_rows()-2,0),nx2,0.0);
    assertEquals(v.datad(v.num_rows()-1,0),nx1,0.0);
  }
  
  public void testVectorExpression(String expr, double n1, double n2, double n3, double nx3, double nx2, double nx1) {
    Key key = executeExpression(expr);
    testKeyValues(key,n1,n2,n3,nx3,nx2,nx1);
    UKV.remove(key);
  }
  
  public void testDataFrameStructure(Key k, int rows, int cols) {
    ValueArray v = (ValueArray) DKV.get(k);
    assertEquals(v.num_rows(), rows);
    assertEquals(v.num_cols(), cols);
  }
  
  @Test public void testNumberParsing() {
    testScalarExpression("5",5);
    testScalarExpression("5.0",5.0);
    testScalarExpression("5e4",5e4);
    testScalarExpression("5.2e3",5.2e3);
  }
  
  
  @Test public void testScalarExpressions() {
    testScalarExpression("5", 5);
    testScalarExpression("-5", -5);
    testScalarExpression("5+6", 11);
    testScalarExpression("5    + 7", 12);
    testScalarExpression("5+-5", 0); 
  }
  
  @Test public void testOperators() {
    testScalarExpression("1+2",3);
    testScalarExpression("1-2",-1);
    testScalarExpression("1*2",2);
    testScalarExpression("1/2",0.5);
    testScalarExpression("2-1",1);
    testScalarExpression("2/1",2);
  }
  
  @Test public void testOperatorPrecedence() {
    testScalarExpression("1+2*3",7);
    testScalarExpression("1*2+3",5);
    testScalarExpression("1+2*3+4",11);
    testScalarExpression("1+2*3+3*3",16);
    testScalarExpression("1-2/4",0.5);
    testScalarExpression("1+2-3",0);
    testScalarExpression("1*2/4",0.5);
  }
  
  @Test public void testParentheses() {
    testScalarExpression("(1+2)*3",9);
    testScalarExpression("(1+2)*(3+3)*3",54);
  }
  
  @Test public void testAssignments() {
    testScalarExpression("a1 = 5",5);
    testScalarExpression("b1 = 6",6);
    testScalarExpression("a1",5);
    testScalarExpression("b1",6);
    testScalarExpression("a2 <- 1",1);
    testScalarExpression("a2",1);
    testScalarExpression("1 -> b2",1);
    testScalarExpression("b2",1);
    UKV.remove(Key.make("a1"));
    UKV.remove(Key.make("b1"));
    UKV.remove(Key.make("a2"));
    UKV.remove(Key.make("b2"));
  }
  
  @Test public void testIdentOperators() {
    testScalarExpression("a3 = 8", 8);
    testScalarExpression("b3 = 2", 2);
    testScalarExpression("a3+b3",10);
    testScalarExpression("a3-b3",6);
    testScalarExpression("a3*b3",16);
    testScalarExpression("a3/b3",4);
    testScalarExpression("a3+4",12); // from right
    testScalarExpression("a3-4",4);
    testScalarExpression("a3*4",32);
    testScalarExpression("a3/4",2);
    testScalarExpression("4+a3",12); // from left
    testScalarExpression("4-a3",-4);
    testScalarExpression("4*a3",32);
    testScalarExpression("32/a3",4);
    testScalarExpression("-a3+2",-6);
    UKV.remove(Key.make("a3"));
    UKV.remove(Key.make("b3"));
  }
 
  @Test public void testQuotedIdents() {
    testScalarExpression("\"a\\\"b/c\\\\d\" = 5", 5);
    testScalarExpression("\"a\\\"b/c\\\\d\"", 5);
    UKV.remove(Key.make("a\"b/c\\d"));
  }
  
  @Test public void testComplexAssignments() {
    testScalarExpression("a4 = 5",5);
    testScalarExpression("b4 = 6",6);
    testScalarExpression("c4 = a4 + b4",11);
    testScalarExpression("c4",11);
    testScalarExpression("c4 + a4 -> c5",16);
    testScalarExpression("c5",16);
    UKV.remove(Key.make("a4"));
    UKV.remove(Key.make("b4"));
    UKV.remove(Key.make("c4"));
    UKV.remove(Key.make("c5"));
  } 

}
