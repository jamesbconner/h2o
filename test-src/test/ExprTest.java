package test;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Properties;
import org.junit.*;
import static org.junit.Assert.*;
import water.*;
import water.exec.Exec;
import water.exec.PositionedException;
import water.parser.ParseDataset;
import water.util.KeyUtil;
import water.web.ExecWeb;
import water.web.H2OPage;

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

  protected void testScalarExpression(String expr, double result) {
    System.out.print("  "+expr);
    Key key = null;
    try {
      key = Exec.exec(expr);
      ValueArray va = (ValueArray) DKV.get(key);
      assertEquals(va.num_rows(), 1);
      assertEquals(va.num_cols(), 1);
      assertTrue(Math.abs(va.datad(0,0)-result) < 0.001);
      System.out.println(" OK");
    } catch (PositionedException e) {
      assertTrue(false);
      System.out.println(" FAIL");
    }
    if (key != null)
      UKV.remove(key);
  }
  
  @Test public void testScalarExpressions() {
    System.out.println("testScalarExpressions");
    testScalarExpression("5", 5);
    testScalarExpression("-5", -5);
    testScalarExpression("5+6", 11);
    testScalarExpression("5    + 7", 12);
    testScalarExpression("5+-5", 0); 
  }
  
  @Test public void testOperators() {
    System.out.println("testOperators");
    testScalarExpression("1+2",3);
    testScalarExpression("1-2",-1);
    testScalarExpression("1*2",2);
    testScalarExpression("1/2",0.5);
    testScalarExpression("2-1",1);
    testScalarExpression("2/1",2);
  }
  
  @Test public void testOperatorPrecedence() {
    System.out.println("testOperatorPrecedence");
    testScalarExpression("1+2*3",7);
    testScalarExpression("1*2+3",5);
    testScalarExpression("1+2*3+4",11);
    testScalarExpression("1+2*3+3*3",16);
    testScalarExpression("1-2/4",0.5);
    testScalarExpression("1+2-3",0);
    testScalarExpression("1*2/4",0.5);
  }
  
  @Test public void testParentheses() {
    System.out.println("testParentheses");
    testScalarExpression("(1+2)*3",9);
    testScalarExpression("(1+2)*(3+3)*3",54);
  }
  
  
  
  // ---
  // Test some basic expressions on "cars.csv"
  @Test public void testBasicCrud() {
    Key fkey = KeyUtil.load_test_file("smalldata/cars.csv");
    Key okey = Key.make("cars.hex");
    ParseDataset.parse(okey,DKV.get(fkey));
    UKV.remove(fkey);
    ValueArray va = (ValueArray)DKV.get(okey);
    ExecWeb eweb = new ExecWeb();

    Properties p = new Properties();
    p.setProperty("Expr","cars.hex[1]+cars.hex$cylinders");
    try { 
      JsonObject res = eweb.serverJson(null,p,null);
      Key key = H2OPage.decode(res.get("ResultKeyHref").getAsString());
      ValueArray ary = (ValueArray)DKV.get(key);

      assertEquals(21.0,ary.datad(  0,0),0.0);
      assertEquals(23.0,ary.datad(  1,0),0.0);
      assertEquals(25.0,ary.datad(  2,0),0.0);
      assertEquals(24.0,ary.datad(403,0),0.0);
      assertEquals(23.0,ary.datad(404,0),0.0);
      assertEquals(36.7,ary.datad(405,0),0.0);
      
      UKV.remove(okey);
      UKV.remove(key);
    } catch( H2OPage.PageError pe ) {
      pe.printStackTrace();
    } 
  }
}
