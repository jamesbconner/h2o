package test;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Properties;
import org.junit.*;
import static org.junit.Assert.*;
import water.*;
import water.parser.ParseDataset;
import water.util.KeyUtil;
import water.web.ExecWeb;
import water.web.H2OPage;

public class ExprTest {
  private static int _initial_keycnt = 0;

  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] { });
    //long start = System.currentTimeMillis();
    //while (System.currentTimeMillis() - start < 10000) {
    //  if (H2O.CLOUD.size() > 2) break;
    //  try { Thread.sleep(100); } catch( InterruptedException ie ) {}
    //}
    //assertEquals("Cloud size of 3", 3, H2O.CLOUD.size());
    _initial_keycnt = H2O.store_size();
  }

  @AfterClass public static void checkLeakedKeys() {
    int leaked_keys = H2O.store_size() - _initial_keycnt;
    assertEquals("No keys leaked", 0, leaked_keys);
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
