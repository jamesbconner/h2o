package test;

import static org.junit.Assert.*;
import hex.rf.Confusion;
import hex.rf.Model;

import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import water.*;
import water.parser.ParseDataset;
import water.util.KeyUtil;
import water.web.RFView;
import water.web.RandomForestPage;

import com.google.gson.JsonObject;

public class RandomForestTest {
  private static int _initial_keycnt = 0;

  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] { });
    _initial_keycnt = H2O.store_size();
  }

  @AfterClass public static void checkLeakedKeys() {
    int leaked_keys = H2O.store_size() - _initial_keycnt;
    assertEquals("No keys leaked", 0, leaked_keys);
  }

  // ---
  // Test parsing "iris2.csv" and running Random Forest - by driving the web interface
  @org.junit.Test public void testRF_Iris() {
    final int CLASSES=3;        // Number of output classes in iris dataset
    Key fkey = KeyUtil.load_test_file("smalldata/iris/iris2.csv");
    Key okey = Key.make("iris.hex");
    ParseDataset.parse(okey,DKV.get(fkey));
    UKV.remove(fkey);
    DKV.get(okey);
    final int NTREE=7;

    // Build a Random Forest
    try {
      // RF Page is driven by a Properties
      Properties p = new Properties();
      p.setProperty("Key",okey.toString());
      p.setProperty("ntree",Integer.toString(NTREE));
      p.setProperty("sample",Integer.toString(100));
      RandomForestPage RFP = new RandomForestPage();

      // Start RFPage, get a JSON result.
      JsonObject res = RFP.serverJson(null,p,null);
      // From the JSON, get modelKey & ntree to be built
      Key modelKey = Key.make(res.get("modelKey").getAsString());
      Model model = new Model();
      int ntree = res.get("ntree").getAsInt();
      assertEquals(ntree,NTREE);
      // Wait for the trees to be built.  This should be a blocking call someday.
      while( true ) {
        // Peel out the model.
        model = UKV.get(modelKey,model);
        if( model.size() >= ntree ) break;
        try { Thread.sleep(100); } catch( InterruptedException ie ) { }
      }

      assertEquals(CLASSES,model._classes);

      // Now build the properties for a RFView page.
      p.setProperty("dataKey",okey.toString());
      p.setProperty("modelKey",modelKey.toString());
      p.setProperty("ntree",Integer.toString(ntree));

      RFView rfv = new RFView();
      JsonObject rfv_res = rfv.serverJson(null,p,null);
      rfv.serveImpl(null,p,null);

      // Verify Goodness and Light
      Key oKey2 = Key.make(rfv_res.get("dataKey").getAsString());
      assertEquals(okey,oKey2);
      Key mkey2 = Key.make(rfv_res.get("modelKey").getAsString());
      assertEquals(modelKey,mkey2);
      Key confKey = Key.make(rfv_res.get("confusionKey").getAsString());
      // Should be a pre-built confusion
      Confusion C = UKV.get(confKey,new Confusion());

      // This should be a 7-tree confusion matrix on the iris dataset, build
      // with deterministic trees.
      // Confirm the actual results.
      long ans[][] = new long[][]{{50,0,0},{0,45,5},{0,0,50}};
      for( int i=0; i<ans.length; i++ )
        assertArrayEquals(ans[i],C._matrix[i]);

      // Cleanup
      //UKV.remove(model._treesKey);
      UKV.remove(modelKey);
      UKV.remove(confKey);

    } catch( water.web.Page.PageError pe ) {
      fail("RandomForestPage fails with "+pe);
    } finally {
      UKV.remove(okey);
    }
  }
}
