package test;

import com.google.gson.JsonObject;
import hex.rf.Model;
import hex.rf.Confusion;
import java.io.File;
import java.util.Properties;
import org.junit.*;
import static org.junit.Assert.*;
import water.*;
import water.parser.ParseDataset;
import water.util.KeyUtil;
import water.web.*;

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
  // Test parsing "iris.csv" and running Random Forest - by driving the web interface
  @org.junit.Test public void testRF_Iris() {
    final int CLASSES=3;        // Number of output classes in iris dataset
    Key fkey = KeyUtil.load_test_file("smalldata/iris/iris.csv");
    Key okey = Key.make("iris.hex");
    ParseDataset.parse(okey,DKV.get(fkey));
    UKV.remove(fkey);
    ValueArray va = (ValueArray)DKV.get(okey);
    final int NTREES=7;

    // Build a Random Forest
    try {
      // RF Page is driven by a Properties
      Properties p = new Properties();
      p.setProperty("Key",H2OPage.encode(okey));
      p.setProperty("ntrees",Integer.toString(NTREES));
      RandomForestPage RFP = new RandomForestPage();

      // Start RFPage, get a JSON result.
      JsonObject res = RFP.serverJson(null,p,null);
      // From the JSON, get treesKey & ntrees to be built
      Key treesKey = H2OPage.decode(res.get("treesKeyHref").getAsString());
      int ntrees = res.get("ntrees").getAsInt();
      assertEquals(ntrees,NTREES);
      // Wait for the trees to be built.  This should be a blocking call someday.
      while( true ) {
        Value tkbits = DKV.get(treesKey);
        if( tkbits != null ) {
          byte[] buf = tkbits.get();
          int klen = UDP.get4(buf,0);
          if( klen >= ntrees ) break;
        }
        try { Thread.sleep(100); } catch( InterruptedException ie ) { }
      }
      // Peel out the modelKey.
      Key modelKey = H2OPage.decode(res.get("modelKeyHref").getAsString());
      Value modelVal = UKV.get(modelKey);
      Model model = new Model();
      model.read(new Stream(modelVal.get()));
      assertEquals(model.size(),0); // Expect zero trees so-far.
      assertEquals(model._classes,CLASSES);

      // Now build the properties for a RFView page.
      p.setProperty("dataKey",H2OPage.encode(okey));
      p.setProperty("modelKey",H2OPage.encode(modelKey));
      p.setProperty("treesKey",H2OPage.encode(treesKey));
      p.setProperty("ntrees",Integer.toString(ntrees));

      // Spin until all trees are built
      Key confKey=null;
      while( true ) {
        RFView rfv = new RFView();
        JsonObject rfv_res = rfv.serverJson(null,p,null);

        // Verify Goodness and Light
        Key oKey2 = H2OPage.decode(rfv_res.get("dataKeyHref").getAsString());
        assertEquals(okey,oKey2);
        Key mkey2 = H2OPage.decode(rfv_res.get("modelKeyHref").getAsString());
        assertEquals(modelKey,mkey2);
        modelVal = UKV.get(modelKey);
        model = new Model();
        model.read(new Stream(modelVal.get()));
        confKey = H2OPage.decode(rfv_res.get("confusionKeyHref").getAsString());
        if( model.size() >= NTREES ) break;
        UKV.remove(confKey);    // Premature incremental Confusion; nuke it
        try { Thread.sleep(100); } catch( InterruptedException ie ) { }
      }
      // Should be a pre-built confusion
      Confusion C = new Confusion();
      C.read(new Stream(UKV.get(confKey).get()));
      
      // This should be a 7-tree confusion matrix on the iris dataset, build
      // with deterministic trees.
      // Confirm the actual results.
      long ans[][] = new long[][]{{50,0,0},{0,45,5},{0,0,50}};
      for( int i=0; i<ans.length; i++ )
        assertArrayEquals(ans[i],C._matrix[i]);

      // Cleanup
      UKV.remove(treesKey);
      UKV.remove(modelKey);
      UKV.remove(confKey);

    } catch( water.web.Page.PageError pe ) {
      fail("RandomForestPage fails with "+pe);
    } finally {
      UKV.remove(okey);
    }
  }
}
