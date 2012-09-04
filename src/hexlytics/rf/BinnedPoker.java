package hexlytics.rf;

import java.io.File;

import test.KVTest;
import water.*;
import water.parser.ParseDataset;

public class BinnedPoker {
  public static void main(String[] args) throws Exception {
    H2O.main(new String[] {});
    
    if(args.length==0) args = new String[] { "smalldata/poker/poker-hand-testing.data" };
    Key fileKey = KVTest.load_test_file(new File(args[0]));
    
    Key parsedKey = Key.make();
    ParseDataset.parse(parsedKey, DKV.get(fileKey));
    ValueArray va = (ValueArray) DKV.get(parsedKey);
    
    DKV.remove(fileKey); // clean up file to save memory
    RandomForest.web_main(va, 10, 40, 4, true);
  }
}
