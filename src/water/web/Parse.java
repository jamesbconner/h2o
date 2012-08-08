package water.web;
import java.util.Properties;
import water.*;
import water.csv.ParseDataset;

public class Parse extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    String skey1 = args.getProperty("Key");
    String skey2 = args.getProperty("Key2");

    Key key1 = Key.make(skey1);
    Key key2 = Key.make(skey2);

    Value dataset = DKV.get(key1);  // Get the source dataset
    ParseDataset.parse(key2,dataset);

    return "Parsed into "+key2;
  }
}
