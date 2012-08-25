package water.web;
import java.util.Properties;
import water.*;
import water.csv.ParseDataset;

public class Parse extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    String skey1 = args.getProperty("Key");
    String skey2 = args.getProperty("Key2");
    if( skey1 == null || skey2 == null )
      return wrap(error("Missing Key or Key2"));

    Key key1 = Key.make(skey1);
    Key key2 = Key.make(skey2);

    String s = "<a href='/Inspect?Key="+urlEncode(key2.toString())+"'>"+key2+"</a>";
    if( DKV.get(key2) == null ) { // Key not parsed?  Parse it
      long start = System.currentTimeMillis();
      Value dataset = DKV.get(key1);  // Get the source dataset root key
      if( dataset == null )
        return wrap(error(key1.toString()+" not found"));

      ParseDataset.parse(key2,dataset);
      long now = System.currentTimeMillis();
      s = "Parsed into "+s+" in "+(now-start)+" msec";
    } else {
      s = "Already parsed into "+s;
    }
    return s;
  }
}
