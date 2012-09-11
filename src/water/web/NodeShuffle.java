package water.web;
import java.util.Properties;

import water.*;

public class NodeShuffle extends H2OPage {
  @Override protected String serveImpl(Server server, Properties args) throws PageError {
    ValueArray vary = ServletUtil.check_array(args, "Key");
    Key key2 = ServletUtil.check_key(args,"Key2");

    Value val2 = DKV.get(key2);
    if( val2 != null )
      return wrap(error("Key would be overwritten: "+ key2));

    // Get the user's shuffle function
    String sfun  = args.getProperty("ShuffleFunc");
    if( sfun == null ) sfun = "Random";

    // Shuffle!
    String res = hexlytics.NodeShuffle.run(key2,vary,sfun);
    return res;
  }
}
