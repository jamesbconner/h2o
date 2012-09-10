package water.web;
import java.util.Properties;
import water.*;

public class NodeShuffle extends H2OPage {
  @Override protected String serveImpl(Server server, Properties args) {
    // Check the input for being a valid ValueArray
    Object res1 = ServletUtil.check_key(args,"Key");
    if( res1 instanceof String ) return (String)res1;
    Key key1 = (Key)res1;
    Value val1 = DKV.get(key1);
    if( val1 == null )
      return wrap(error("Key not found: "+ key1));
    if( !(val1 instanceof ValueArray) )
      return wrap(error("Key not a parsed dataset: "+ key1));
    ValueArray vary = (ValueArray)val1;
    if( !(val1 instanceof ValueArray) )
      return wrap(error("Key not a parsed dataset: "+ key1));
    if( vary.num_cols() == 0 )
      return wrap(error("Key not a parsed dataset: "+ key1));

    // Check that the result is a valid missing key
    Object res2 = ServletUtil.check_key(args,"Key2");
    if( res2 instanceof String ) return (String)res2;
    Key key2 = (Key)res2;
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
