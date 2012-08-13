package water.web;
import java.util.Properties;
import water.*;
import water.csv.ParseDataset;

public class LinearRegression extends H2OPage {
  @Override protected String serve_impl(Properties args) {
    String skey = args.getProperty("Key");

    // Parse the Key & validate it
    Key key;
    try { 
      key = Key.make(skey);      // Get a Key from a raw byte array, if any
    } catch( IllegalArgumentException e ) {
      return wrap(error("Not a valid key: "+ skey));
    }
    if (!key.user_allowed())
      return wrap(error("Not a user key: "+ skey));
    // Distributed get
    Value val = DKV.get(key);
    if( val == null )
      return wrap(error("Key not found: "+ skey));
    if( !(val instanceof ValueArray) ||
        ((ValueArray)val).num_cols() == 0 )
      return wrap(error("Key not a structured (parsed) array"));
    ValueArray ary = (ValueArray)val;

    int colA = getAsNumber(args,"colA",0);
    int colB = getAsNumber(args,"colB",1);

    if( !(0 <= colA && colA < ary.num_cols()) )
      return wrap(error("Column "+colA+" must be between 0 and "+(ary.num_cols()-1)));
    if( !(0 <= colB && colB < ary.num_cols()) )
      return wrap(error("Column "+colB+" must be between 0 and "+(ary.num_cols()-1)));

    return hexlytics.LinearRegression.run(ary,colA,colB);
  }
}
