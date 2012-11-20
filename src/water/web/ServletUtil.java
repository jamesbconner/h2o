package water.web;

import java.util.Properties;

import water.DKV;
import water.Key;
import water.Value;
import water.ValueArray;
import water.parser.CsvParser;
import water.web.Page.PageError;

/**
 * Utility holding common code for checking errors on servlet request pages
 */
public class ServletUtil {

  // Returns a structured ValueArray or an error String
  public static ValueArray check_array(Properties args, String s) throws PageError {
    Key key = check_key(args,s);
    // Distributed get
    Value val = DKV.get(key);
    if( val == null ) throw new PageError("Key not found: "+ key);
    if( !(val instanceof ValueArray) || ((ValueArray)val).num_cols() == 0 )
      throw new PageError("Key not a structured (parsed) array");
    return (ValueArray) val;
  }

  // Returns a Key or an error String
  public static Key check_key(Properties args, String s) throws PageError {
    String skey = args.getProperty(s);
    if( skey == null ) throw new PageError("Missing argument key: "+ s);
    try {
      return Key.make(skey);
    } catch( IllegalArgumentException e ) {
      throw new PageError("Not a valid key: "+ skey);
    }
  }

  // Task to pass the static task execution function
  public static abstract class RunnableTask {
    public abstract String run(ValueArray ary, int colA, int colB);
  }

  public static void createBestEffortSummary(Key key, RString row) {
    final int maxCols = 100;
    // Guess any separator
    Key key0 = DKV.get(key).chunk_get(0); // Key for 1st chunk
    byte[] bs = DKV.get(key0).get(); // First chunk
    int[] rows_cols = CsvParser.inspect(bs);
    // Inject into the HTML
    if (rows_cols != null) {
      row.replace("rows",rows_cols[0]);
      row.replace("cols",rows_cols[1]);
    }
    row.append();
  }
}
