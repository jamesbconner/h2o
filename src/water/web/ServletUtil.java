package water.web;

import java.util.Properties;

import water.DKV;
import water.Key;
import water.Value;
import water.ValueArray;
import water.parser.SeparatedValueParser;
import water.parser.SeparatedValueParser.Row;
import water.web.Page.PageError;

/**
 * Utility holding common code for checking errors on servlet request pages
 */
public class ServletUtil {

  // Pull out two parameters and check for errors on the key
  public static String serveTwoParams(Properties args, RunnableTask task) throws PageError {
    ValueArray ary = check_array(args,"Key");

    int colA = H2OPage.getAsNumber(args,"colA",0);
    int colB = H2OPage.getAsNumber(args,"colB",1);

    if( !(0 <= colA && colA < ary.num_cols()) )
      return H2OPage.wrap(H2OPage.error("Column "+colA+" must be between 0 and "+(ary.num_cols()-1)));
    if( !(0 <= colB && colB < ary.num_cols()) )
      return H2OPage.wrap(H2OPage.error("Column "+colB+" must be between 0 and "+(ary.num_cols()-1)));

    return task.run(ary,colA,colB);
  }

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
      return H2OPage.decode(skey);
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
    char sep = ' ';
    for( byte b : bs ) {
      if( b==',' || b=='\t' ) { sep=(char)b; break; }
      else if( b=='\r' || b=='\n' ) { break; }
    }
    SeparatedValueParser csv = new SeparatedValueParser(key, sep, maxCols);
    double sums[] = new double[maxCols];
    double mins[] = new double[maxCols];
    double maxs[] = new double[maxCols];
    int    rows[] = new int   [maxCols];
    for( int i = 0; i < maxCols; i++ ) {
      mins[i] = Double.MAX_VALUE;
      maxs[i] = Double.MIN_VALUE;
    }
    int maxrows = 0;
    int maxValidColumn = 0;
    for( Row r : csv ) {
      for( int i = 0; i < maxCols; ++i ) {
        double d = r._fieldVals[i];
        if( !Double.isNaN(d) ) {
          rows[i]++;
          sums[i] += d;
          mins[i] = Math.min(mins[i], d);
          maxs[i] = Math.max(maxs[i], d);
          maxrows = Math.max(rows[i],maxrows);
          maxValidColumn = Math.max(i, maxValidColumn);
        }
      }
    }
    // Inject into the HTML
    if( maxValidColumn > 0 && maxrows > 0 ) {
      row.replace("rows",maxrows);
      row.replace("cols",maxValidColumn);
      for( int i=0; i<Math.min(maxValidColumn,5); i++ ) {
        String s = "";
        if( mins[i] < Double.MAX_VALUE && maxs[i] > Double.MIN_VALUE && rows[i] > 0 )
          s = String.format("%4.1f / %4.1f / %4.1f",mins[i],sums[i]/rows[i],maxs[i]);
        row.replace("col"+i,s);
      }
    }
    row.append();
  }
}
