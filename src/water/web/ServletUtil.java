package water.web;

import java.util.Properties;
import water.DKV;
import water.Key;
import water.Value;
import water.ValueArray;
import water.parser.SeparatedValueParser;
import water.web.H2OPage;
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
    SeparatedValueParser csv = new SeparatedValueParser(key, ',', maxCols);
    double sums[] = new double[maxCols];
    double mins[] = new double[maxCols];
    double maxs[] = new double[maxCols];
    for( int i = 0; i < maxCols; i++ ) {
      mins[i] = Double.MAX_VALUE;
      maxs[i] = Double.MIN_VALUE;
    }
    int rows = 0;
    int maxValidColumn = 0;
    for( double[] vals : csv ) {
      ++rows;
      for( int i = 0; i < maxCols; ++i ) {
        if( Double.isNaN(vals[i]) )
          break;
        maxValidColumn = Math.max(i, maxValidColumn);
        // Skipping any 1st record, try to count columns in the 2nd record
        sums[i] += vals[i];
        mins[i] = Math.min(mins[i], vals[i]);
        maxs[i] = Math.max(maxs[i], vals[i]);
      }
    }
    // Inject into the HTML
    if( maxValidColumn > 0 && rows > 0 ) {
      row.replace("rows",rows);
      row.replace("cols",maxValidColumn);
      for( int i=0; i<Math.min(maxValidColumn,5); i++ ) {
        String s = String.format("%4.1f / %4.1f / %4.1f",mins[i],sums[i]/rows,maxs[i]);
        row.replace("col"+i,s);
      }
    }
    row.append();
  }
}
