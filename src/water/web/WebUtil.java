package water.web;

import water.Key;
import water.parser.SeparatedValueParser;

public class WebUtil {
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
