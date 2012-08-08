package water.csv;
import water.*;

// Helper class to parse an entire ValueArray data, and produce a structured
// ValueArray result.
//
// @author <a href="mailto:cliffc@0xdata.com"></a>

public final class ParseDataset {
  // Parse the dataset as a CSV-style thingy and produce a structured dataset result.
  public static void parse( Key result, Value dataset ) {
    if( dataset instanceof ValueArray && ((ValueArray)dataset).num_cols() > 0 )
      throw new IllegalArgumentException("This is a binary structured dataset; parse() only works on text files.");
    
    // Best-guess on count of columns.  Skip 1st line.  Count column delimiters
    // in the next line.
    Value v0 = DKV.get(dataset.chunk_get(0)); // First chunk
    byte[] b = v0.get();                      // Bytes for 1st chunk
    int i=0;
    while( b[i++] != '\n' ) ;   // Skip a line
    // Start counting columns on the 2nd line
    int cols = 0;
    int mode = 0;
    while( true ) {
      char c = (char)b[i++];
      if( c=='\n' ) {
        break;
      } if( Character.isWhitespace(c) ) {
        if( mode == 1 ) mode = 2;
      } else if( c == ',' ) {
        if( mode == 0 ) cols++;
        mode = 0;
      } else if( c == '"' ) {
        throw new Error("string skipping not implemented");
      } else {
        if( mode != 1 ) cols++;
        mode = 1;
      }
    }

    System.out.println("Found "+cols+" columns");

  }
}
