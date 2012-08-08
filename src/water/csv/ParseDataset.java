package water.csv;
import java.io.*;
import water.*;

// Helper class to parse an entire ValueArray data, and produce a structured
// ValueArray result.
//
// @author <a href="mailto:cliffc@0xdata.com"></a>

public final class ParseDataset {

  // Parse the dataset as a CSV-style thingy and produce a structured dataset
  // result.  This does a distributed parallel parse.
  public static void parse( Key result, Value dataset ) {
    if( dataset instanceof ValueArray && ((ValueArray)dataset).num_cols() > 0 )
      throw new IllegalArgumentException("This is a binary structured dataset; parse() only works on text files.");

    // Guess on the number of columns, build a column array.
    int num_cols = guess_num_cols(dataset);
    //System.out.println("Found "+num_cols1+" columns");
    //int num_cols = CSVParserKV.getNColumns(dataset._key);

    DParse dp = new DParse();
    dp._num_cols = num_cols;

    long start = System.currentTimeMillis();

    dp.rexec(dataset._key);

    long now = System.currentTimeMillis();
    System.out.println("Found "+num_cols+" columns and "+dp._num_rows+" rows, in "+(now-start)+" msec for "+dataset.length()+" bytes" );
    for( int i=0; i<num_cols; i++ ) {
      System.out.println("col "+i+" min="+dp._cols[i]._min+" max="+dp._cols[i]._max);
    }
  }

  // Distributed parsing
  public static class DParse extends DRemoteTask {
    int _num_cols;              // Input
    int _num_rows;              // Output
    ValueArray.Column _cols[];  // Column summary data

    public int wire_len() {
      return 4+4+(_cols==null?0:(_num_cols*ValueArray.Column.wire_len()));
    }

    public int write( byte[] buf, int off ) { 
      UDP.set4(buf,(off+=4)-4,_num_cols);
      UDP.set4(buf,(off+=4)-4,_num_rows);
      if( _cols == null ) return off; // No columns?
      for( ValueArray.Column col : _cols )
        off = col.write(buf,off); // Yes columns; write them all
      return off;
    }
    public void write( DataOutputStream dos ) { 
      throw new Error("unimplemented"); 
    }
    public void read( byte[] buf, int off ) { 
      _num_cols = UDP.get4(buf,off);  off += 4; 
      _num_rows = UDP.get4(buf,off);  off += 4; 
      if( _num_rows == 0 ) return; // No columns
      assert _cols == null;
      _cols = new ValueArray.Column[_num_cols];
      final int l = ValueArray.Column.wire_len();
      for( int i=0; i<_num_cols; i++ )
        _cols[i] = ValueArray.Column.read(buf,(off+=l)-l);
    }
    public void read( DataInputStream dis ) { 
      new Error("unimplemented"); 
    }

    // Parse just this chunk: gather min & max
    public void map( Key key ) {
      assert _cols == null;
      // A place to hold the column summaries
      _cols = new ValueArray.Column[_num_cols];
      for( int i=0; i<_num_cols; i++ )
        _cols[i] = new ValueArray.Column();
      // A place to hold each column datum
      double[] data = new double[_num_cols];
      // The parser
      CSVParserKV<double[]> csv = new CSVParserKV<double[]>(key,1,data,null);
      int num_rows = 0;
      for( double[] ds : csv ) {
        num_rows++;
        for( int i=0; i<_num_cols; i++ ) {
          if( ds[i] < _cols[i]._min ) _cols[i]._min = ds[i];
          if( ds[i] > _cols[i]._max ) _cols[i]._max = ds[i];
        }
      }
      _num_rows = num_rows;
    }
    // Combine results
    public void reduce( RemoteTask rt ) {
      DParse dp = (DParse)rt;
      _num_rows += dp._num_rows;
      for( int i=0; i<_num_cols; i++ ) {
        if( dp._cols[i]._min < _cols[i]._min ) _cols[i]._min = dp._cols[i]._min;
        if( dp._cols[i]._max > _cols[i]._max ) _cols[i]._max = dp._cols[i]._max;
      }
    }
  }


  // Alternative column guesser
  private static int guess_num_cols( Value dataset ) {
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
    return cols;
  }
}
