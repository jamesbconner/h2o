package hexlytics.rf;

import java.io.File;
import java.io.FileInputStream;

import water.DKV;
import water.ValueArray;
import water.parser.CSVParser.CSVParserSetup;
import water.parser.ValueCSVRecords;

public class Poker {
  Data _dapt;
  
  public Poker(File inputFile) throws Exception {
    String[] names = new String[]{"0","1","2","3","4","5","6","7","8","9","10"};
    DataAdapter poker = new DataAdapter("poker", names, "10");  
    int[] r = new int[names.length];
    CSVParserSetup setup = new CSVParserSetup();
    setup._parseColumnNames = false;
    ValueCSVRecords<int[]> p1 = 
        new ValueCSVRecords<int[]>(new FileInputStream(inputFile), r, null, setup);
    double[] v = new double[names.length];
    for (int[] x : p1) {
      for(int i=0;i<names.length;i++)  v[i]=x[i];
      poker.addRow(v);
    }
    poker.freeze();
    poker.shrinkWrap();
    _dapt = Data.make(poker);
  }

  public static void main(String[] args) throws Exception {
    if(args.length==0)args = new String[] { "smalldata/poker/poker-hand-testing.data" };
    File f = new File(args[0]);
    Poker p = new Poker(f);         
    Data d = p._dapt;
    Data t = d.sampleWithReplacement(.6);
    Data v = t.complement();
    p._dapt=d=null;// GC!
    DataAdapter.FEATURES = 4;

    Director dir = new LocalBuilder(t,v,20);
  }

  // Dataset launched from web interface
  public static void web_main( ValueArray ary, int ntrees) {
    final int rowsize = ary.row_size();
    final int num_cols = ary.num_cols();
    String[] names = ary.col_names();
    DataAdapter dapt = new DataAdapter(ary._key.toString(), names, 
                                       names[num_cols-1]); // Assume class is the last column
    double[] ds = new double[num_cols];
    final long num_chks = ary.chunks();
    for( long i=0; i<num_chks; i++ ) { // By chunks
      byte[] bits = DKV.get(ary.chunk_get(i)).get();
      final int num_rows = bits.length/rowsize;
      for( int j=0; j<num_rows; j++ ) { // For all rows in this chunk
        for( int k=0; k<num_cols; k++ )
          ds[k] = ary.datad(bits,j,rowsize,k);
        dapt.addRow(ds);
      }
    }
    dapt.freeze();
    dapt.shrinkWrap();
    Data d = Data.make(dapt);
    Data t = d.sampleWithReplacement(.666);
    Data v = t.complement();
    Director dir = new LocalBuilder(t,v,ntrees);
  }
}
