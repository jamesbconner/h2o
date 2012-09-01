package hexlytics.rf;

import java.io.File;
import java.io.FileInputStream;

import water.parser.CSVParser.CSVParserSetup;
import water.parser.ValueCSVRecords;

public class Poker {
  DataAdapter _dapt;
  
  public Poker(File inputFile) throws Exception {
    String[] names = new String[]{"0","1","2","3","4","5","6","7","8","9","10"};
    _dapt = new DataAdapter("poker", names, "10");  
    int[] r = new int[names.length];
    CSVParserSetup setup = new CSVParserSetup();
    setup._parseColumnNames = false;
    ValueCSVRecords<int[]> p1 = null;
    p1=  new ValueCSVRecords<int[]>(new FileInputStream(inputFile), r, null, setup);
    double[] v = new double[names.length];
    for (int[] x : p1) {
      for(int i=0;i<names.length;i++)  v[i]=x[i];
      _dapt.addRow(v);
    }
    _dapt.shrinkWrap();
  }

  public static void main(String[] args) throws Exception {
    if(args.length==0) args = new String[] { "smalldata/poker/poker-hand-testing.data" };
    File f = new File(args[0]);
    Poker p = new Poker(f);
    RandomForest.build(p._dapt, 
        .6, //sampling
        -1,  //# features
        100, //# trees
        40, //depth
        -1, //min error rate
        4); // threads
  }


}
