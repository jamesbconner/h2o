package hexlytics.tests;


import hexlytics.RFBuilder.Director;
import hexlytics.data.Data;
import hexlytics.data.DataAdapter;

import java.io.File;
import java.io.FileInputStream;

import water.csv.CSVParser.CSVParserSetup;
import water.csv.ValueCSVRecords;

public class Poker {
  Data poker_;
  
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
    poker_ = Data.make(poker.shrinkWrap());
  }

  public static void main(String[] args) throws Exception {
    if(args.length==0)args = new String[] { "smalldata/poker/poker-hand-testing.data" };
    File f = new File(args[0]);
    Poker p = new Poker(f);         
    Data d = p.poker_;
    Data t = d.sampleWithReplacement(.6);
    Data v = t.complement();
    Director dir = Director.createLocal(t,v,10);
  }
}
