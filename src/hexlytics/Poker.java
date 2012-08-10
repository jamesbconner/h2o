package hexlytics;


import hexlytics.data.Data;
import hexlytics.data.DataAdapter;

import java.io.File;
import java.io.FileInputStream;

import water.csv.CSVParser.CSVParserSetup;
import water.csv.ValueCSVRecords;

public class Poker {
  Data poker_;
  
  public Poker(File inputFile) throws Exception {
    DataAdapter poker = new DataAdapter("poker", new String[]{"0","1","2","3","4","5","6","7","8","9"}, "9");  
    int[] r = new int[11];
     CSVParserSetup setup = new CSVParserSetup();
    setup._parseColumnNames = false;
    ValueCSVRecords<int[]> p1 = 
        new ValueCSVRecords<int[]>(new FileInputStream(inputFile), r, null, setup);
    double[] v = new double[10];
    for (int[] x : p1) {
      for(int i=0;i<10;i++)  v[i]=x[i];
      poker.addRow(v);
    }
    poker.freeze();
    poker_ = Data.make(poker.shrinkWrap());
  }

  public static void main(String[] args) throws Exception {
    if(args.length==0)args = new String[] { "smalldata/poker/poker-hand-testing.data" };
    for( String path : args ){
      System.out.print("parsing " + path + "...");
      File f = new File(path);
      if( !f.exists() ){
        System.out.println("file not found!");
        continue;
      }
      Poker p = new Poker(f);         
      Data d = p.poker_;
      System.out.println(d);
      System.out.println("Computing trees...");
      Data train = d.sampleWithReplacement(.6);
      Data valid = train.complement();
      int[][] score = new int[valid.rows()][valid.classes()];
      for(int i=0;i<1000;i++) {
        RF rf = new RF(train);
        rf.compute();
        rf.classify(valid, score);
        System.out.println("error = "+RF.score(valid, score));
      } 
    }
  }
}
