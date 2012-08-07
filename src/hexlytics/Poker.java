package hexlytics;


import java.io.File;
import java.io.FileInputStream;

import water.csv.CSVParser.CSVParserSetup;
import water.csv.ValueCSVRecords;

public class Poker {

  Data poker;
  
  Poker() {
    poker =  Data.make("poker", new String[]{"0","1","2","3","4","5","6","7","8","9"}, "9");
  }
  public int bagSizePercent()     { return 70; } // usually 70%, but for a big data set....

  

  public Poker(File inputFile) throws Exception {
    this();
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
  }



  /**
   * for testing...
   * 
   * @param args list of filenames to be processed
   * @throws Exception
   */
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
      System.out.println(p.poker.shrinkWrap().select(0, 100));
    }
  }

}
