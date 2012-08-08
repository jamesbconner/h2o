package test.analytics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import water.Value;
import water.csv.CSVParser.CSVParseException;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.ValueCSVRecords;
import analytics.DataAdapter;
import analytics.NumericSplitterStatistic;
import analytics.RF;
import analytics.Statistic;

/**
 * Simple adaptor for Poker dataset.
 * Can be invoked with Value (arraylet root) and arraylet indexes or with File (for convenience when testing)
 * If creating with arraylet range, note that the last index should point to two places after the last processed chunk
 * @author tomas
 */
public class PokerAdapter extends DataAdapter {

  int[][] data;

  public int numRows()            { return data.length; }
  public int numColumns()         { return data[0].length - 1;  }
  public boolean isInt(int index) { return true;  }
  public int toInt(int index)     { return data[cur][index];  }
  public double toDouble(int index){return data[cur][index];  }
  public int numClasses()         { return 2;//10; 
  }
  public int dataClass()          { return data[cur][10]==2?1:0; 
      //data[cur][10];  }
  }
  public int bagSizePercent()     { return 70; } // usually 70%, but for a big data set....
  public int numFeatures()        { return 6; } // this should be roughly 2/3 of numCol
  static int TREES = 4 * 10;
  
  
  public String summary() {
    int[] dist = new int[numClasses()];
    for(int i=0;i<numRows();i++) dist[data[i][10]==2?1:0]++;
    String res = "rows=" + numRows() +", columns="+numColumns()+"\n class distribution:";
    for(int i : dist) res+=" "+i;
    return res;
  }
  protected PokerAdapter(PokerAdapter from) {
    super();
    data = from.data;
    cur = from.cur;
  }
  
  @Override public PokerAdapter view() {
    return new PokerAdapter(this);
  }
  
  public PokerAdapter(Value v) throws NoSuchFieldException, SecurityException,
  IllegalArgumentException, IllegalAccessException, CSVParseException, IOException {
    this(v,0,(int)(v.chunks()+1));
  }
  
  public PokerAdapter(Value v, int chunkFrom, int chunkTo) throws NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, CSVParseException,  IOException {
    int[] r = new int[11];
    ArrayList<int[]> parsedRecords = new ArrayList<int[]>();
    CSVParserSetup setup = new CSVParserSetup();
    setup._parseColumnNames = false;    
    if (v != null) {
      ValueCSVRecords<int[]> p1 = new ValueCSVRecords<int[]>(v.chunk_get(chunkFrom), chunkTo - chunkFrom, r, null, setup);
      for (int[] x : p1)
        parsedRecords.add(x.clone());
      data = new int[parsedRecords.size()][];
      data = parsedRecords.toArray(data);
    } else throw new Error("no such key in K/V store");
  }

  public PokerAdapter(File inputFile) throws NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException,
      FileNotFoundException, CSVParseException, IOException {
    int[] r = new int[11];
    ArrayList<int[]> parsedRecords = new ArrayList<int[]>();
    CSVParserSetup setup = new CSVParserSetup();
    setup._parseColumnNames = false;
    ValueCSVRecords<int[]> p1 = new ValueCSVRecords<int[]>(new FileInputStream(
        inputFile), r, null, setup);
    for (int[] x : p1)
      parsedRecords.add(x.clone());
    data = new int[parsedRecords.size()][];
    data = parsedRecords.toArray(data);
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
      PokerAdapter data = new PokerAdapter(f);
      
      System.out.println("done.\n"+data.summary());
      System.out.println("there are " + data.numRows() + " rows and "  + data.numColumns() + " columns");
      RF rf = new RF(data, TREES);
      NumericSplitterStatistic.openForGainBuffering("gains.txt");
      rf.compute();
      NumericSplitterStatistic.closeGainReporting();
      System.out.print("Done. Computing accuracy.");
      System.out.println(rf);
    }
  }
  public Statistic createStatistic() {
  return new NumericSplitterStatistic(this);
  //  return new NumericStat(this);
    //return new AverageStatistic(this);
  }

}
