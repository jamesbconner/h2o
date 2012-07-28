package test.analytics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import water.csv.CSVParser.CSVParseException;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.ValueCSVRecords;
import analytics.DataAdapter;

public class PokerAdapter implements DataAdapter{

  int [][] data;  
  int currentRow;
  HashSet<Integer> _classes;
  
  public void getRow(int index) {  currentRow = index;  }
  public int numRows() { return data.length;  }
  public int numColumns() { return data[0].length-1; }
  public boolean isInt(int index) {  return true;  }
  public int toInt(int index) { return data[currentRow][index]; }
  public double toDouble(int index) { return data[currentRow][index]; }
  public Object originals(int index) { return null; }
  public int numClasses() { return _classes.size(); }
  public int dataClass() {return data[currentRow][data[currentRow].length-1]; }
  
  public PokerAdapter(File inputFile) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, FileNotFoundException, CSVParseException, IOException{
    int[] r = new int[11];  
    ArrayList<int[]> parsedRecords = new ArrayList<int[]>();
    CSVParserSetup setup = new CSVParserSetup();
    setup._parseColumnNames = false;
    ValueCSVRecords<int[]> p1 = new ValueCSVRecords<int[]>(
        new FileInputStream(inputFile), r, new String[] { "id", "sl", "sw", "pl", "pw", "class_" }, setup);
    for (int[] x : p1) parsedRecords.add(x.clone());
    data = new int[parsedRecords.size()][];
    data = parsedRecords.toArray(data);
  }
    
  public static void main(String[] _) throws Exception {
    PokerAdapter data = new PokerAdapter(new File("C:\\datasets\\poker-hand-testing.data"));    
    System.out.println("there are " + data.numRows() + " rows and " + data.numColumns() + " columns");
    for(int i = 0; i < data.numRows(); ++i){
      data.getRow(i);
      for(int j = 0; j < data.numColumns(); ++j){        
        System.out.print(data.toInt(j));
        System.out.print(", ");
      }
      System.out.println(data.dataClass());
    }
  }
  
}
