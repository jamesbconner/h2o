package test;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import water.H2O;
import water.Key;
import water.parser.CSVParser.CSVParseException;
import water.parser.*;


public class CSVParserKVTest {
 
  public static class TimeSeriesRecord {
    public double value;
    public CSVParserKV<?>.CSVString date;
  }
  
  public static class TimeSeriesRecordOld {
    public double value;
    public CSVString date;
  }
  
  @BeforeClass static public void startLocalNode() {
    H2O.main(new String[] {});
  }

  @Test public void testBasicCSVParse() {
    byte [] data = (
        "1.0,2.0,.,0.3e1,0.04e2,50.0e-1\n" +
        " 100.0e-3 , -2.0,. , 3.0, 4.0  ,  5.0\n"
        ).getBytes();
    float [] rec1 = new float[6];
    float [] rec2 = new float[3];
    float [] rec3 = new float[6];
    CSVParserKV<float[]> p1 = new CSVParserKV<float[]>(data,rec1,null);
    CSVParserKV<float[]> p2 = new CSVParserKV<float[]>(data,rec2,null);
    CSVParserKV<float[]> p3 = new CSVParserKV<float[]>(data,rec3,null);
    for( int i = 0; i < 2; ++i ) {
      p1.next();
      p2.next();
      p3.next();
    }
  }
  
  @Test
  public void testNColumns(){    
    CSVParserKV.ParserSetup setup = new CSVParserKV.ParserSetup();
    setup.parseColumnNames = true;
    Key key = KVTest.load_test_file("smalldata/poker/poker10");
    int n = CSVParserKV.getNColumns(key);
    Assert.assertEquals(11, n);
    int [] rec = new int[n];
    CSVParserKV<int[]> p = new CSVParserKV<int[]>(key,1,rec,null);
    int x[] = new int[]{1,1,1,2,1,3,1,4,1,5,8}; // straight-flush A,2,3,4,5
    Assert.assertArrayEquals(x, p.next());
    x[10]=5;                    // For the rest, the final class column is 5: a flush
    int q=6;                    // A,2,3,4,6..K
    for(int [] y:p) {           // Parse the rest
      x[9]=q++;                 // just flipping the final card
      Assert.assertArrayEquals(x, y);
    }
  }  
  
  @Test
  public void testSpeedOfParsingbigDataCSV(){    
    TimeSeriesRecord r = new TimeSeriesRecord();
    CSVParserKV.ParserSetup setup = new CSVParserKV.ParserSetup();
    setup.parseColumnNames = true;
    CSVParserKV<TimeSeriesRecord> p = new CSVParserKV<TimeSeriesRecord>(Key.make("bigdata_csv"),Integer.MAX_VALUE,r,new String [] {"date","value"}, setup);
    System.out.println(Arrays.toString(p.columnNames()));
    int counter = 0;
    for(TimeSeriesRecord x:p){
      Assert.assertNotNull(x);
      ++counter;      
    }                           
    Assert.assertEquals(14102837, counter);    
  }  
      
  @Test
  public void testSpeedOfParsingbigDataCSV_OldParser() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, CSVParseException, IOException{    
    TimeSeriesRecordOld r = new TimeSeriesRecordOld();
    ValueCSVRecords<TimeSeriesRecordOld> p = new ValueCSVRecords<TimeSeriesRecordOld>(Key.make("bigdata_csv"),Integer.MAX_VALUE,r,new String [] {"date","value"});
    int counter = 0;
    for(TimeSeriesRecordOld x:p){
      Assert.assertNotNull(x);
      ++counter;      
    }                           
    Assert.assertEquals(counter,14102837);    
    
  }
  
  @Test
  public void testParsingBigDataCSV() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, CSVParseException, IOException{
    System.out.println(Arrays.toString(CSVParserKV.getColumnNames(Key.make("bigdata_csv"))));
    TimeSeriesRecord r = new TimeSeriesRecord();
    CSVParserKV.ParserSetup setup = new CSVParserKV.ParserSetup();
    setup.parseColumnNames = true;
    CSVParserKV<TimeSeriesRecord> p = new CSVParserKV<TimeSeriesRecord>(Key.make("bigdata_csv"),Integer.MAX_VALUE,r,new String [] {"date","value"}, setup);
    System.out.println(Arrays.toString(p.columnNames()));
    int counter = 0;
    TimeSeriesRecordOld rOld = new TimeSeriesRecordOld();
    ValueCSVRecords<TimeSeriesRecordOld> pOld = new ValueCSVRecords<TimeSeriesRecordOld>(Key.make("bigdata_csv"),Integer.MAX_VALUE,rOld,new String [] {"date","value"});
        
    for(TimeSeriesRecord x:p){
      ++counter;
      pOld.next();
      Assert.assertTrue((rOld.value == x.value) || (Double.isNaN(rOld.value) && Double.isNaN(x.value)));      
      if(!rOld.date.toString().equals(x.date.toString())){
        System.out.println(counter + ": " + rOld.date.toString() + " != " + x.date.toString());
      }
      Assert.assertTrue(rOld.date.toString().equals(x.date.toString()));
      Assert.assertTrue(x.date.equals(rOld.date.toString()));
      Assert.assertTrue(x.date.compareTo(rOld.date.toString()) == 0);      
    }                           
    Assert.assertEquals(counter,14102837);    
  }
  
  
}
