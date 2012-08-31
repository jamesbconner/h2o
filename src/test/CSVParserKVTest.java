package test;

import init.init;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import water.DKV;
import water.H2O;
import water.Key;
import water.Value;
import water.parser.CSVParser.CSVParseException;
import water.parser.CSVParserKV;
import water.parser.CSVString;
import water.parser.ValueCSVRecords;


public class CSVParserKVTest {
 
  public static class TimeSeriesRecord {
    public double value;
    public CSVParserKV.CSVString date;
  }
  
  public static class TimeSeriesRecordOld {
    public double value;
    public CSVString date;
  }
  
  @BeforeClass static public void startLocalNode() {
    H2O.main(new String[] {});
    System.out.println("Running tests in "+CSVParserKVTest.class);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    //Code executed after the last test method
  }  
  
  @Test
  public void testBasicCSVParse() {
    byte [] data = "1.0,2.0,.,0.3e1,0.04e2,50.0e-1\n 100.0e-3 , -2.0,. , 3.0, 4.0  ,  5.0\n".getBytes();
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
    TimeSeriesRecord r = new TimeSeriesRecord();
    CSVParserKV.ParserSetup setup = new CSVParserKV.ParserSetup();
    setup.parseColumnNames = true;
    Key key = KVTest.load_test_file("smalldata/poker/poker10");
    int n = CSVParserKV.getNColumns(key);
    Assert.assertEquals(n,11);
    int [] rec = new int[n];
    CSVParserKV<int[]> p = new CSVParserKV<int[]>(key,1,rec,null);
    int x[] = new int[]{1,1,1,2,1,3,1,4,1,5,8}; // straight-flush A,2,3,4,5
    Assert.assertTrue(Arrays.equals(x,p.next()));
    x[10]=5;                    // For the rest, the final class column is 5: a flush
    int q=6;                    // A,2,3,4,6..K
    for(int [] y:p) {           // Parse the rest
      x[9]=q++;                 // just flipping the final card
      Assert.assertTrue(Arrays.equals(x,y));
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
      ++counter;      
    }                           
    Assert.assertEquals(counter,14102837);    
  }  
      
  @Test
  public void testSpeedOfParsingbigDataCSV_OldParser() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, CSVParseException, IOException{    
    TimeSeriesRecordOld r = new TimeSeriesRecordOld();
    ValueCSVRecords<TimeSeriesRecordOld> p = new ValueCSVRecords<TimeSeriesRecordOld>(Key.make("bigdata_csv"),Integer.MAX_VALUE,r,new String [] {"date","value"});
    int counter = 0;
    for(TimeSeriesRecordOld x:p){
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
  
  
//  // does not handle escaped stuff
//  ArrayList<String> nextCSVLine(BufferedReader r) {
//    ArrayList<String> result = new ArrayList<String>();
//    //read comma separated file line by line
//    try {
//      String strLine = null;
//      if((strLine = r.readLine()) != null) {
//        //break comma separated line using ","
//        StringTokenizer st = new StringTokenizer(strLine, ",");
//        while(st.hasMoreTokens()) {
//          //display csv values
//          result.add(st.nextToken());
//        }
//        //reset token number
//      }
//    } catch(Exception e){
//      e.printStackTrace();
//    }
//    return result;
//  }
//  
//  @Test
//  public void testParsingbigDataCSV(){
//    Key k = Key.make("bigdata_csv");
//    Value v = DKV.get(k);
//    Assert.assertNotNull(v);
//    TimeSeriesRecord r = new TimeSeriesRecord();
//    TimeSeriesRecord r2 = new TimeSeriesRecord();
//    TimeSeriesRecord r3 = new TimeSeriesRecord();
//    final int expectedNRecords = 14102837;
//    int recCounter = 0;
//    
//    
//    File f = new File("c:\\Users\\tomas\\big_data.csv");
//    Assert.assertTrue(f.exists());    
//    BufferedReader reader = null;
//    try {
//      reader = new BufferedReader(new FileReader(f));
//      reader.readLine();
//    } catch (FileNotFoundException e1) {
//      Assert.assertTrue(false);
//    } catch (IOException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//      Assert.assertTrue(false);
//    }   
//    CSVParserKV.ParserSetup setup = new CSVParserKV.ParserSetup();
//    setup.parseColumnNames = true;
//    if(v != null){
//      for(int i = 0; i < 1/*v.chunks()*/; ++i){
//        
//        System.out.println("Processing chunk # "  + i);
//        try {
//          ValueCSVRecords<TimeSeriesRecord> p1 = new ValueCSVRecords<TimeSeriesRecord>(v.chunk_get(0), v.chunks(),r, new String [] {"date","value"}, setup);          
//          InputStream is = f.exists()?new FileInputStream(f):null;
//          ValueCSVRecords<TimeSeriesRecord> p2 = (is != null)?new ValueCSVRecords<TimeSeriesRecord>(is,r2, new String [] {"date","value"}, new CSVParserSetup()):null;
//                    
//          for(int j = 0; j < v.chunks(); ++j){
//            System.out.println("processing chunk #" + j);
//            ValueCSVRecords<TimeSeriesRecord> p3 = new ValueCSVRecords<TimeSeriesRecord>(v.chunk_get(j),1,r3, new String [] {"date","value"}, new CSVParserSetup());
//            for(TimeSeriesRecord x:p3){
//              ++recCounter;
//              if(recCounter == 607776){
//                System.out.println("*");
//              }
//              Assert.assertTrue(p1.hasNext());
//              r = p1.next();
//              Assert.assertTrue((p2 == null)|| p2.hasNext());
//              r2 = p2.next();
//              if(!r.equals(r2) || ! r.equals(r3)){
//                System.out.println("mismatch at record " + recCounter + ": " + r + " <==> " + r2 + " <==>" + r3);
//              }
//              Assert.assertTrue(r.equals(r2));
//              Assert.assertTrue(r.equals(r3));
//              ArrayList<String> str = nextCSVLine(reader);
//              Assert.assertEquals(str.size(), 2);
//              if(!x.date.equals(str.get(0))){
//                System.out.println("'"  + x.date.toString() + "' <==> '" + str.get(0) + "'");
//              }
//              if(!x.date.equals(r2.date.toString())){
//                System.out.println("'"  + x.date.toString() + "' <==> '" + str.get(0) + "'" + "' <==> '" + r2.date.toString() + "'");
//              }
//              Assert.assertTrue(x.date.equals(str.get(0)));
//              Assert.assertTrue(x.date.equals(r2.date.toString()));
//              if(str.get(1).trim().equals(".")){
//                Assert.assertEquals(x.value, setup._defaultDouble);
//                Assert.assertEquals(r2.value, setup._defaultDouble);
//                continue;
//              } else if(x.value != Double.valueOf((str.get(1).trim()))){
//                System.out.println(x.value + " != " + Double.valueOf(str.get(1).trim()));
//              }
//              Assert.assertEquals(x.value,Double.valueOf((str.get(1).trim())));
//              Assert.assertEquals(r2.value,Double.valueOf((str.get(1).trim())));  
//            }                                                                                                
//          }
//          System.out.println("parsed " + recCounter + " records");
//          Assert.assertFalse(p2.hasNext());
//          Assert.assertEquals(expectedNRecords, recCounter);          
//        } catch(CSVEscapedBoundaryException e){
//          System.out.println("escaped boundary at chunk " + i + ", skipping the next one");
//          return;
//        } catch (Exception e) {
//          System.err.println("error after processing " + recCounter + " records");
//          e.printStackTrace();
//          Assert.assertTrue(false);
//        }
//      }    
//    }
//  }
}
