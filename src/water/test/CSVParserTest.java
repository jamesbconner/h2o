package water.test;

import init.init;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import test.analytics.PokerAvg;
import water.DKV;
import water.H2O;
import water.Key;
import water.UKV;
import water.Value;
import water.ValueArray;
import water.csv.CSVParser.CSVEscapedBoundaryException;
import water.csv.CSVParser.CSVParseException;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.CSVString;
import water.csv.ValueCSVRecords;



public class CSVParserTest {
  public static class Record {
    public String prefix = "";
    public int _totalSamples;
    public int _trigSamples;
    public double _trigSamplesPercent;
    public int _trigDeleg;
    public int _trigMal;
    public double _trigMalPercent;
    @Override
    public String toString(){
      return "{prefix->" + prefix + ", _totalSamples->" + _totalSamples  + ", _trigSamples->" + _trigSamples  + ", _trigSamplesPercent->" + _trigSamplesPercent
        + ", _trigDeleg->" + _trigDeleg + ", _trigMal->" + _trigMal + ", _trigMalPercent->" + _trigMalPercent + "}";
    }
  }

  public static class TestRecord1 {
    public CSVString str1;
    public CSVString str2;

    @Override
    public String toString(){
      return "{str1->" + str1.toString() + ", str2->" + str1.toString() + "}";
    }

  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    if(H2O.CLOUD == null)
      init.main(new String [] {"-ip", "192.168.56.1", "-test", "none"});
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    //Code executed after the last test method
  }

  public static class TestRecord2 {
    public double x;
    public double y;

    public TestRecord2(){}
    public TestRecord2(double x, double y){this.x = x; this.y = y;}
    @Override
    public boolean equals(Object other){
      if(other instanceof TestRecord2){
        TestRecord2 t = (TestRecord2)other;
        return (t.x == x) && (t.y == y);
      }
      return false;
    }

    @Override
    public String toString(){
      return "{x->" + x + ",y->" + y + "}";
    }
  }
  public static class TestRecord3 {
    public double d;
    public float f;
    public int i;
    public CSVString s;

    public TestRecord3(){}
    public TestRecord3(double d, float f, int i) {this.d = d; this.f = f; this.i = i; }

    @Override
    public boolean equals(Object other){
      if(other instanceof TestRecord3){
        TestRecord3 t = (TestRecord3)other;
        return (t.d == d) && (t.f == f) && (t.i == i);
      }
      return false;
    }

    @Override
    public String toString(){
      return "{d->" + d + ", f->" + f + ", i->" + i + ", s->" + s.toString() + "}";
    }
  }

  public static class PyTriggerRecord {
    public CSVString time;
    public CSVString processName;
    public int PID;
    public CSVString Operation;
    public CSVString Path;
    public CSVString Result;
    public CSVString Detail;
  }

  public static class TimeSeriesRecord {
    public CSVString date;
    public double value;
    @Override
    public String toString(){
      return date.toString() + ", " + Double.toString(value);
    }
    @Override
    public boolean equals(Object o){
      if(o instanceof TimeSeriesRecord){
        TimeSeriesRecord other = (TimeSeriesRecord)o;        
        return (Double.isNaN(value) && Double.isNaN(other.value) || (value == other.value)) && date.equals(other.date);
      }
      return false;
    }
  }


  @Test
  public void testLineEnding(){
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 3*1024*1024);
    Key k1 = ValueArray.make_chunkkey(k,0);
    Value v1 = new Value(k1,"\"double\",float,\"int\", CSVString \r\n .123 , .123, 123 ,\r\r\n");
    try{
      DKV.put(k1,v1);
      TestRecord3 r1 = new TestRecord3();
      TestRecord3 [] rExp = {new TestRecord3(.123,.123f,123)};
      ValueCSVRecords<TestRecord3> p1 = new ValueCSVRecords<TestRecord3>(var.chunk_get(0), 1,r1, new String [] {"d","f","i","s"});
      String [] expectedColumnNames = {"double","float","int"," CSVString "};
      Assert.assertTrue(Arrays.equals(expectedColumnNames, p1.columnNames()));
      int i = 0;
      for(TestRecord3 r:p1){
        Assert.assertEquals(r,rExp[i++]);
        Assert.assertTrue(r.s.equals("\r"));
      }
      Assert.assertEquals(i,1);
    } catch(Exception e){
      e.printStackTrace();
    } finally {
      DKV.remove(k1);
    }
  }

  public byte[] randomBytes(int bytelen){
    byte [] bytes = new byte[bytelen];
    for(int i = 0; i < bytelen; ++i){
      bytes[i] = (byte)(256*Math.random());
    }
    return bytes;
  }

  int randInt(int N){
    return (int)(N*Math.random());
  }
  
  @Test
  public void testIgnoredColumn(){
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 3*1024*1024);
    Key k1 = ValueArray.make_chunkkey(k,0);
    Value v1 = new Value(k1,"\"test1\", haha,\"test2\"\n12345,xxx,.12345\n1.2345e13,xxx,-12345\n-1.3e-3,xxx,123\n");
    try{
      DKV.put(k1,v1);
      TestRecord2 r1 = new TestRecord2();
      TestRecord2 [] rExp = {new TestRecord2(12345,.12345),new TestRecord2(1.2345e13,-12345),new TestRecord2(-1.3e-3,123)};
      String [] expectedColumnNames = {"test1", " haha","test2"};
      ValueCSVRecords<TestRecord2> p1 = new ValueCSVRecords<TestRecord2>(var.chunk_get(0), 1, r1, new String [] {"x",null,"y"});
      assert(Arrays.equals(expectedColumnNames, p1.columnNames()));
      int i = 0;
      for(TestRecord2 r:p1){
        Assert.assertEquals(r,rExp[i++]);
      }
      Assert.assertEquals(i,3);
    } catch(Exception e){
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      DKV.remove(k1);
    }
  }
  @Test
  public void testCSVString() {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 1024*1024*3);
    Key k1 = ValueArray.make_chunkkey(k,var.chunk_offset(1));
    Key k2 = ValueArray.make_chunkkey(k,var.chunk_offset(2));

    Value v1 = new Value(k1,"..\nhaha");
    Value v2 = new Value(k2,"gaga,\n");
    try{
      DKV.put(k1,v1);
      DKV.put(k2,v2);
      TestRecord1 r = new TestRecord1();
      ValueCSVRecords<TestRecord1> p1 = new ValueCSVRecords<TestRecord1>(var.chunk_get(1),1, r, new String [] {"str1","str2"});
      for(TestRecord1 x:p1){
        Assert.assertTrue(x.str1.equals("hahagaga"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }finally{
      DKV.remove(k1);
      DKV.remove(k2);
    }
  }

  @Test
  public void testDataTypes() {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 3*1024*1024);
    Key k1 = ValueArray.make_chunkkey(k,0);
    Value v1 = new Value(k1,"\"double\",float,\"int\",CSVString \n .123 , .123, 123, \"123\"\n");
    try{
      DKV.put(k1,v1);
      TestRecord3 r1 = new TestRecord3();
      TestRecord3 [] rExp = {new TestRecord3(.123,.123f,123)};
      ValueCSVRecords<TestRecord3> p1 = new ValueCSVRecords<TestRecord3>(var.chunk_get(0),1, r1, new String [] {"d","f","i","s"});
      String [] expectedColumnNames = {"double","float","int","CSVString "};
      Assert.assertTrue(Arrays.equals(expectedColumnNames, p1.columnNames()));
      int i = 0;
      for(TestRecord3 r:p1){
        Assert.assertEquals(r, rExp[i++]);
      }
      Assert.assertEquals(i,1);
    } catch(Exception e){
      e.printStackTrace();
    } finally {
      DKV.remove(k1);
    }
  }

  @Test
  public void testSeparator() {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 3*1024*1024);
    Key k1 = ValueArray.make_chunkkey(k,0);
    Value v1 = new Value(k1,"\"test1\"  \"test2\" \n 12345 .12345 \n             1.2345e13    -12345\n -1.3e-3 123  \n");

    try{
      DKV.put(k1,v1);
      TestRecord2 r1 = new TestRecord2();
      TestRecord2 [] rExp = {new TestRecord2(12345,.12345),new TestRecord2(1.2345e13,-12345),new TestRecord2(-1.3e-3,123)};
      CSVParserSetup s = new CSVParserSetup((byte)' ',true);
      ValueCSVRecords<TestRecord2> p1 = new ValueCSVRecords<TestRecord2>(var.chunk_get(0), 1, r1, new String [] {"x","y"}, s);
      String [] expectedColumnNames = {"test1","test2"};
      Assert.assertTrue(Arrays.equals(expectedColumnNames, p1.columnNames()));
      int i = 0;
      for(TestRecord2 r:p1){
        Assert.assertEquals(r, rExp[i++]);
      }
      Assert.assertEquals(i,3);
    } catch(Exception e){
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      DKV.remove(k1);
    }
  }

  @Test
  public void testCorrectNumberParsing() {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 3*1024*1024);
    Key k1 = ValueArray.make_chunkkey(k,0);
    Value v1 = new Value(k1,"\"test1\", \"test2\"\n12345,.12345\n1.2345e13,-12345\n-1.3e-3,123\n");
    try{
      DKV.put(k1,v1);
      TestRecord2 r1 = new TestRecord2();
      TestRecord2 [] rExp = {new TestRecord2(12345,.12345),new TestRecord2(1.2345e13,-12345),new TestRecord2(-1.3e-3,123)};
      String [] expectedColumnNames = {"test1","test2"};
      ValueCSVRecords<TestRecord2> p1 = new ValueCSVRecords<TestRecord2>(var.chunk_get(0),1, r1, new String [] {"x","y"});
      assert(Arrays.equals(expectedColumnNames, p1.columnNames()));
      int i = 0;
      for(TestRecord2 r:p1){
        Assert.assertEquals(r,rExp[i++]);
      }
      Assert.assertEquals(i,3);
    } catch(Exception e){
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      DKV.remove(k1);
    }
  }
  @Test
  public void testCorrectBoundaryProcessing() {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 1024*1024*3);
    Key k1 = ValueArray.make_chunkkey(k,var.chunk_offset(0));
    Key k2 = ValueArray.make_chunkkey(k,var.chunk_offset(1));
    Key k3 = ValueArray.make_chunkkey(k,var.chunk_offset(2));
    Value v1 = new Value(k1,"\"test1\", \"test2\"\n1.23456e3, 1.");
    Value v2 = new Value(k2,"23456e3\n1.23456e3,1.2");
    Value v3 = new Value(k3,"3456e3\n,1.23456e3\n");
    try {
      DKV.put(k1, v1);
      DKV.put(k2, v2);
      DKV.put(k3, v3);

      TestRecord2 r1 = new TestRecord2();
      TestRecord2 r2 = new TestRecord2();
      TestRecord2 expectedR = new TestRecord2();
      expectedR.x = 1.23456e3;
      expectedR.y = 1.23456e3;

      ValueCSVRecords<TestRecord2> p1 = new ValueCSVRecords<TestRecord2>(var.chunk_get(0), 1,r1, new String [] {"x","y"});
      ValueCSVRecords<TestRecord2> p2 = new ValueCSVRecords<TestRecord2>(var.chunk_get(1),1,r2, new String [] {"x","y"});
      String [] expectedColumnNames = {"test1","test2"};
      String [] colNames = p1.columnNames();      
      
      Assert.assertTrue(Arrays.equals(expectedColumnNames, p1.columnNames()));
      int i = 0;
      for(TestRecord2 r:p1){
        ++i;
        Assert.assertEquals(r, expectedR);
      }
      Assert.assertEquals(i,1);
      i = 0;
      for(TestRecord2 r:p2){
        ++i;
        Assert.assertEquals(r,expectedR);
      }
      Assert.assertEquals(i,1);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      DKV.remove(k1);
      DKV.remove(k2);
      DKV.remove(k3);
    }

  }

  // does not handle escaped stuff
  ArrayList<String> nextCSVLine(BufferedReader r) {
    ArrayList<String> result = new ArrayList<String>();
    //read comma separated file line by line
    try {
      String strLine = null;
      if((strLine = r.readLine()) != null) {
        //break comma separated line using ","
        StringTokenizer st = new StringTokenizer(strLine, ",");
        while(st.hasMoreTokens()) {
          //display csv values
          result.add(st.nextToken());
        }
        //reset token number
      }
    } catch(Exception e){
      e.printStackTrace();
    }
    return result;
  }
  @Test
public void testUnknownData(){
    Key k = Key.make("Iris.data");
    if(DKV.get(k) == null){
      DKV.put(k, new Value(k,"5.1,3.5,1.4,0.2,Iris-setosa\n4.9,3.0,1.4,0.2,Iris-setosa\n4.7,3.2,1.3,0.2,Iris-setosa"));
    }
    CSVParserSetup setup = new CSVParserSetup();
    setup._partialRecordPolicy = CSVParserSetup.PartialRecordPolicy.fillWithDefaults;
    setup._ignoreAdditionalColumns = true;
    setup._parseColumnNames = false;
    float [] rec1 = new float[3];
    float [] rec2 = new float[10];
    
    float [][] expData = new float[3][];
    expData[0] = new float[]{5.1f,3.5f,1.4f,0.2f, Float.NaN};
    expData[1] = new float[]{4.9f,3.0f,1.4f,0.2f, Float.NaN};
    expData[2] = new float[]{4.7f,3.2f,1.3f,0.2f, Float.NaN};
    
    try {
      ValueCSVRecords<float[]> records1 = new ValueCSVRecords<float[]>(k,1,rec1,null,setup);
      ValueCSVRecords<float[]> records2 = new ValueCSVRecords<float[]>(k,1,rec2,null,setup);
      for(int i = 0; i < 3; ++i){
        records1.next();
        records2.next();
        for(int j = 0; j < 5; ++j){
          if(Float.isNaN(expData[i][j])){
            if(j < rec1.length) Assert.assertTrue(Float.isNaN(rec1[j]));
            if(j < rec2.length && !Float.isNaN(rec2[j]))
              System.out.println(i + ":" + j + ":" + rec2[j]);
            if(j < rec2.length) Assert.assertTrue(Float.isNaN(rec2[j]));            
          } else {
            if((j < rec1.length) && (rec1[j] != expData[i][j]))
              System.out.println(rec1[j] + " != " + expData[i][j]);
            if(j < rec1.length) Assert.assertTrue(rec1[j] == expData[i][j]);
            if(j < rec2.length) Assert.assertTrue(rec2[j] == expData[i][j]);
          }          
        }
        for(int j = expData[i].length; j < Math.max(rec1.length, rec2.length); ++j){
          if(j < rec1.length) Assert.assertTrue(Float.isNaN(rec1[j]));
          if(j < rec2.length) Assert.assertTrue(Float.isNaN(rec2[j]));
        }        
      }
      Assert.assertFalse(records1.hasNext());
      Assert.assertFalse(records2.hasNext());
    } catch (Exception e) {
      throw new Error(e);
    }     
  }
  
  @Test
  public void testParsingbigDataCSV(){
    Key k = Key.make("bigdata_csv");
    Value v = DKV.get(k);
    Assert.assertNotNull(v);
    TimeSeriesRecord r = new TimeSeriesRecord();
    TimeSeriesRecord r2 = new TimeSeriesRecord();
    TimeSeriesRecord r3 = new TimeSeriesRecord();
    final int expectedNRecords = 14102837;
    int recCounter = 0;
    
    
    File f = new File("c:\\Users\\tomas\\big_data.csv");
    Assert.assertTrue(f.exists());    
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(f));
      reader.readLine();
    } catch (FileNotFoundException e1) {
      Assert.assertTrue(false);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      Assert.assertTrue(false);
    }   
    CSVParserSetup setup = new CSVParserSetup();
    if(v != null){
      for(int i = 0; i < 1/*v.chunks()*/; ++i){
        
        System.out.println("Processing chunk # "  + i);
        try {
          ValueCSVRecords<TimeSeriesRecord> p1 = new ValueCSVRecords<TimeSeriesRecord>(v.chunk_get(0), (int)v.chunks(),r, new String [] {"date","value"}, setup);          
          InputStream is = f.exists()?new FileInputStream(f):null;
          ValueCSVRecords<TimeSeriesRecord> p2 = (is != null)?new ValueCSVRecords<TimeSeriesRecord>(is,r2, new String [] {"date","value"}, new CSVParserSetup()):null;
                    
          for(int j = 0; j < v.chunks(); ++j){
            System.out.println("processing chunk #" + j);
            ValueCSVRecords<TimeSeriesRecord> p3 = new ValueCSVRecords<TimeSeriesRecord>(v.chunk_get(j),1,r3, new String [] {"date","value"}, new CSVParserSetup());
            for(TimeSeriesRecord x:p3){
              ++recCounter;
              if(recCounter == 607776){
                System.out.println("*");
              }
              Assert.assertTrue(p1.hasNext());
              r = p1.next();
              Assert.assertTrue((p2 == null)|| p2.hasNext());
              r2 = p2.next();
              if(!r.equals(r2) || ! r.equals(r3)){
                System.out.println("mismatch at record " + recCounter + ": " + r + " <==> " + r2 + " <==>" + r3);
              }
              Assert.assertTrue(r.equals(r2));
              Assert.assertTrue(r.equals(r3));
              ArrayList<String> str = nextCSVLine(reader);
              Assert.assertEquals(str.size(), 2);
              if(!x.date.equals(str.get(0))){
                System.out.println("'"  + x.date.toString() + "' <==> '" + str.get(0) + "'");
              }
              if(!x.date.equals(r2.date.toString())){
                System.out.println("'"  + x.date.toString() + "' <==> '" + str.get(0) + "'" + "' <==> '" + r2.date.toString() + "'");
              }
              Assert.assertTrue(x.date.equals(str.get(0)));
              Assert.assertTrue(x.date.equals(r2.date.toString()));
              if(str.get(1).trim().equals(".")){
                Assert.assertEquals(x.value, setup._defaultDouble);
                Assert.assertEquals(r2.value, setup._defaultDouble);
                continue;
              } else if(x.value != Double.valueOf((str.get(1).trim()))){
                System.out.println(x.value + " != " + Double.valueOf(str.get(1).trim()));
              }
              Assert.assertEquals(x.value,Double.valueOf((str.get(1).trim())));
              Assert.assertEquals(r2.value,Double.valueOf((str.get(1).trim())));  
            }                                                                                                
          }
          System.out.println("parsed " + recCounter + " records");
          Assert.assertFalse(p2.hasNext());
          Assert.assertEquals(expectedNRecords, recCounter);          
        } catch(CSVEscapedBoundaryException e){
          System.out.println("escaped boundary at chunk " + i + ", skipping the next one");
          return;
        } catch (Exception e) {
          System.err.println("error after processing " + recCounter + " records");
          e.printStackTrace();
          Assert.assertTrue(false);
        }
      }
     
    }
  }




  public void doRun(){
    Result result = JUnitCore.runClasses(CSVParserTest.class);
    System.out.println("Test result: " + (result.wasSuccessful()?"OK":"FAILED"));
    System.out.println("\t" + result.getRunCount() + " test run  in " + ((double)result.getRunTime()/1000.0) + "s");
    System.out.println("Failures:");
    for (Failure failure : result.getFailures()) {
      System.out.println("\t" + failure.toString());
      System.out.println("\t\t" + failure.getDescription() + failure.getTrace());
    }
  }
  
  @Test
  public void testParsingPokerCSV(){
    Key k = Key.make("poker.data");
    Value v = DKV.get(k);
    ArrayList<Key> kkk = new ArrayList<Key>();    
    for(int i = 0; i < v.chunks(); ++i){
      Key kk = v.chunk_get(i);
      if(DKV.get(kk) != null)
        kkk.add(kk);    
    }
    Key [] keys = new Key[kkk.size()];
    kkk.toArray(keys);
    try {
      PokerAvg avg = new PokerAvg();      
      avg.rexec(keys);
      System.out.println();
      System.out.println("Processed " + avg.N() + " records");
      System.out.print("Results: ");
      double [] vals = avg.getAvg();      
      for(int i = 0; i < vals.length; ++i)
        System.out.print(" " + vals[i]);
      System.out.println();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  } 
}

//System.out.print("Test CSVString sequence...");
//if(testCSVString())
//System.out.println("ok");
//else
//System.out.println("failed");
//System.out.print("Test line ending parsing...");
//if(testLineEnding())
//System.out.println("ok");
//else
//System.out.println("failed");
//System.out.print("Test correct number parsing...");
//if(testCorrectNumberParsing())
//System.out.println("ok");
//else
//System.out.println("failed");
//
//System.out.print("Test space separator...");
//if(testSeparator())
//System.out.println("ok");
//else
//System.out.println("failed");
//
//System.out.print("Test correct boundary processing");
//if(testCorrectBoundaryProcessing())
//System.out.println("ok");
//else
//System.out.println("failed");
//}

