package test;
import java.io.*;
import java.util.Arrays;

import org.junit.*;

import water.*;
import water.parser.CSVParser.CSVEscapedBoundaryException;
import water.parser.CSVParser.CSVParserSetup;
import water.parser.*;


public class CSVParserTest {
  @BeforeClass
  public static void setUpClass() throws Exception {
      H2O.main(new String[] {});
  }

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
  public void testLineEnding() throws Exception {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 3*1024*1024,Value.ICE);
    Key k1 = ValueArray.make_chunkkey(k,0);
    Value v1 = new Value(k1,"\"double\",float,\"int\", CSVString \r\n .123 , .123, 123 ,\r\r\n");
    try{
      DKV.put(k1,v1);
      TestRecord3 r1 = new TestRecord3();
      TestRecord3 [] rExp = {new TestRecord3(.123,.123f,123)};
      ValueCSVRecords<TestRecord3> p1 = new ValueCSVRecords<TestRecord3>(var.chunk_get(0), 1,r1, new String [] {"d","f","i","s"});
      String [] expectedColumnNames = {"double","float","int"," CSVString "};
      Assert.assertArrayEquals(expectedColumnNames, p1.columnNames());
      int i = 0;
      for(TestRecord3 r:p1){
        Assert.assertEquals(r,rExp[i++]);
        Assert.assertTrue(r.s.equals("\r"));
      }
      Assert.assertEquals(i,1);
    } finally {
      DKV.remove(k1);
    }
  }

  @Test public void testIgnoredColumn() throws Exception {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 3*1024*1024, Value.ICE);
    Key k1 = ValueArray.make_chunkkey(k,0);
    Value v1 = new Value(k1,
        "\"test1\", haha,\"test2\"\n" +
        "12345,xxx,.12345\n" +
        "1.2345e13,xxx,-12345\n" +
        "-1.3e-3,xxx,123\n");
    TestRecord2 [] rExp = {
        new TestRecord2(12345,.12345),
        new TestRecord2(1.2345e13,-12345),
        new TestRecord2(-1.3e-3,123)
    };
    try{
      DKV.put(k1,v1);
      TestRecord2 r1 = new TestRecord2();
      String [] expectedColumnNames = {"test1", " haha","test2"};
      ValueCSVRecords<TestRecord2> p1 = new ValueCSVRecords<TestRecord2>(var.chunk_get(0), 1, r1, new String [] {"x",null,"y"});
      Assert.assertArrayEquals(expectedColumnNames, p1.columnNames());
      int i = 0;
      for(TestRecord2 r:p1){
        Assert.assertEquals(rExp[i++], r);
      }
      Assert.assertEquals(3, i);
    } finally {
      DKV.remove(k1);
    }
  }
  
  @Test
  public void testCSVString() throws Exception {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 1024*1024*3, Value.ICE);
    Key k1 = ValueArray.make_chunkkey(k,Value.chunk_offset(1));
    Key k2 = ValueArray.make_chunkkey(k,Value.chunk_offset(2));

    Value v1 = new Value(k1,"..\nhaha");
    Value v2 = new Value(k2,"gaga,\n");
    try {
      DKV.put(k1,v1);
      DKV.put(k2,v2);
      TestRecord1 r = new TestRecord1();
      ValueCSVRecords<TestRecord1> p1 = new ValueCSVRecords<TestRecord1>(var.chunk_get(1),1, r, new String [] {"str1","str2"});
      for(TestRecord1 x:p1) {
        Assert.assertEquals("hahagaga", x.str1.toString());
      }
    } finally {
      DKV.remove(k1);
      DKV.remove(k2);
    }
  }

  @Test
  public void testDataTypes() throws Exception {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 3*1024*1024, Value.ICE);
    Key k1 = ValueArray.make_chunkkey(k,0);
    Value v1 = new Value(k1,
        "\"double\",float,\"int\",CSVString \n" +
        " .123 , .123, 123, \"123\"\n");
    TestRecord3 [] rExp = {
        new TestRecord3(.123,.123f,123)
    };
    try{
      DKV.put(k1,v1);
      TestRecord3 r1 = new TestRecord3();
      ValueCSVRecords<TestRecord3> p1 = new ValueCSVRecords<TestRecord3>(var.chunk_get(0),1, r1, new String [] {"d","f","i","s"});
      String [] expectedColumnNames = {"double","float","int","CSVString "};
      Assert.assertArrayEquals(expectedColumnNames, p1.columnNames());
      int i = 0;
      for(TestRecord3 r:p1){
        Assert.assertEquals(rExp[i++], r);
      }
      Assert.assertEquals(1, i);
    } finally {
      DKV.remove(k1);
    }
  }

  @Test
  public void testSeparator() throws Exception {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 3*1024*1024, Value.ICE);
    Key k1 = ValueArray.make_chunkkey(k,0);
    Value v1 = new Value(k1,
        "\"test1\"  \"test2\" \n" +
        " 12345 .12345 \n" +
        "             1.2345e13    -12345\n" +
        " -1.3e-3 123  \n");
    TestRecord2 [] rExp = {
        new TestRecord2(12345,.12345),
        new TestRecord2(1.2345e13,-12345),
        new TestRecord2(-1.3e-3,123)
    };

    try{
      DKV.put(k1,v1);
      TestRecord2 r1 = new TestRecord2();
      CSVParserSetup s = new CSVParserSetup((byte)' ',true);
      ValueCSVRecords<TestRecord2> p1 = new ValueCSVRecords<TestRecord2>(var.chunk_get(0), 1, r1, new String [] {"x","y"}, s);
      String [] expectedColumnNames = {"test1","test2"};
      Assert.assertArrayEquals(expectedColumnNames, p1.columnNames());
      int i = 0;
      for(TestRecord2 r:p1){
        Assert.assertEquals(rExp[i++], r);
      }
      Assert.assertEquals(3, i);
    } finally {
      DKV.remove(k1);
    }
  }

  @Test
  public void testCorrectNumberParsing() throws Exception {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 3*1024*1024, Value.ICE);
    Key k1 = ValueArray.make_chunkkey(k,0);
    Value v1 = new Value(k1,
        "\"test1\", \"test2\"\n" +
        "12345,.12345\n" +
        "1.2345e13,-12345\n" +
        "-1.3e-3,123\n");
    TestRecord2 [] rExp = {
        new TestRecord2(12345,.12345),
        new TestRecord2(1.2345e13,-12345),
        new TestRecord2(-1.3e-3,123)
    };
    try{
      DKV.put(k1,v1);
      TestRecord2 r1 = new TestRecord2();
      String [] expectedColumnNames = {"test1","test2"};
      ValueCSVRecords<TestRecord2> p1 = new ValueCSVRecords<TestRecord2>(var.chunk_get(0),1, r1, new String [] {"x","y"});
      assert(Arrays.equals(expectedColumnNames, p1.columnNames()));
      int i = 0;
      for(TestRecord2 r:p1){
        Assert.assertEquals(rExp[i++], r);
      }
      Assert.assertEquals(3, i);
    } finally {
      DKV.remove(k1);
    }
  }
  @Test
  public void testCorrectBoundaryProcessing() throws Exception {
    Key k = Key.make("csvTest");
    ValueArray var = new ValueArray(k, 1024*1024*3, Value.ICE);
    Key k1 = ValueArray.make_chunkkey(k,Value.chunk_offset(0));
    Key k2 = ValueArray.make_chunkkey(k,Value.chunk_offset(1));
    Key k3 = ValueArray.make_chunkkey(k,Value.chunk_offset(2));
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
      
      Assert.assertArrayEquals(expectedColumnNames, p1.columnNames());
      int i = 0;
      for(TestRecord2 r:p1){
        Assert.assertEquals(expectedR, r);
        ++i;
      }
      i = 0;
      for(TestRecord2 r:p2){
        ++i;
        Assert.assertEquals(expectedR, r);
      }
      Assert.assertEquals(1, i);
    } finally {
      DKV.remove(k1);
      DKV.remove(k2);
      DKV.remove(k3);
    }

  }

  // does not handle escaped stuff
  String[] nextCSVLine(BufferedReader r) throws Exception {
    String strLine = r.readLine();
    if (strLine == null) return null;
    return strLine.split(",");
  }
  
  @Test
  public void testUnknownData() throws Exception {
    Key k = Key.make("Iris.data");
    if(DKV.get(k) == null) {
      DKV.put(k, new Value(k,
          "5.1,3.5,1.4,0.2,Iris-setosa\n" +
          "4.9,3.0,1.4,0.2,Iris-setosa\n" +
          "4.7,3.2,1.3,0.2,Iris-setosa"));
    }
    CSVParserSetup setup = new CSVParserSetup();
    setup._partialRecordPolicy = CSVParserSetup.PartialRecordPolicy.fillWithDefaults;
    setup._ignoreAdditionalColumns = true;
    setup._parseColumnNames = false;
    float [] rec1 = new float[3];
    float [] rec2 = new float[10];
    float [][] expData = new float[][] {
        {5.1f,3.5f,1.4f,0.2f, Float.NaN},
        {4.9f,3.0f,1.4f,0.2f, Float.NaN},
        {4.7f,3.2f,1.3f,0.2f, Float.NaN},
    };

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
          if(j < rec1.length) Assert.assertEquals(expData[i][j], rec1[j], 0.0001);
          if(j < rec2.length) Assert.assertEquals(expData[i][j], rec2[j], 0.0001);
        }          
      }
      for(int j = expData[i].length; j < Math.max(rec1.length, rec2.length); ++j){
        if(j < rec1.length) Assert.assertTrue(Float.isNaN(rec1[j]));
        if(j < rec2.length) Assert.assertTrue(Float.isNaN(rec2[j]));
      }        
    }
    Assert.assertFalse(records1.hasNext());
    Assert.assertFalse(records2.hasNext());
  }
  
  @Test
  public void testParsingbigDataCSV() throws Exception {
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
    BufferedReader reader = new BufferedReader(new FileReader(f));
    reader.readLine();
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
              Assert.assertTrue(p2.hasNext());
              r2 = p2.next();
              Assert.assertEquals(r, r2);
              Assert.assertEquals(r, r3);
              
              String[] str = nextCSVLine(reader);
              Assert.assertEquals(str.length, 2);
              String d = str[0];
              String trim = str[1].trim();
              Assert.assertEquals(x.date, d);
              Assert.assertEquals(x.date, r2.date);
              if(trim.equals(".")) {
                Assert.assertEquals(x.value, setup._defaultDouble, 0.0001);
                Assert.assertEquals(r2.value, setup._defaultDouble, 0.0001);
                continue;
              } else {
                double value = Double.valueOf(trim);
                Assert.assertEquals(x.value, value, 0.0001);
                Assert.assertEquals(r2.value,value, 0.0001);
              }
            }                                                                                                
          }
          System.out.println("parsed " + recCounter + " records");
          Assert.assertFalse(p2.hasNext());
          Assert.assertEquals(expectedNRecords, recCounter);          
        } catch(CSVEscapedBoundaryException e){
          System.out.println("escaped boundary at chunk " + i + ", skipping the next one");
          return;
        }
      }
    }
  }
}
