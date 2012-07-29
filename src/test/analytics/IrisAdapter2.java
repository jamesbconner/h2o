package test.analytics;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import water.Value;
import water.csv.CSVParser.CSVParseException;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.CSVString;
import water.csv.ValueCSVRecords;
import analytics.AverageStatistic;
import analytics.DataAdapter;
import analytics.RF;
import analytics.RFBuilder;

/**
 * Simple adapter for iris dataset.
 * 
 * Can be created either with value (arraylet root) from K/V store
 * or with File (for testing purposes)
 * 
 * @author tomas
 *
 */
public class IrisAdapter2 extends DataAdapter {

  String[] names = new String[] { "ID", "Sepal.Length", "Sepal.Width",
      "Petal.Length", "Petal.Width", "Species" };

  Map<String, Integer> _classOrdinals;
  Map<Integer, String> _classNames;

  int class2Int(CSVString s) {
    if (s.equals("setosa") || s.equals("Iris-setosa") || s.equals("0"))
      return 0;
    if (s.equals("versicolor") || s.equals("Iris-versicolor") || s.equals("1"))
      return 1;
    if (s.equals("virginica") || s.equals("Iris-virginica") || s.equals("2"))
      return 2;
    throw new Error("unknown class name " + s.toString());
  }

  String int2Class(int i) {
    switch (i) {
    case 0:
      return "setosa";
    case 1:
      return "versicolor";
    case 2:
      return "virginica";
    default:
      throw new Error("unknown class ordinal " + i);
    }
  }

  public class CSVRecord {
    public int id;
    public double sl, sw, pl, pw;
    public CSVString class_;
  }

  class F {
    int id;
    double sl, sw, pl, pw;
    int class_;

    F(int id, CSVRecord r) {
      this.id = id;
      sl = r.sl;
      sw = r.sw;
      pl = r.pl;
      pw = r.pw;
      class_ = class2Int(r.class_);
    }
  }

  F[] data = null;

  public int numColumns() {
    return 5;
  }

  public boolean isInt(int index) {
    return index == 0;
  }

  public int toInt(int index) {
    if (index == 0)
      return data[cur].id;
    else
      throw new Error();
  }

  public double toDouble(int index) {
    if (index > 0 && index <= 4) {
      if (index == 1)
        return data[cur].sl;
      if (index == 2)
        return data[cur].sw;
      if (index == 3)
        return data[cur].pl;
      if (index == 4)
        return data[cur].pw;
    } else if (index == 0)
      return (double) data[cur].id;
    throw new Error("Accessing column " + index);
  }

  public int numRows() {
    return data.length;
  }

  public int numClasses() {
    return 3;
  }

  public int dataClass() {
    return data[cur].class_;
  }

  public IrisAdapter2(Value v) throws NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, CSVParseException,
      IOException {
    this(v, 0, v.chunks() + 1);
  }

  public IrisAdapter2(Value v, int fromChunk, int toChunk)
      throws NoSuchFieldException, SecurityException, IllegalArgumentException,
      IllegalAccessException, CSVParseException, IOException {
    CSVRecord r = new CSVRecord();
    ArrayList<F> parsedRecords = new ArrayList<F>();
    CSVParserSetup setup = new CSVParserSetup();
    setup._parseColumnNames = false;
    int id = 0;
    if (v != null) {
      ValueCSVRecords<CSVRecord> p1 = new ValueCSVRecords<CSVRecord>(v,
          fromChunk, toChunk, r, new String[] { "sl", "sw", "pl", "pw",
              "class_" }, setup);
      for (CSVRecord x : p1) {
        parsedRecords.add(new F(id++, x));
      }
      data = new F[parsedRecords.size()];
      data = parsedRecords.toArray(data);
    } else
      throw new IllegalArgumentException("passed value can not be null");
  }

  public IrisAdapter2(File f) throws NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, FileNotFoundException,
      CSVParseException, IOException {
    CSVRecord r = new CSVRecord();
    ArrayList<F> parsedRecords = new ArrayList<F>();
    CSVParserSetup setup = new CSVParserSetup();
    setup._parseColumnNames = false;
    ValueCSVRecords<CSVRecord> p1 = new ValueCSVRecords<CSVRecord>(
        new FileInputStream(f), r, new String[] { "sl", "sw", "pl", "pw",
            "class_" }, setup);
    int id = 0;
    for (CSVRecord x : p1) {
      parsedRecords.add(new F(id++, x));
    }
    data = new F[parsedRecords.size()];
    data = parsedRecords.toArray(data);
  }

  /**
   *  for debuging purposes...
   * @param args list of filenames to be processed
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    for (String path : args) {
      System.out.print("parsing " + path + "...");
      File f = new File(path);
      if (!f.exists()) {
        System.out.println("file not found");
        continue;
      }
      IrisAdapter2 data = new IrisAdapter2(f);
      System.out.println("done");
      System.out.println("there are " + data.numRows() + " rows and "
          + data.numColumns() + " columns");
      for (int i = 0; i < data.numRows(); ++i) {
        data.seekToRow(i);
        for (int j = 0; j < data.numColumns(); ++j) {
          System.out.print(data.isInt(j) ? data.toInt(j) : data.toDouble(j));
          System.out.print(", ");
        }
        System.out.println(data.dataClass());
      }
      RF rf = new RF(data);
      rf.compute(100, new IrisBuilder2(67436482,data));
    }
  }
}

class IrisBuilder2 extends RFBuilder {
  protected IrisBuilder2(long seed, DataAdapter data) {
    super(seed, data);
  }

  @Override
  protected void createStatistic(ProtoNode node, int[] columns) {
    node.addStatistic(new AverageStatistic(columns, 3));
  }

  @Override
  protected int numberOfFeatures(ProtoNode node, ProtoTree tree) {
    return 3;
  }
}
