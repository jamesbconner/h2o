package test.analytics;

import java.io.File;
import java.io.FileInputStream;

import water.ValueArray;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.ValueCSVRecords;
import analytics.AverageStatistic;
import analytics.DataAdapter;
import analytics.Statistic;

public class AlphaDSetAdapter extends DataAdapter {

  DataChunk chunk = new DataChunk();
  int maxRow;

  AlphaDSetAdapter() {
    chunk.addIntCol();
    for (int i = 0; i < 500; i++)
      chunk.addFloatCol();
  }

  void addRow(float[] data, int clazz) {
    chunk.setI(clazz, maxRow, 0);
    for (int i = 0; i < 500; i++)
      chunk.setF(data[i], maxRow, i+1);
    maxRow++;
  }

  public int numRows() {
    return maxRow;
  }

  public int numColumns() {
    return 500;
  }

  public boolean isInt(int index) {
    return index == 0;
  }

  public int toInt(int index) {
    return (index == 0) ? chunk.getI(cur, index) : Math.round(chunk.getF(cur,
        index));
  }

  public double toDouble(int index) {
    return chunk.getF(cur, index);
  }

  public int numClasses() {
    return 2;
  }

  public int dataClass() {
    return chunk.getI(cur, 0);
  }

  public int numFeatures() {
    return 500;
  }

  public Statistic createStatistic() {
    return new AverageStatistic(this);
  }

  public AlphaDSetAdapter(File data, File labels) {
    chunk.addIntCol();
    for (int i = 0; i < 500; i++)
      chunk.addFloatCol();
    try {
      FileInputStream dataIn = new FileInputStream(data);
      float[] rec = new float[500];
      int [] label = new int[1];
      CSVParserSetup setup = new CSVParserSetup();
      setup._parseColumnNames = false;
      setup._skipFirstRecord = false;
      setup._separator = (byte) ' ';
      setup._collapseSpaceSeparators = true;

      ValueCSVRecords<float[]> dataRecords = new ValueCSVRecords<float[]>(
          new ValueCSVRecords.StreamDataProvider(1 << ValueArray.LOG_CHK, dataIn,
              0, Long.MAX_VALUE), rec, null, setup);
      FileInputStream labelsIn = new FileInputStream(labels);
      ValueCSVRecords<int[]> labelRecords = new ValueCSVRecords<int[]>(
          new ValueCSVRecords.StreamDataProvider(1 << ValueArray.LOG_CHK, labelsIn,
              0, Long.MAX_VALUE), label, null, setup);
      int rowIdx = 0;
      while(dataRecords.hasNext() && labelRecords.hasNext()){
        dataRecords.next();
        labelRecords.next();
        if((++rowIdx % 1000) == 0)
          System.out.println("row " + rowIdx);
        addRow(rec, label[0]);
      }      
    } catch (Exception e) {
      throw new Error(e);
    }
  }
}
