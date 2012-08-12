package hexlytics.tests;

import hexlytics.RFBuilder.Director;
import hexlytics.data.Data;
import hexlytics.data.DataAdapter;

import java.io.File;
import java.io.FileInputStream;

import water.csv.CSVParser.CSVParserSetup;
import water.csv.ValueCSVRecords;

public class AlphaDSet {
  Data _dset;

  public AlphaDSet(File dataFile, File labelsFile) throws Exception {
    String[] columns = new String[501];
    for (int i = 0; i < 501; ++i)
      columns[i] = Integer.toString(i);
    DataAdapter dset = new DataAdapter("poker", columns, "500");
    double[] dR = new double[501];
    int[] lR = new int[1];
    CSVParserSetup setup = new CSVParserSetup();
    setup._parseColumnNames = false;
    setup._separator = ' ';
    ValueCSVRecords<double[]> dataRec = new ValueCSVRecords<double[]>(
        new FileInputStream(dataFile), dR, null, setup);
    ValueCSVRecords<int[]> labelRec = new ValueCSVRecords<int[]>(
        new FileInputStream(labelsFile), lR, null, setup);
    int counter = 0;
    while (dataRec.hasNext() && labelRec.hasNext()) {
      if(++counter % 1000 == 0) System.out.println("row " + counter);
      if(counter == 50000) break;
      dataRec.next();
      labelRec.next();
      dR[500] = lR[0] == 1 ? 1 : 0;
      dset.addRow(dR);
    }
    dset.freeze();
    _dset = Data.make(dset.shrinkWrap());
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0)
      args = new String[] { "e:\\datasets\\alpha_train" };
    for (String path : args) {
      System.out.print("parsing " + path + "...");
      File data = new File(path + ".dat");
      File labels = new File(path + ".lab");
      if (!data.exists() || !labels.exists()) {
        System.out.println("files not found!");
        continue;
      }
      AlphaDSet dset = new AlphaDSet(data, labels);
      Data d = dset._dset;
      Data train = d.sampleWithReplacement(.6);
      Data valid = train.complement();
      Director dir = Director.createLocal(train, valid, 1000);
    }
  }
}
