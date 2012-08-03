package water.csv;

import java.io.File;
import java.io.FileInputStream;

import water.DKV;
import water.Key;
import water.UDP;
import water.Value;
import water.ValueArray;
import water.csv.CSVParser.CSVParserSetup;

public abstract class DProcessCSVTaskFromFile<T> extends DProcessCSVTask<T> {
    
  private static final long serialVersionUID = 9122574022128679651L;    
    
  public DProcessCSVTaskFromFile(T csvRecord, String[] columns, CSVParserSetup setup) {
    super(csvRecord, columns,setup);        
  }
  
  @Override
  public void map(Key key) {    
    try{
      Value v = DKV.get(key);
      if(v == null) throw new Error("did not find key " + key);
      byte [] mem = v.mem();
      int filenameLen = UDP.get4(mem, 0);
      String filename = new String(mem,4,filenameLen);
      int fileOffset = UDP.get4(mem, 4 + filenameLen);
      int dataLength = UDP.get4(mem, 4 + filenameLen + 4);
      File f = new File(filename);
      ValueCSVRecords<T> records = new ValueCSVRecords<T>(new ValueCSVRecords.StreamDataProvider(1 << ValueArray.LOG_CHK, new FileInputStream(f),fileOffset,dataLength),_csvRecord,null,_setup);
      processRecords(records);
    } catch(Exception e){
      throw new Error(e);      
    }
  }
}
