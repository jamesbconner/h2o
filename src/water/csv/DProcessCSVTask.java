package water.csv;

import water.DKV;
import water.DRemoteTask;
import water.Key;
import water.ValueArray;
import water.csv.CSVParser.CSVParserSetup;

/**
 * Base class for distributed CSV parsing.
 * 
 * Just override processRecord(csvRecord) and reduce().
 * 
 * @author tomas
 * 
 * @param <T>
 */
public abstract class DProcessCSVTask<T> extends DRemoteTask {

  private static final long serialVersionUID = -2869007295927691240L;

  T _csvRecord;
  CSVParserSetup _setup;
  
  boolean _mapFinished;
  
  
  protected final String[] _columns;
 
  abstract protected void processRecord(T csvRecord);  
  
  public DProcessCSVTask(T csvRecord, String[] columns, CSVParserSetup setup)
      throws NoSuchFieldException, SecurityException {
    _csvRecord = csvRecord;
    _setup = setup;
    _columns = columns;    
  }  

  @Override
  public void map(Key key) {    
    int index = ValueArray.getChunkIndex(key);
    System.out.println("Map(" + index + ")");
    Key nextKey = ValueArray.getChunk(key, index+1);
    if (DKV.get(nextKey) == null)
      nextKey = null;
    try {
      ValueCSVRecords<T> records = new ValueCSVRecords<T>(key, nextKey, index,
          _csvRecord, _columns, _setup);      
      for (T r : records) {
        processRecord(r);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new Error("unexpected exception");
    }
    _mapFinished = true;
  }
}
