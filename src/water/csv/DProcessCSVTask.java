package water.csv;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;

import water.DKV;
import water.DRemoteTask;
import water.Key;
import water.RemoteTask;
import water.UDP;
import water.ValueArray;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.CSVParser.DataType;

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
  
  
  boolean _isArray;
  
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
    int index = getChunkIndex(key);
    Key nextKey = getChunk(key, index+1);
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
  }

 
  /**
   * Get the index of this chunks ASSUMING it is an arraylet chunk.
   * If not, the number returned does not make sense.  
   *   
   * @param k - key of arraylet chunk 
   * @return - index (offset) of arraylet chunk
   */
  public static int getChunkIndex(Key k) {
    if(k._kb[0] != 0 || k._kb[1] != 0) // arraylet chunks are system keys 
      throw new IllegalArgumentException("can only work an an arraylet chunk (must be a system key)");
    long n = UDP.get8(k._kb, 2);
    return (int) (n >> ValueArray.LOG_CHK);
  }

  /**
   * Get the index of this chunks ASSUMING it is an arraylet chunk.
   * If not, the number returned does not make sense.  
   *   
   * @param k - key of arraylet chunk 
   * @return - index (offset) of arraylet chunk
   */
  public static Key getChunk(Key k, int index) {
    if(k._kb[0] != 0 || k._kb[1] != 0) // arraylet chunks are system keys 
      throw new IllegalArgumentException("can only work an an arraylet chunk (must be a system key)");
    byte[] arr = k._kb.clone();
    long n = ((long) index) << ValueArray.LOG_CHK;
    UDP.set8(arr, 2, n);
    return Key.make(arr);
  }

}
