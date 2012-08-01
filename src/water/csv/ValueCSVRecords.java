package water.csv;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import water.DKV;
import water.Key;
import water.UKV;
import water.Value;
import water.csv.CSVParser.CSVEscapedBoundaryException;
import water.csv.CSVParser.CSVParseException;
import water.csv.CSVParser.CSVParserSetup;


/**
 * Wrapper around CSVParser implementing iterator interface.
 *
 * Allows for iteration over csv records stored in a Value. if passed Value is
 * an arraylet, its behavior depends on the index.  The chunk 0 is parsed
 * completely + the first record of chunk 1. For all other chunks, first record
 * is skipped and the first record from the next chunk is parsed as well.
 *
 * Note that escaped sequences (quoted) are not allowed to cross a chunk
 * boundary. Such record will cause an exception.
 *
 * Iterator interface returns this as an iterator, meaning all iterators
 * returned by iterator() are shared and call to iterator() method reset all
 * previously returned iterators (which are all in fact the same object).
 *
 * Objects returned by iterator's next method all all the same object as the
 * one passed in as an csvRecord argument with the values of its attributes set
 * by the method.  Therefore, every call to the next or hasNext() method
 * overwrites previously returned data.
 *
 * @author tomas
 *
 */
public class ValueCSVRecords<T> implements Iterable<T>,Iterator<T> {
  T _rec;
  String [] _columns;
  
  
  abstract class ADataProvider {
    int _currentChunkId;
    final int _maxChunkId;
    
    abstract boolean hasMoreData();
    final boolean lastChunk() {
      return (_currentChunkId == _maxChunkId-1);
    }
    public ADataProvider(int chunkId,int maxChunkId) {
      _currentChunkId = chunkId;
      _maxChunkId = maxChunkId;
    }
    final byte [] nextData(){
      return nextData(Integer.MAX_VALUE);
    }
    abstract byte [] nextData(int len);
    final int chunkIdx(){return _currentChunkId;};
    void close(){}
    
  }
  
  class KV_DataProvider extends ADataProvider {
    Value _root; // arraylet root
    Value _currentChunk;    
    int _dataRead;
    
    private boolean getChunk(){
      if(_currentChunkId < _root.chunks()){
        Key k = _root.chunk_get(_currentChunkId);
        _currentChunk = DKV.get(k);             
        return(_currentChunk != null);
      } else {
        _currentChunk = null;        
        return false;
      }         
    }
    
    @Override
    byte[] nextData(int len) {
      if(_currentChunk == null)
        return null;
      if(_dataRead == _currentChunk.length()){        
        if(++_currentChunkId > _maxChunkId) 
          return null;
        if(!getChunk())
          return null;
        _dataRead = 0;
      }
      int N = _dataRead + len;
      byte [] res = _currentChunk.get(N);
      if(_dataRead > 0){
        res = Arrays.copyOfRange(res, _dataRead, res.length);
      }
      _dataRead = res.length;
      return res;      
    }   
    
    KV_DataProvider(Value v, int indexFrom, int indexTo){   
      super(indexFrom,indexTo);
      _root = v;
      _currentChunkId = indexFrom;      
      getChunk();
    }
    void close(){_currentChunkId = _maxChunkId+1;}

    @Override
    boolean hasMoreData() {
      return (_currentChunk != null) && ((_dataRead < _currentChunk.length()) || (_currentChunkId < _maxChunkId));      
    }
  } 
  
  class Simple_KV_DataProvider extends ADataProvider {
    Value _firstChunk; // arraylet root
    Value _secondChunk;
    
    int _dataRead;    
    
        
    @Override
    byte[] nextData(int len) {
      int N = _dataRead + len;
      byte [] res;
      if(_dataRead < _firstChunk.length()){
        res = _firstChunk.get(N);
        res = (_dataRead == 0)?res:Arrays.copyOfRange(res, _dataRead, res.length);
        _dataRead = res.length;
      } else {
        if(_secondChunk == null) return null;
        assert N > _firstChunk.length();
        N -= (int)_firstChunk.length();
        res = _secondChunk.get(N);        
        res = (_dataRead == _firstChunk.length())?res:Arrays.copyOfRange(res, _dataRead - (int)_firstChunk.length(), res.length);
        _dataRead = (int)_firstChunk.length() + res.length;
      }            
      return res;
    }   
    
    Simple_KV_DataProvider(Key k1, Key k2){   
      super(0,1);
      _firstChunk = DKV.get(k1);
      if(_firstChunk == null) throw new Error("missing key " + k1);
      _secondChunk = (k2 == null)?null:DKV.get(k2);
      _currentChunkId = 0; // not really applicable here            
    }
    
    void close(){_firstChunk = null; _secondChunk = null;}

    @Override
    boolean hasMoreData() {
      long maxData = ((_firstChunk == null)?0:_firstChunk.length()) + ((_secondChunk == null)?0:_secondChunk.length());
      return _dataRead < maxData;      
    }
  } 
  
  
  class StreamDataProvider extends ADataProvider {
    
    byte [][] _data;        
    InputStream _is;    
    
    StreamDataProvider(int chunkSize, InputStream is){
      super(0,Integer.MAX_VALUE);
      _data = new byte[][]{new byte[chunkSize],new byte[chunkSize]};
      _is = is;                        
    }
    
    
    @Override
    byte[] nextData(int len) {     
      byte [] data = (len >= _data[_currentChunkId&1].length)?_data[_currentChunkId&1]:new byte[len];
      try {
        int read = _is.read(data);
        if(read < data.length){
           data = (read == -1)?null:Arrays.copyOf(data, read);                      
        }        
      } catch (IOException e) {
        e.printStackTrace();
        data = null;        
      }
      if(data != null)
        ++_currentChunkId;
      return data;
    }
    
    void close(){
      try {
        _is.close();        
      } catch (IOException e) {
        e.printStackTrace();
      }      
    }


    @Override
    boolean hasMoreData() {          
      try {
        return (_is != null) && (_is.available() > 0);
      } catch (IOException e) {
        e.printStackTrace();
        return false;
      }
    }
  }
  
  
  
  CSVParser _parser;
  boolean _next;
  boolean _fresh = false;
  ADataProvider _dataProvider;
  
  public boolean hasNext() {
    if(_next)
      return true;
    if(_dataProvider.chunkIdx() >= _dataProvider._maxChunkId)
      return false;      
    if(!_next)
      try {
        _next = _parser.next();
      } catch(CSVEscapedBoundaryException e){
        throw e;
      } catch (Exception e1) {
        e1.printStackTrace();
        _parser.close();
        return false;
      }

    if(!_next && _dataProvider.hasMoreData()){
      try {
        if(_dataProvider.lastChunk()){ // the boundary case
          _parser.addData(_dataProvider.nextData(1024));
          if(_next = _parser.next())return _next;         
        }
        if(_dataProvider.hasMoreData()){
          _parser.addData(_dataProvider.nextData(Integer.MAX_VALUE));      
          _next = _parser.next();
        }
      } catch (Exception e) {
        _next = false;
        e.printStackTrace();
      }
      if(!_next && (_parser._column > 0))
        throw new Error("unfinished record");      
    }
    return _next;
  }

  public T next() {
    _fresh = false;
    if(hasNext()){
      _next = false;
      return _rec;
    }
    throw new NoSuchElementException();
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  public ValueCSVRecords(Value v, int index, T csvRecord, String [] columns) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, CSVParseException, IOException {
    this(v, index, index+1, csvRecord, columns, new CSVParserSetup());
  }

  
  public ValueCSVRecords(InputStream is, T csvRecord, String [] columns, CSVParserSetup setup) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, CSVParseException, IOException {
    _dataProvider = new StreamDataProvider(1 << water.ValueArray.LOG_CHK, is);
    _rec = csvRecord;
    setup._parseColumnNames = setup._parseColumnNames;        
    _parser = new CSVParser(_dataProvider.nextData(Integer.MAX_VALUE), csvRecord, columns, setup);        
    iterator();        
  }
  
 // TODO just a temporary solution - needed for DAvg because DRemoteTask does not provide more than a single key in its map interface 
 public ValueCSVRecords(Key k1, Key k2, int index,  T csvRecord, String [] columns, CSVParserSetup setup) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, CSVParseException, IOException {
   _dataProvider = new Simple_KV_DataProvider(k1,k2);
   _rec = csvRecord;
   setup._parseColumnNames = setup._parseColumnNames;        
   _parser = new CSVParser(_dataProvider.nextData(Integer.MAX_VALUE), csvRecord, columns, setup);
   setup._parseColumnNames = setup._parseColumnNames && (index == 0);
   setup._skipFirstRecord = (index > 0);      
   iterator();
 }
 
 public ValueCSVRecords(Value v, int indexFrom, int indexTo,  T csvRecord, String [] columns, CSVParserSetup setup) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, CSVParseException, IOException {
   _dataProvider = new KV_DataProvider(v, indexFrom, indexTo);
   _rec = csvRecord;
   setup._parseColumnNames = setup._parseColumnNames;        
   _parser = new CSVParser(_dataProvider.nextData(Integer.MAX_VALUE), csvRecord, columns, setup);
   setup._parseColumnNames = setup._parseColumnNames && (indexFrom == 0);
   setup._skipFirstRecord = (indexFrom > 0);     
   iterator();
 }
 
  
  
  /**
   *
   * @param v     - Value containing the data to be parsed or arraylet root in case of arraylets
   * @param index - index of the arraylet chunk to be parsed in case of arraylet
   * @param csvRecord - instance of an object which will be filled by next() method.
   * @param columns - atrributes to be filled by next method, in the same order as the columns in the csv file.
   * @param setup - contains settings of CSVParser
   * @throws NoSuchFieldException - happens in case of mismatch between the passed object nad passed columns attribute.
   * @throws SecurityException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws CSVParseException
   * @throws IOException
   */
  public ValueCSVRecords(Value v, int index,  T csvRecord, String [] columns, CSVParserSetup setup) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, CSVParseException, IOException {
    this(v,index,index+1,csvRecord,columns,setup);
  }
//    byte [] data = null;
//    byte [] nextData = null;
//    if(v.chunks() > 1){
//      if(index > v.chunks())
//        throw new ArrayIndexOutOfBoundsException(index);
//      Key chunkKey = v.chunk_get(index);
//      Value chunk = DKV.get(chunkKey);
//      if(chunk == null)
//        throw new CSVParseException("trying to parse non existing data");
//      data = chunk.get();
//      if(index + 1 < v.chunks()){
//        chunk = DKV.get(v.chunk_get(index+1));
//        if(chunk != null){
//          _nextData = chunk;
//          nextData = chunk.get(1024);
//        }
//      }
//    } else {
//      data = v.get();
//    }
//    _rec = csvRecord;
//    setup._parseColumnNames = setup._parseColumnNames && (index == 0);
//    setup._skipFirstRecord = (index > 0);
//    _parser = new CSVParser(data, csvRecord, columns, setup);
//    _parser.addData(nextData);
//    iterator();
//  }
  /**
   * Getter for column names.
   * @return column names if parseColumn names set to true and we have the first chunk, null otherwise
   */
  public final String [] columnNames() {
    if(_parser != null)
      return _parser.columnNames();
    return null;
  }

  public Iterator<T> iterator() {
    if(!_fresh){      
      try {
        _parser.reset();
      } catch (Exception e) {
        // should not happen
        e.printStackTrace();
        throw new IllegalStateException();
      }
      _fresh = true;
    }
    return this;
  }
}
