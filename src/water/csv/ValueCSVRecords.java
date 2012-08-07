package water.csv;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import water.DKV;
import water.Key;
import water.Value;
import water.ValueArray;
import water.csv.CSVParser.CSVEscapedBoundaryException;
import water.csv.CSVParser.CSVParseException;
import water.csv.CSVParser.CSVParserSetup;
import water.csv.CSVParser.CSVParserSetup.PartialRecordPolicy;

/**
 * Wrapper around CSVParser implementing iterator interface.
 * 
 * Allows for iteration over csv records stored in a Value. if passed Value is
 * an arraylet, its behavior depends on the index. The chunk 0 is parsed
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
 * Objects returned by iterator's next method all all the same object as the one
 * passed in as an csvRecord argument with the values of its attributes set by
 * the method. Therefore, every call to the next or hasNext() method overwrites
 * previously returned data.
 * 
 * @author tomas
 * 
 */
public class ValueCSVRecords<T> implements Iterable<T>, Iterator<T> {
  T _rec;
  String[] _columns;

  public abstract static class ADataProvider {

    abstract boolean hasMoreData();

    public boolean lastChunk() {
      return false;
    }

    public ADataProvider() {
    }

    final byte[] nextData() {
      return nextData(Integer.MAX_VALUE);
    }

    abstract byte[] nextData(int len);

    abstract void reset();

    abstract void close();
  }

  final class SingleValue_DataProvider extends ADataProvider {
    final Value _val;
    int _dataRead;

    SingleValue_DataProvider(Key k) {
      _val = DKV.get(k);
    }

    @Override
    boolean hasMoreData() {
      return _val != null && _dataRead < _val.length();
    }

    @Override
    byte[] nextData(int len) {
      if (_val == null || _dataRead == _val.length())
        return null;
      byte[] res = _val.get(_dataRead + len);
      if (_dataRead > 0) {
        res = Arrays.copyOfRange(res, (int) _dataRead, res.length);
      }
      _dataRead += res.length;
      return res;
    }

    @Override
    void reset() {
      _dataRead = 0;
    }

    @Override
    void close() {
      if (_val != null)
        _dataRead = (int) _val.length();
    }
  }
  
  final class ByteArrayDataProvider extends ADataProvider {
    byte [] _data;
    boolean _read = false;;
    
    ByteArrayDataProvider(byte [] data) {
      _data = data;
    }

    @Override
    boolean hasMoreData() {
      return (_data != null) && !_read;
    }

    @Override
    byte[] nextData(int len) {
      byte [] res = _read?null:_data;
      _read = true;
      return res;
    }

    @Override
    void reset() {
      _read = false;
    }

    @Override
    void close() {
      _read = true;
    }
  }


  final class KV_DataProvider extends ADataProvider {
    Key _currentKey;
    Value _currentChunk;
    public final int minChunkId;
    public final int maxChunkId;
    int _currentChunkId;
    int _dataRead;

    private void getChunk(int idx) {
      _currentKey = ValueArray.getChunk(_currentKey, idx);
      _currentChunkId = idx;
      _currentChunk = DKV.get(_currentKey);
      _dataRead = 0;
    }

    private boolean nextChunk() {
      // only fetch up to the last chunk
      if (_currentChunkId >= maxChunkId)
        return false;
      getChunk(_currentChunkId + 1);
//      if(_currentChunk != null){
//        System.out.println("fetched chunk #" + _currentChunkId);
//      } else
//        System.out.println("stopped at chunk #" + (_currentChunkId-1));
      return (_currentChunk != null);
    }

    @Override
    byte[] nextData(int len) {
      if (_currentChunk == null)
        return null;
      if ((_dataRead == _currentChunk.length()) && !nextChunk()) {
        return null;
      }
      int N = _dataRead + len;
      byte[] res = _currentChunk.get(N);
      if (_dataRead > 0) {
        // inefficient but should not happen in 99.9% of cases
        // (should happen only when the remaining record fragment is longer than
        // 1024 bits into the next chunk)
        res = Arrays.copyOfRange(res, _dataRead, res.length);
      }
      _dataRead += res.length;
      return res;
    }

    KV_DataProvider(Key k, int nChunks) {
      minChunkId = ValueArray.getChunkIndex(k);
      // toChunkId should be smaller than Integer.MAX_VALUE so that the
      // provider can be closed.
      // (last record is read from the last chunk if it exists, so we need to
      // point one behind last when closing)
      maxChunkId = nChunks != (Integer.MAX_VALUE) ? minChunkId + nChunks
          : Integer.MAX_VALUE - 1;
      _currentChunkId = minChunkId;
      _currentKey = k;
      getChunk(_currentChunkId);
    }

    public boolean lastChunk() {
      return ((_currentChunkId == (maxChunkId - 1)) && (_dataRead == _currentChunk
          .length())) || (_currentChunkId == maxChunkId);
    }

    void close() {
      _currentChunk = null;
    }

    @Override
    boolean hasMoreData() {
      return ((_currentChunk != null) && (_dataRead < _currentChunk.length()))
          || nextChunk();
    }

    @Override
    void reset() {
      getChunk(minChunkId);
    }
  }

  public static class StreamDataProvider extends ADataProvider {

    byte[][] _data;
    InputStream _is;
    
    long _dataRead;
    long _dataLimit = Long.MAX_VALUE;
    int _currentChunkId;

    public StreamDataProvider(int chunkSize, InputStream is, long offset, long length) throws IOException {
      _data = new byte[][] { new byte[chunkSize], new byte[chunkSize] };
      _is = is;
      _is.skip(offset);
      _dataLimit = length;
    }
    public StreamDataProvider(int chunkSize, InputStream is) {
      _data = new byte[][] { new byte[chunkSize], new byte[chunkSize] };
      _is = is;
    }

    @Override
    byte[] nextData(int len) {      
      long n = (_dataLimit - _dataRead);      
      len = Math.min(len,(n > Integer.MAX_VALUE)?Integer.MAX_VALUE:(int)n);
      if(len == 0) return null;      
      byte[] data = (len >= _data[_currentChunkId & 1].length) ? _data[_currentChunkId & 1]
          : new byte[len];
      try {
        int read = _is.read(data);
        _dataRead += read;
        if (read < data.length) {
          data = (read == -1) ? null : Arrays.copyOf(data, read);
        }
      } catch (IOException e) {
        e.printStackTrace();
        data = null;
      }
      if (data != null)
        ++_currentChunkId;
      return data;
    }

    void close() {
      try {
        _is.close();
        _is = null;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    boolean hasMoreData() {
      try {
        return (_is != null) && (_is.available() > 0) && (_dataRead != _dataLimit);
      } catch (IOException e) {
        e.printStackTrace();
        return false;
      }
    }

    @Override
    void reset() {
      throw new UnsupportedOperationException();
    }
  }

  class FileDataProvider extends StreamDataProvider {
    File _f;

    public FileDataProvider(int chunksize, File f) throws FileNotFoundException {
      super(chunksize, new FileInputStream(f));
      _f = f;
    }

    void reset() {
      if (_is != null)
        try {
          _is.close();
          _is = new FileInputStream(_f);
        } catch (IOException e) {
          e.printStackTrace();
          throw new Error(e);
        }
    }
  }

  CSVParser _parser;
  boolean _next;
  ADataProvider _dataProvider;
  boolean _fresh = true;
  
  public boolean hasNext() {
    if (_next)
      return true;
    if(_parser == null)
      return false;
    try {
      _next = _parser.next();
    } catch (CSVEscapedBoundaryException e) {
      throw e;
    } catch (Exception e1) {
      _parser.close();
      return false;
    }

    if (!_next && _dataProvider.hasMoreData()) {
      try {
        if (_dataProvider.lastChunk()) { // the boundary case - make sure we use
                                         // only up to the first record of data
          _parser.addData(_dataProvider.nextData(1024)); // first try with small
                                                         // piece
          if( (_next = _parser.next())==true ) {
            _dataProvider.close(); // we hit the end of a record
            _parser.close();
            return true;
          }
          // the record did not end in the first part of chunk, try to read the
          // whole chunk and try again
          _parser.addData(_dataProvider.nextData(Integer.MAX_VALUE));
          _next = _parser.next();
          // whether we succeeded or not, this was the last part of data to be
          // processed by this provider (records are required not to cross chunk
          // boundaries
          _dataProvider.close();
          _parser.close();
        } else if (_dataProvider.hasMoreData()) {
          _parser.addData(_dataProvider.nextData(Integer.MAX_VALUE));
          _next = _parser.next();
        }
        if (!_next && (_parser._column > 0)){
          // we must have reached a record end by now, otherwise the record is
          // either unfinished or it is longer than one the size of one chunk
          // which is illegal as well
         _parser.addData("\n".getBytes());       
         _next = _parser.next();
         _parser.close();
       }
      } catch (Exception e) {
        _next = false;
        throw new Error(e);
      }            
    }
    if(!_next && _parser._column > 0){
      try {
        return (_next = _parser.endRecord());
      } catch (Exception e) {
        throw new Error(e);
      } 
    }      
    return _next;
  }

  public T next() {
    if (hasNext()) {
      _next = false;
      return _rec;
    }
    throw new NoSuchElementException();
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  public ValueCSVRecords(Key key, int nChunks, T csvRecord, String[] columns)
      throws NoSuchFieldException, SecurityException, IllegalArgumentException,
      IllegalAccessException, CSVParseException, IOException {
    this(key, nChunks, csvRecord, columns, new CSVParserSetup());
  }

  public ValueCSVRecords(InputStream is, T csvRecord, String[] columns,
      CSVParserSetup setup) throws NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, CSVParseException,
      IOException {
    _dataProvider = new StreamDataProvider(1 << water.ValueArray.LOG_CHK, is);
    _rec = csvRecord;
    _parser = new CSVParser(_dataProvider.nextData(), csvRecord, columns, setup);
  }
  
  public ValueCSVRecords(ADataProvider dataProvider, T csvRecord, String[] columns,
      CSVParserSetup setup) throws NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, CSVParseException,
      IOException {
    _dataProvider = dataProvider;
    _rec = csvRecord;
    _parser = new CSVParser(_dataProvider.nextData(), csvRecord, columns, setup);
  }

  public ValueCSVRecords(Key k, int nChunks, T csvRecord, String[] columns,
      CSVParserSetup setup) throws NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, CSVParseException,
      IOException {
    _rec = csvRecord;
    Value v = DKV.get(k);
    if( v instanceof ValueArray )
      k = ((ValueArray)v).make_chunkkey(0); // Move from array to 1st chunk in array

    if( k._kb[0] == Key.ARRAYLET_CHUNK ) { // Arraylet?
      int index = ValueArray.getChunkIndex(k);
      _dataProvider = new KV_DataProvider(k, nChunks);
      setup._parseColumnNames = setup._parseColumnNames && (index == 0);
      setup._skipFirstRecord = (index > 0);
      if(_dataProvider.hasMoreData())
        _parser = new CSVParser(_dataProvider.nextData(), csvRecord, columns, setup);
    } else {
      assert nChunks == 1;
      int index = 0;
      _dataProvider = new ByteArrayDataProvider(v.get());
      setup._parseColumnNames = setup._parseColumnNames && (index == 0);
      setup._skipFirstRecord = (index > 0);
      _parser = new CSVParser(_dataProvider.nextData(), csvRecord, columns, setup);
    }
  }

  /**
   * 
   * @param v
   *          - Value containing the data to be parsed or arraylet root in case
   *          of arraylets
   * @param index
   *          - index of the arraylet chunk to be parsed in case of arraylet
   * @param csvRecord
   *          - instance of an object which will be filled by next() method.
   * @param columns
   *          - atrributes to be filled by next method, in the same order as the
   *          columns in the csv file.
   * @param setup
   *          - contains settings of CSVParser
   * @throws NoSuchFieldException
   *           - happens in case of mismatch between the passed object nad
   *           passed columns attribute.
   * @throws SecurityException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws CSVParseException
   * @throws IOException
   */
  // public ValueCSVRecords(Value v, int index, T csvRecord, String[] columns,
  // CSVParserSetup setup) throws NoSuchFieldException, SecurityException,
  // IllegalArgumentException, IllegalAccessException, CSVParseException,
  // IOException {
  // this(v, index, index + 1, csvRecord, columns, setup);
  // }

  // byte [] data = null;
  // byte [] nextData = null;
  // if(v.chunks() > 1){
  // if(index > v.chunks())
  // throw new ArrayIndexOutOfBoundsException(index);
  // Key chunkKey = v.chunk_get(index);
  // Value chunk = DKV.get(chunkKey);
  // if(chunk == null)
  // throw new CSVParseException("trying to parse non existing data");
  // data = chunk.get();
  // if(index + 1 < v.chunks()){
  // chunk = DKV.get(v.chunk_get(index+1));
  // if(chunk != null){
  // _nextData = chunk;
  // nextData = chunk.get(1024);
  // }
  // }
  // } else {
  // data = v.get();
  // }
  // _rec = csvRecord;
  // setup._parseColumnNames = setup._parseColumnNames && (index == 0);
  // setup._skipFirstRecord = (index > 0);
  // _parser = new CSVParser(data, csvRecord, columns, setup);
  // _parser.addData(nextData);
  // iterator();
  // }
  /**
   * Getter for column names.
   * 
   * @return column names if parseColumn names set to true and we have the first
   *         chunk, null otherwise
   */
  public final String[] columnNames() {
    if (_parser != null)
      return _parser.columnNames();
    return null;
  }

  public Iterator<T> iterator() {
    if(!_fresh){
      try {
        _dataProvider.reset();
        _parser.reset(_dataProvider.nextData());        
        _fresh = true;
      } catch (Exception e) {
        // should not happen
        e.printStackTrace();
        throw new Error(e);
      }      
    }
    return this;
  }
}
