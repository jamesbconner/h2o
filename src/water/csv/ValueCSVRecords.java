package water.csv;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import water.DKV;
import water.Key;
import water.Value;
import water.csv.CSVParser.CSVParseException;
import water.csv.CSVParser.CSVParserSetup;


/**
 * Wrapper around CSVParser. Takes two keys of two consecutive arraylets. 
 * It will parse all the records from the first one (starting from the second record, unless this is the first arraylet) 
 * @author tomas
 *
 */
public class ValueCSVRecords<T> implements Iterable<T>,Iterator<T> {	
	Key _k;
	Key _nextK;	
	Value _nextData;
	T _rec;
	String [] _columns;
		
	CSVParser _parser;
	boolean _next;
	boolean _done = false;
	boolean _fresh = false;
	
	@Override
	public boolean hasNext() {
		if(_next)
			return true;
		if(_done)
			return false;
		if(!_next)
			try {
				_next = _parser.next();				
			} catch (Exception e) {
				e.printStackTrace();	
				_next = false;
			} 
		if(!_next && (_nextK != null)){
			_done = true;				
			if(_nextData != null){
				if(_parser._state == 2)
					throw new IllegalStateException("There is an escaped (quoted) sequence crossing chunk boundary, handling of these cases is not currently implemented.");
				_parser.setNextData(_nextData.get());
				try {
					_next = _parser.next();
				} catch (Exception e) {
					_next = false;					
					e.printStackTrace();
				}
			}							
			_parser.close();
		}		
		return _next;
	}

	@Override
	public T next() {
		_fresh = false;
		if(hasNext()){
			_next = false;
			return _rec;
		}
		throw new NoSuchElementException();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();			
	}
	
	public ValueCSVRecords(Value v, int index, T csvRecord, String [] columns) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, CSVParseException, IOException {
		this(v, index, csvRecord, columns, new CSVParserSetup());
	}
	public CSVParser parser() {
		return _parser;
	}	
	public ValueCSVRecords(Value v, int index,  T csvRecord, String [] columns, CSVParserSetup setup) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, CSVParseException, IOException {		
		byte [] data = null;
		byte [] nextData = null;
		if(v.chunks() > 1){
			if(index > v.chunks())
				throw new ArrayIndexOutOfBoundsException(index);
			Key chunkKey = v.chunk_get(index);
			Value chunk = DKV.get(chunkKey);
			if(chunk == null)
				throw new CSVParseException("trying to parse non existing data");
			data = chunk.get();
			if(index + 1 < v.chunks()){
				chunk = DKV.get(v.chunk_get(index+1));
				if(chunk != null){
					_nextData = chunk;
					nextData = chunk.get(1024);
				}
			}
		} else {
			data = v.get();
		}
		_rec = csvRecord;					
		setup.parseColumnNames = setup.parseColumnNames && (index == 0);
		setup.skipFirstRecord = (index > 0);		
		_parser = new CSVParser(data, csvRecord, columns, setup);
		_parser.setNextData(nextData);
		iterator();
	}			
	
	public final String [] columnNames() {
		if(_parser != null)
			return _parser.columnNames();
		return null;
	}
	
	@Override
	public Iterator<T> iterator() {
		if(!_fresh){				
			_done = false;
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
