package water.csv;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import water.DKV;
import water.Key;
import water.Value;
import water.csv.CSVParser.CSVEscapedBoundaryException;
import water.csv.CSVParser.CSVParseException;
import water.csv.CSVParser.CSVParserSetup;


/**
 * Wrapper around CSVParser implementing iterator interface.  
 *  
 * Allows for iteration over csv records stored in a Value. if passed Value is an arraylet, its behavior depends on the index.
 * The chunk 0 is parsed completely + the first record of chunk 1. For all other chunks, first record is skipped and the first record from the next chunk is parsed as well. 
 * 
 * Note that escaped sequences (quoted) are not allowed to cross a chunk boundary. Such record will cause an exception.
 * 
 * Iterator interface returns this as an iterator, meaning all iterators returned by iterator() are shared and call to iterator() method
 * reset all previously returned iterators (which are all in fact the same object).
 * 
 * Objects returned by iterator's next method all all the same object as the one passed in as an csvRecord argument with the values of its attributes set by the method.
 * Therefore, every call to the next or hasNext() method overwrites previously returned data. 
 *  
 * @author tomas
 *
 */
public class ValueCSVRecords<T> implements Iterable<T>,Iterator<T> {	
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
			} catch(CSVEscapedBoundaryException e){
				throw e;
			} catch (Exception e1) {
				e1.printStackTrace();
				_parser.close();				
				return false;
			} 
			 
		if(!_next && (_nextData != null)){
			_done = true;										
			_parser.setNextData(_nextData.get());
			try {
				_next = _parser.next();
			} catch (Exception e) {
				_next = false;					
				e.printStackTrace();
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
	/**
	 * Getter for column names.
	 * @return column names if parseColumn names set to true and we have the first chunk, null otherwise
	 */
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
