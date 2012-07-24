package water.csv;

public class CSVString {
	int _offset = -1;
	int _len = -1;		
	CSVParser _parser;
	
	public CSVString(int off, int len, CSVParser p) {
		_offset = off;
		_len = len;
		_parser = p;		
	} 
	
	public final byte byteAt(int i){
		if((i < 0) || (i >= _len))
			throw new ArrayIndexOutOfBoundsException(i);
		return ((i + _offset) < _parser._data.length)?_parser._data[_offset + i]:_parser._nextData[_offset + i - _parser._data.length];
	}
	
	public final char charAt(int i){
		return (char)byteAt(i);
	}
	
	public int compare(byte [] arr){
		int N = Math.min(_len,arr.length);
		if((_offset + _len) <= _parser._data.length){ // no boundary crossing									
			for(int i = 0; i < N; ++i){
				int res = _parser._data[i+_offset] - arr[i];
				if(res != 0)return res;
			}						
		} else{
			// deal with the case of crossing the boundary			
			for(int i = 0; i < N; ++i){
				int res = (((i + _offset) < _parser._data.length)?_parser._data[i+_offset]:_parser._nextData[i+_offset-_parser._data.length]) - arr[i]; 
				if(res != 0) return res;
			}
		}
		return (_len - arr.length);		
	}
	
	public int compare(String s){
		return compare(s.getBytes());
	}
	
	public boolean equals(String s){
		return (s.length() == _len)?(compare(s) == 0):false;		
	}				
	
	@Override
	public String toString(){
		if(_len <= 0)
			return "";
		if((_offset + _len) <= _parser._data.length)
			return new String(_parser._data,_offset,_len);
		else if(_offset >= _parser._data.length){
			return new String(_parser._nextData,_offset - _parser._data.length, _len);
		}
		int len1 = _parser._data.length - _offset;
		int len2 = _len - len1;
		return new String(_parser._data,_offset,len1) + new String(_parser._nextData,0,len2);			
	}	
}
