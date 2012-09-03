package water.parser;


public class CSVString implements CharSequence, Comparable<String> {
  int _offset = -1;
  int _len = -1;
  CSVParser _parser;

  public CSVString(int off, int len, CSVParser p) {
    _offset = off;
    _len = len;
    _parser = p;
  }
  
  public CSVString(CSVString other){
    this(other._offset,other._len,other._parser);    
  }
  
  public final byte byteAt(int i){   
    return _parser.getByte(i);
  }

  public final char charAt(int i){
    return (char)byteAt(i);
  }

  public int compare(byte [] arr){
    int N = Math.min(_len,arr.length);
    
    if((_offset + _len) <= _parser.data().length){ // no boundary crossing
      byte [] data = _parser.data(); 
      for(int i = 0; i < N; ++i){
        int res = data[i+_offset] - arr[i];
        if(res != 0)return res;
      }
    } else{
      // deal with the case of crossing the boundary
      for(int i = 0; i < N; ++i){
        int res = _parser.getByte(i+_offset) - arr[i];
        if(res != 0) return res;
      }
    }
    return (_len - arr.length);
  }

  
  public boolean equals(CSVString s){
    return (s._len == _len) && equals(s.toString());
  }
  public boolean equals(String s){
    return (s.length() == _len)?(compareTo(s) == 0):false;
  }

  @Override
  public String toString(){
    if(_len <= 0)
      return "";
    if((_offset + _len) <= _parser.data().length)
      return new String(_parser.data(),_offset,_len);
    else if(_offset >= _parser.data().length){
      return new String(_parser.nextData(),_offset - _parser.data().length, _len);
    }
    int len1 = _parser.data().length - _offset;
    int len2 = _len - len1;
    return new String(_parser.data(),_offset,len1) + new String(_parser.nextData(),0,len2);
  }

  public int compareTo(String arg0) {
    return compare(arg0.getBytes());
  }

  public int length() {
    return _len;
  }

  public CharSequence subSequence(int off, int end) {
    return new CSVString(off, end - off, _parser);
  }
  
  int _hash;
  
  // taken from String.java
  @Override
  public int hashCode(){
    int h = _hash;
    if (h == 0 && _len > 0) {
         int off = _offset;
        for (int i = 0; i < _len; i++) {
            h = 31*h + _parser.getByte(off++);
        }
        _hash = h;
    }
    return h;    
  }  
}
