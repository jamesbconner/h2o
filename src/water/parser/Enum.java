package water.parser;

import java.io.*;
import java.util.*;
import water.AutoBuffer;
import water.Iced;
import water.TCPReceiverThread;
import water.nbhm.NonBlockingHashMap;

/**
 * Class for tracking enum columns.
 *
 * Basically a wrapper around non blocking hash map.
 * In the first pass, we just collect set of unique strings per column
 * (if there are less than MAX_ENUM_SIZE unique elements).
 *
 * After pass1, the keys are sorted and indexed alphabetically.
 * In the second pass, map is used only for lookup and never updated.
 *
 * Enum objects are shared among threads on the local nodes!
 *
 * @author tomasnykodym
 *
 */
public final class Enum extends Iced {

  public static final int MAX_ENUM_SIZE = 65535;

  NonBlockingHashMap<ValueString, Integer> _map;

  public Enum(){
    _map = new NonBlockingHashMap<ValueString, Integer>();
  }

  /**
   * Add key to this map (treated as hash set in this case).
   * All keys are added with value = 1.
   * @param str
   */
  void addKey(ValueString str){
    // _map is shared and be cast to null (if enum is killed) -> grab local copy
    NonBlockingHashMap<ValueString, Integer> m = _map;
    if(m == null)return;
    if(m.get(str) == null){
      m.put(new ValueString(Arrays.copyOfRange(str._buf, str._off, str._off + str._length)), 1);
      if(m.size() > MAX_ENUM_SIZE)
        kill();
    }
  }

  public void addKey(String str) {
    addKey(new ValueString(str));
  }

  public int getTokenId(String str) {
    return getTokenId(new ValueString(str));
  }

  int getTokenId(ValueString str){
    assert _map.get(str) != null:"missing value! " + str.toString();
    return _map.get(str);
  }

  public void merge(Enum other){
    if(this != other) {
      if(isKilled() || other.isKilled()){
        kill(); // too many values, enum should be killed!
      } else { // do the merge
        NonBlockingHashMap<ValueString, Integer> myMap = _map;
        for(ValueString str:other._map.keySet()){
          myMap.put(str, 0);
        }
        if(myMap.size() > MAX_ENUM_SIZE)
          kill();
      }
    }
  }
  public int size() { return _map.size(); }
  public boolean isKilled() { return _map == null; }
  public void kill() { _map = null; }

  // assuming single threaded
  public String [] computeColumnDomain(){
    if( isKilled() ) return null;
    String [] res = new String[_map.size()];
    NonBlockingHashMap<ValueString, Integer> oldMap = _map;
    Iterator<ValueString> it = oldMap.keySet().iterator();
    for( int i = 0; i < res.length; ++i )
      res[i] = it.next().toString();
    Arrays.sort(res);
    NonBlockingHashMap<ValueString, Integer> newMap = new NonBlockingHashMap<ValueString, Integer>();
    for( int j = 0; j < res.length; ++j )
      newMap.put(new ValueString(res[j]), j);
    oldMap.clear();
    _map = newMap;
    return res;
  }

  public AutoBuffer write( AutoBuffer ab ) {
    if( _map == null ) return ab.put4(0);
    ab.put4(_map.size());
    for( Map.Entry<ValueString,Integer> e : _map.entrySet() )
      ab.putA1(e.getKey()._buf).put4(e.getValue());
    return ab;
  }

  public Enum read( AutoBuffer ab ) {
    assert _map == null || _map.size()==0;
    _map = new NonBlockingHashMap<ValueString, Integer>();
    int n = ab.get4();
    for( int i=0; i<n; i++ ) {
      byte[] buf = ab.getA1();
      _map.put(new ValueString(buf),ab.get4());
    }
    return this;
  }
}
