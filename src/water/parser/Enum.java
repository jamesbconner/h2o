
package water.parser;

import init.H2OSerializable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import water.Stream;
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
public final class Enum implements H2OSerializable {

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

  int getTokenId(ValueString str){
    if (_map.get(str) == null) 
      System.out.println(Arrays.toString(_map.keySet().toArray()));
    assert _map.get(str) != null:"missing value! " + str.toString();
    return _map.get(str);
  }

  public void merge(Enum other){
    if(this != other) {
      if(isKilled() || other.isKilled()){
        kill(); // too many values, enum shoudl be killed!
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
  public int size() {return _map.size();}
  public boolean isKilled() {return _map == null;}
  public void kill(){_map = null;}

  // assuming single threaded
  public String [] computeColumnDomain(){
    if(isKilled())return null;
    String [] res = new String[_map.size()];
    NonBlockingHashMap<ValueString, Integer> oldMap = _map;
    if(oldMap == null)return null;
    Iterator<ValueString> it = oldMap.keySet().iterator();
    for(int i = 0; i < res.length; ++i)
      res[i] = it.next().toString();
    Arrays.sort(res);
    NonBlockingHashMap<ValueString, Integer> newMap = new NonBlockingHashMap<ValueString, Integer>();
    for(int j = 0; j < res.length; ++j)
      newMap.put(new ValueString(res[j]), j);
    oldMap.clear();
    _map = newMap;
    return res;
  }
// assuming single threaded
  int wire_len(){
    // 4 bytes for map size + size* (4 bytes per ValueString._buf.length of the key + 4 bytes of int value)
    int res = 4 + 8*_map.size();
    // compute sum of keylens
    for(ValueString str:_map.keySet())res += str._length;
    return res;
  }
// assuming single threaded
  public void write(DataOutputStream dos) throws IOException{
    dos.writeInt(_map.size());
    Object [] kvs = _map.kvs();
    for(int i = 2; i < kvs.length; i+= 2){
      if(kvs[i] == null)continue;
      assert kvs[i] instanceof ValueString:"invalid key inside enum: "+kvs[i].toString();
      assert kvs[i+1] instanceof Integer:"invalid value inside enum: "+kvs[i+1].toString();
      ValueString k = (ValueString)kvs[i];
      assert k._off == 0:"invalid ValueString offset in enum: " + k._off;
      Integer v = (Integer)kvs[i+1];
      TCPReceiverThread.writeAry(dos,k._buf);
      dos.writeInt(v);
    }
  }
// assuming single threaded
  public void read(DataInputStream dis) throws IOException{
    int n = dis.readInt();
    _map = new NonBlockingHashMap<ValueString, Integer>();
    for(int i = 0; i < n; ++i){
      ValueString k = new ValueString(TCPReceiverThread.readByteAry(dis));
      Integer v = dis.readInt();
      _map.put(k, v);
    }
  }
// assuming single threaded
  public void write(Stream s) throws IOException{
    s.set4(_map.size());
    Object [] kvs = _map.kvs();
    for(int i = 2; i < kvs.length; i+= 2){
      if(kvs[i] == null)continue;
      assert kvs[i] instanceof ValueString:"invalid key inside enum: "+kvs[i].toString();
      assert kvs[i+1] instanceof Integer:"invalid value inside enum: "+kvs[i+1].toString();
      s.setAry1(((ValueString)kvs[i])._buf);
      s.set4((Integer)kvs[i+1]);
    }
  }
// assuming single threaded
  public void read(Stream s) throws IOException{
    int n = s.get4();
    _map = new NonBlockingHashMap<ValueString, Integer>();
    for(int i = 0; i < n; ++i){
      ValueString k = new ValueString(s.getAry1());
      Integer v = s.get4();
      _map.put(k, v);
    }
  }
}
