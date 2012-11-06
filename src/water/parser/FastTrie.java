package water.parser;

import java.io.*;
import java.util.*;

import water.Stream;

/**
 * Trie for parsing enums in the FastParser.
 *
 *
 * @author tomasnykodym
 *
 */
public final class FastTrie {
  short _state;
  State [] _states = new State[256];
  BitSet _finaStates = new BitSet(256);
  short _nstates = 1;
  boolean _compressed;
  boolean _killed;
  int max_tokens = 1024;
  short _state0 = (short)0;


  public FastTrie(){
    _states[0] = new State();
  }
  final private short addState(State s){
    if(_nstates == _states.length) {
      _states = Arrays.copyOf(_states, _states.length + (_states.length >> 1));
      BitSet newFinalStates = new BitSet(_states.length);
      newFinalStates.or(_finaStates);
      _finaStates = newFinalStates;
    }
    _states[_nstates] = s;
    return _nstates++;
  }

  public void kill(){
    _killed = false;
  }

  final class State {
//    byte [] _alpha;// = new byte[8];
//    short [] _succ;// = new short[8];
    short _skip;

    short _transitions[][];

    State(){}



    int wire_len(){
      return 10 ;//+ ((_alpha != null)?3*_alpha.length:0);
    }


    void write(Stream s){
      s.set2(_skip);
//      s.setAry1(_alpha);
//      s.setAry2(_succ);
    }

    void read(Stream s){
      _skip = (short)s.get2();
//      _alpha = s.getAry1();
//      _succ = s.getAry2();
    }

    public void write(DataOutputStream os) throws IOException {
      os.writeShort(_skip);
//      if(_alpha != null){
//        os.writeInt(_alpha.length);
//        os.write(_alpha);
//        os.writeInt(_succ.length);
//        for(short s:_succ)os.writeShort(s);
//      } else {
//        os.writeInt(-1);
//        os.writeInt(-1);
//      }
    }

    public void read ( DataInputStream is) throws IOException {
      _skip = is.readShort();
      int n = is.readInt();
      if(n == -1)
        is.readInt();
      else {
//        _alpha = new byte[n];
//        is.readFully(_alpha);
//        int m = is.readInt();
//        assert n == m;
//        _succ = new short[n];
//        for(int i = 0; i < n; ++i)_succ[i] = is.readShort();
      }
    }

    void merge(FastTrie otherTrie, short sIdx){
      State other = otherTrie._states[sIdx];
      if(other._transitions == null)return;
      for(int i = 0; i < 16; ++i){
        if(other._transitions[i] == null)continue;
        for(int j = 0; j < 16; ++j){
          if(other._transitions[i][j] == 0)continue;
          _states[getTransition((byte)((i << 4) + j))].merge(otherTrie, other._transitions[i][j]);
        }
      }
    }

    short compress(ArrayList<State> states, String [] strings, StringBuilder currentString, short skip, boolean finalState){
      int nsucc = 0;
      int x = 0,y = 0;
      if(_transitions != null){
        for(int i = 0; i < 16; ++i){
          if(_transitions[i] == null)continue;
          for(int j = 0; j < 16; ++j)
            if(_transitions[i][j] != 0){
              ++nsucc;
              x = i; y = j;
            }
        }
      }
      if(nsucc != 1 || finalState || states.size() <= strings.length){
        State s = new State();
        short res = (short)states.size();
        if(finalState){
          for(res = 0; res < states.size(); ++res)if(states.get(res) == null)break;
          states.set(res, s);
          strings[res] = currentString.toString();
        } else states.add(s);
        s._skip = skip;
        if(nsucc > 0){
          s._transitions = new short[16][];
          for(int i = 0; i < 16; ++i) {
            if(_transitions[i] != null){
              s._transitions[i] = new short[16];
              Arrays.fill(s._transitions[i],(short)strings.length); // fill with the new state 0
              for(int j = 0; j < 16; ++j){
                short nextS = _transitions[i][j];
                if(nextS != 0){
                  currentString.append((char)((i << 4) + j));
                  s._transitions[i][j] = _states[nextS].compress(states, strings, currentString, (short)0, _finaStates.get(nextS));
                  currentString.setLength(currentString.length()-1);
                }
              }
            }
          }
        }
        return res;
      } else {
        short nextS = _transitions[x][y];
        currentString.append((char)((x << 4) + y));
        short res =  _states[nextS].compress(states, strings, currentString, ++skip, _finaStates.get(nextS));
        currentString.setLength(currentString.length()-1);
        return res;
      }
    }

    final short getTransition(byte c){
      int idx = c >> 4;
      c &= 0x0F;
      // todo CAS everything so that it can be shared
      if(_transitions == null)_transitions = new short[16][];
      if(_transitions[idx] == null)_transitions[idx] = new short[16];
      if(_transitions[idx][c] == _state0){
        assert !_compressed:"missing transition";
        _transitions[idx][c] = addState(new State());
      }
      return _transitions[idx][c];
    }
  }

  @Override
  public String toString() {
    if(_killed)return "FastTrie(killed)";
    LinkedList<Short> openedNodes = new LinkedList<Short>();
    openedNodes.add(_state0);
    StringBuilder sb = new StringBuilder();
    while(!openedNodes.isEmpty()){
      short stidx = openedNodes.pollFirst();
      State st = _states[stidx];
      boolean finalState = _compressed?(stidx < _state0):_finaStates.get(stidx);
      sb.append("state: " + stidx + (finalState?"*":"") + " skip:" + st._skip + " transitions: ");
      if(st._transitions != null){
        for(int i = 0; i < 16; ++i) {
          if(st._transitions[i] == null)continue;
          for(int j = 0; j < 16; ++j){
            short s = st._transitions[i][j];
            if(s == _state0)continue;
            openedNodes.push(s);
            sb.append((char)((i << 4) + j) + ":" + s + " ");
          }
        }
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  public short addByte(byte b){
    if(_killed)return 0;
    assert  0 <= b && b < 128;
    _state = _states[_state].getTransition(b);
    return _states[_state]._skip;
  }

  String [] compress(){
    if(_killed) return null;
    int  nfinalStates = _finaStates.cardinality();
    ArrayList<State> newStates = new ArrayList<State>();
    // add nulls for final states to make sure we can add all final states in the beginningof the array
    for(int i = 0; i < nfinalStates; ++i)newStates.add(null);
    // put final states in the beginning...
    String [] strings = new String[nfinalStates];
    _states[0].compress(newStates, strings, new StringBuilder(),(short)0,false);
    _states = new State[newStates.size()];
    _states = newStates.toArray(_states);
    _compressed = true;
    _state0 = (short)nfinalStates;
    _state = _state0;
    _nstates = (short)_states.length;
    _finaStates = null;
    return strings;
  }

  /**
   * get token id of the currently parsed String and reset the Trie to start new word.
   */
  public int getTokenId(){
    if(_killed)return -1;
    assert !_compressed || _state < _state0;
    int res =  _state;
    if(!_compressed)_finaStates.set(_state);
    _state = _state0;
    return res;
  }

  public void merge(FastTrie other){
    if(_killed || other._killed){
      _killed = true;
      _states = null;
      return;
    }
    if(_nstates == 0){
      _states = other._states;
      _nstates = other._nstates;
    }
    _states[0].merge(other, (short)0);
  }

  public int wire_len(){
    if(_killed)return 1;
    int res = 5;
    for(int i = 0; i < _nstates; ++i){
      res += _states[i].wire_len();
    }
    return res;
  }

  public void write(DataOutputStream os) throws IOException {
    os.writeBoolean(_killed);
    if(!_killed){
      os.writeBoolean(_compressed);
      os.writeShort(_nstates);
      for(int i =0; i < _nstates; ++i)_states[i].write(os);
    }
  }

  public void write(Stream s) {
    s.setz(_killed);
    if(!_killed){
      s.setz(_compressed);
      s.set2(_nstates);

      for(int i =0; i < _nstates; ++i)_states[i].write(s);
    }
  }

  public void read(Stream s) {
    _killed = s.getz();
    if(!_killed){
      _compressed = s.get1() == 1;
      _nstates = (short)s.get2();
      _states= new State[_nstates];
      for(int i = 0; i < _states.length; ++i){
        _states[i] = new State();
        _states[i].read(s);
      }
    }
  }

  public void read (DataInputStream is) throws IOException {
    _killed = is.readBoolean();
    if(!_killed){
      _compressed = is.readBoolean();
      _nstates = is.readShort();

      for(int i = 0; i < _states.length; ++i){
        _states[i] = new State();
        _states[i].read(is);
      }
    }
  }

  public static int [] addWords (String [] words, FastTrie t){
    int [] res = new int[words.length];
    int i = 0;
    for(String w:words){
      int j = 0;
      byte [] bs = w.getBytes();
      while(j < bs.length){
        System.out.println((char)bs[j]);
        j += t.addByte(bs[j])+1;
      }
      res[i++] = t.getTokenId();
    }
    return res;
  }

  static String [] data = new String[] {"J","G","B","B","D","D","I","I","F","F","I","I","I","I","I","H","I","I","I","I","C","A","A","J","J","I","I"};
  public static void main(String [] args){
    FastTrie t = new FastTrie();
    int [] res = addWords(data, t);
    System.out.println(Arrays.toString(res));
    int [] res2 = addWords(data, t);
    System.out.println(t);
    System.out.println(Arrays.toString(res2));
    String [] vals = t.compress();
    System.out.println(Arrays.toString(vals));
    System.out.println(t);
    int [] res3 = addWords(data, t);
    System.out.println(Arrays.toString(res3));
    System.out.println(t);

//    FastTrie t = new FastTrie();
//    String [] words = new String[]{"haha","gaga","hahagaga", "hahaha","gagaga","abcdefghijklmnopqrstuvwvxyz"};
//    int [] res1 = addWords(words, t);
//    int [] res2 = addWords(words, t);
//    System.out.println("res1 = " + Arrays.toString(res1));
//    System.out.println("res2 = " + Arrays.toString(res2));
//    System.out.println("Trie: ");
//    System.out.println(t.toString());
//
//    FastTrie t2 = new FastTrie();
//    String [] words2 = new String[]{"haha","abc", "gogo","gaga","hahagaga", "hahaha","gagaga","abcdefghijklmnopqrstuvwvxyz"};
//    int [] res3 = addWords(words2, t2);
//    int [] res4 = addWords(words2, t2);
//    System.out.println("res3 = " + Arrays.toString(res3));
//    System.out.println("res4 = " + Arrays.toString(res4));
//    t.merge(t2);
//    res2 = addWords(words, t);
//    System.out.println("res5 = " + Arrays.toString(res2));
//    System.out.println(t);
//    res2 = addWords(words2, t);
//    System.out.println("res6 = " + Arrays.toString(res2));
//    System.out.println(t);
//    String[] strings = t.compress();
//    System.out.println("###################################################################");
//    System.out.println(Arrays.toString(strings));
//    System.out.println("===================================================================");
//    System.out.println(t);
//    res2 = addWords(words2, t);
//    System.out.println("res7 = " + Arrays.toString(res2));
//    System.out.println(t);
  }
}
