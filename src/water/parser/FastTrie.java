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
  State [] _states = new State[1024];
  short [] _finalStates = new short[256];
  short _nstates = 1;
  boolean _compressed;
  boolean _killed;
  int max_tokens = 1024;
  short _nfinalStates = (short)0;
  short _state0 = (short)0;


  public FastTrie(){
    _states[0] = new State(8);
  }
  final private short addState(State s){
    if(_nstates == _states.length)_states = Arrays.copyOf(_states, _states.length + (_states.length >> 1));
    _states[_nstates] = s;
    return _nstates++;
  }

  public void kill(){
    _killed = false;
  }

  final class State {
    byte [] _alpha;// = new byte[8];
    short [] _succ;// = new short[8];
    short _skip;

    State(){}
    State(int i){
      _succ = new short[8];
      _alpha = new byte[8];
    }

    int n(){
      if(_compressed)return (_alpha == null)?0:_alpha.length;
      int n = _alpha.length;
      for(int i = 0; i < _succ.length && _succ[i] == _state0; ++i)--n;
      return n;
    }

    int wire_len(){
      return 10 + ((_alpha != null)?3*_alpha.length:0);
    }


    void write(Stream s){
      s.set2(_skip);
      s.setAry1(_alpha);
      s.setAry2(_succ);
    }

    void read(Stream s){
      _skip = (short)s.get2();
      _alpha = s.getAry1();
      _succ = s.getAry2();
    }

    public void write(DataOutputStream os) throws IOException {
      os.writeShort(_skip);
      if(_alpha != null){
        os.writeInt(_alpha.length);
        os.write(_alpha);
        os.writeInt(_succ.length);
        for(short s:_succ)os.writeShort(s);
      } else {
        os.writeInt(-1);
        os.writeInt(-1);
      }
    }

    public void read ( DataInputStream is) throws IOException {
      _skip = is.readShort();
      int n = is.readInt();
      if(n == -1)
        is.readInt();
      else {
        _alpha = new byte[n];
        is.readFully(_alpha);
        int m = is.readInt();
        assert n == m;
        _succ = new short[n];
        for(int i = 0; i < n; ++i)_succ[i] = is.readShort();
      }
    }

    void merge(FastTrie otherTrie, short sIdx){
      State other = otherTrie._states[sIdx];
      for(int i = 0; i < other._alpha.length; ++i){
        if(other._succ[i] == 0)continue;
        _states[getTransition(other._alpha[i])].merge(otherTrie, other._succ[i]);
      }
    }

    short compress(ArrayList<State> states, String [] strings, StringBuilder currentString, short skip, boolean finalState){
      int firstSucc = 0;
      for(firstSucc = 0; firstSucc < _succ.length; ++firstSucc)if(_succ[firstSucc] != 0)break;
      int nsucc = _succ.length-firstSucc;

      if(nsucc != 1 || finalState){
        State s = new State();
        short res = (short)states.size();
        if(finalState){
          for(res = 0; res < states.size(); ++res)if(states.get(res) == null)break;
          states.set(res, s);
          strings[res] = currentString.toString();
        } else states.add(s);

        s._skip = skip;

        if(nsucc > 0){
          s._succ = new short[nsucc];
          s._alpha = Arrays.copyOfRange(_alpha,firstSucc,firstSucc+nsucc);
          for(int i = 0; i < nsucc; ++i){
            if(_succ[i+firstSucc] == 0)continue;
            short nextS = _succ[i+firstSucc];
            currentString.append((char)_alpha[i+firstSucc]);
            s._succ[i] = _states[nextS].compress(states, strings, currentString, (short)0, Arrays.binarySearch(_finalStates, 0, _nfinalStates, nextS) >= 0);
            currentString.setLength(currentString.length()-1);
          }
        }
        return res;
      }
      short nextS = _succ[firstSucc];
      currentString.append((char)_alpha[firstSucc]);
      short res =  _states[_succ[firstSucc]].compress(states, strings, currentString, ++skip, Arrays.binarySearch(_finalStates,0,_nfinalStates, nextS) >= 0);
      currentString.setLength(currentString.length()-1);
      return res;
    }

    final short getTransition(byte c){
      int firstSucc = 0;
      for(;firstSucc < _succ.length && _succ[firstSucc] == _state0; ++firstSucc);
      int idx = Arrays.binarySearch(_alpha, firstSucc, _succ.length, c);
      if(idx >= 0)return _succ[idx];
      if(_compressed)throw new Error("missing transition in compressed trie!");
      // we need a new state
      short s = addState(new State(8));
      idx = -idx - 2;
      if(_succ[0] == 0){ // array is not full
        if(idx == _succ.length)--idx;
        for(int i = firstSucc; i <= idx; ++i){
          _alpha[i-1] = _alpha[i];
          _succ[i-1] = _succ[i];
        }
        _alpha[idx] = c;
        _succ[idx] = s;
      } else {
        int N = _alpha.length + (_alpha.length >> 1);
        byte [] newTransitions = new byte[N];
        short [] newStates = new short[N];
        System.arraycopy(_succ, 0, newStates, 0,idx);
        System.arraycopy(_alpha, 0, _alpha, 0,idx);
        newStates[idx] = s;
        newTransitions[idx] = c;
        System.arraycopy(_succ, idx, newStates, idx+1,_succ.length-idx);
        System.arraycopy(_alpha, 0, _alpha, idx+1,_alpha.length-idx);
        _alpha = newTransitions;
        _succ = newStates;
      }
      return s;
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
      sb.append("state: " + stidx + " skip:" + st._skip + " transitions: ");
      if(st._succ != null){
        for(int i = st._succ.length-1; i >= 0; --i) {
          short s = st._succ[i];
          if(s == _state0)break;
          openedNodes.push(s);
          sb.append((char)st._alpha[i] + ":" + s + " ");
        }
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  public short addByte(byte b){
    if(_killed)return -1;
    assert  0 <= b && b < 128;
    if(_nfinalStates < max_tokens)_state = _states[_state].getTransition(b);
    return _states[_state]._skip;
  }

  String [] compress(){
    if(_killed) return null;
    ArrayList<State> newStates = new ArrayList<State>();
    // put final states in the beginning...
    for(int i = 0; i < _nfinalStates;++i)newStates.add(null);
    String [] strings = new String[_nfinalStates];
    _states[0].compress(newStates, strings, new StringBuilder(),(short)0,false);
    _states = new State[newStates.size()];
    _states = newStates.toArray(_states);
    _compressed = true;
    _state0 = _nfinalStates;
    _state = _state0;
    _nstates = (short)_states.length;
    return strings;
  }

  /**
   * get token id of the currently parsed String and reset the Trie to start new word.
   */
  public int getTokenId(){
    if(_nfinalStates >= max_tokens-1)return -1;
    int idx = Arrays.binarySearch(_finalStates, 0,_nfinalStates,_state);
    if(idx < 0) {
      if(_nfinalStates == _finalStates.length){
        _finalStates = Arrays.copyOf(_finalStates, _nfinalStates + (_nfinalStates >> 1));
      }
      _finalStates[_nfinalStates++] = _state;
    }
    int res = _state;
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
      os.writeShort(_compressed?_state0:_nfinalStates);
      for(int i =0; i < _nstates; ++i)_states[i].write(os);
    }
  }

  public void write(Stream s) {
    s.setz(_killed);
    if(!_killed){
      s.setz(_compressed);
      s.set2(_nstates);
      s.set2(_compressed?_state0:_nfinalStates);
      for(int i =0; i < _nstates; ++i)_states[i].write(s);
    }
  }

  public void read(Stream s) {
    _killed = s.getz();
    if(!_killed){
      _compressed = s.get1() == 1;
      _nstates = (short)s.get2();
      _nfinalStates = (short)s.get2();
      if(_compressed)_state0 = _nfinalStates;
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
      _nfinalStates = is.readShort();
      if(_compressed)_state0 = _nfinalStates;
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
        j += t.addByte(bs[j])+1;
      }
      res[i++] = t.getTokenId();
    }
    return res;
  }
  public static void main(String [] args){
    FastTrie t = new FastTrie();
    String [] words = new String[]{"haha","gaga","hahagaga", "hahaha","gagaga"};
    int [] res1 = addWords(words, t);
    int [] res2 = addWords(words, t);
    System.out.println("res1 = " + Arrays.toString(res1));
    System.out.println("res2 = " + Arrays.toString(res2));
    System.out.println("Trie: ");
    System.out.println(t.toString());

    FastTrie t2 = new FastTrie();
    String [] words2 = new String[]{"haha","abc", "gogo","gaga","hahagaga", "hahaha","gagaga"};
    int [] res3 = addWords(words2, t2);
    int [] res4 = addWords(words2, t2);
    System.out.println("res3 = " + Arrays.toString(res3));
    System.out.println("res4 = " + Arrays.toString(res4));
    t.merge(t2);
    res2 = addWords(words, t);
    System.out.println("res5 = " + Arrays.toString(res2));
    System.out.println(t);
    res2 = addWords(words2, t);
    System.out.println("res6 = " + Arrays.toString(res2));
    System.out.println(t);
    String[] strings = t.compress();
    System.out.println("###################################################################");
    System.out.println(Arrays.toString(strings));
    System.out.println("===================================================================");
    System.out.println(t);
    res2 = addWords(words2, t);
    System.out.println("res7 = " + Arrays.toString(res2));
    System.out.println(t);
  }
}
