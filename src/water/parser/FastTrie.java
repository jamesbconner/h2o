package water.parser;

import init.H2OSerializable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Trie for parsing enums in the FastParser.
 *
 *
 * @author tomasnykodym
 *
 */
public final class FastTrie implements H2OSerializable {
  int _state;
  State [] _states = new State[1];
  byte[] _finalStates = new byte[1];
  short _nstates = 1;
  boolean _compressed;
  boolean _killed;
  short _state0 = (short)0;

  static class TooManyStatesException extends Exception{
    public TooManyStatesException(){super("Too many states in FastTrie");}
  }
  public FastTrie clone() {
    FastTrie res = new FastTrie();
    res._state = _state;
    res._states = _states;
    res._finalStates = _finalStates;
    res._nstates = _nstates;
    res._compressed = _compressed;
    res._killed =_killed;
    res._state0 = _state0;
    return res;
  }
  int _id = (int)Math.random()*100;
  public FastTrie(){
    _states[0] = new State();
  }
  final private short addState(State s) throws TooManyStatesException {
    if(_nstates == Short.MAX_VALUE)throw new TooManyStatesException();
    if(_nstates == _states.length) {
      _states = Arrays.copyOf(_states, Math.min(Short.MAX_VALUE, _states.length + (_states.length >> 1) + 1));
      _finalStates = Arrays.copyOf(_finalStates, Math.min(Short.MAX_VALUE, _finalStates.length + (_finalStates.length >> 1) + 1));
    }
    _states[_nstates] = s;
    assert _nstates < _states.length:"unexpected number of states:" + _nstates + ", states.length = " + _states.length;
    return _nstates++;
  }

  public void kill(){
    assert !_compressed:"should not be killing compressed trie!";
    _killed = true;
    _states = null;
  }

  final static class State implements H2OSerializable {
    short _skip;
    short _transitions[][];
    public State(){}
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
      boolean finalState = _compressed?(stidx < _state0):_finalStates[stidx] == 1;
      sb.append("; { state: " + stidx + (finalState?"*":"") + " skip:" + st._skip + " transitions: ");
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
      sb.append(" }");
    }
    return sb.toString();
  }

  public short addCharacter(int b){
    if(_killed)return 0;
    try{
      _state = getTransition(_states[_state],b);
      return _states[_state]._skip;
    }catch(TooManyStatesException e){
      kill();
      return 0;
    }
  }

  int compressState(State oldS, ArrayList<State> states, String [] strings, StringBuilder currentString, short skip, boolean finalState){
    int nsucc = 0;
    int x = 0,y = 0;
    if(oldS._transitions != null){
      for(int i = 0; i < 16; ++i){
        if(oldS._transitions[i] == null)continue;
        for(int j = 0; j < 16; ++j)
          if(oldS._transitions[i][j] != 0){
            ++nsucc;
            x = i; y = j;
          }
      }
    }
    if(nsucc != 1 || finalState || states.size() <= strings.length){
      State s = new State();
      short res = (short)states.size();
      if(finalState) {
        for(res = 0; res < states.size(); ++res)if(states.get(res) == null)break;
        states.set(res, s);
        strings[res] = currentString.toString();
      } else states.add(s);
      s._skip = skip;
      if(nsucc > 0){
        s._transitions = new short[16][];
        for(int i = 0; i < 16; ++i) {
          if(oldS._transitions[i] != null){
            s._transitions[i] = new short[16];
            Arrays.fill(s._transitions[i],(short)strings.length); // fill with the new state 0
            for(int j = 0; j < 16; ++j){
              short nextS = oldS._transitions[i][j];
              if(nextS != 0){
                currentString.append((char)((i << 4) + j));
                s._transitions[i][j] = (short)compressState(_states[nextS],states, strings, currentString, (short)0, _finalStates[nextS] == 1);
                currentString.setLength(currentString.length()-1);
              }
            }
          }
        }
      }
      return res;
    } else {
      short nextS = oldS._transitions[x][y];
      currentString.append((char)((x << 4) + y));
      int res =  compressState(_states[nextS], states, strings, currentString, ++skip, _finalStates[nextS] == 1);
      currentString.setLength(currentString.length()-1);
      return res;
    }
  }


  String [] compress(){
    if(_killed) return null;
    int  nfinalStates = 0;
    for(byte b:_finalStates)nfinalStates += b;
    ArrayList<State> newStates = new ArrayList<State>();
    // add nulls for final states to make sure we can add all final states in the beginningof the array
    for(int i = 0; i < nfinalStates; ++i)newStates.add(null);
    // put final states in the beginning...
    String [] strings = new String[nfinalStates];
    int origStates = _states.length;
    compressState(_states[0],newStates, strings, new StringBuilder(),(short)0,false);
    _states = new State[newStates.size()];
    _states = newStates.toArray(_states);
    _compressed = true;
    _state0 = (short)nfinalStates;
    _state = _state0;
    _nstates = (short)_states.length;
    _finalStates = null;
    System.out.println("Trie compressed  from " + origStates + " to " + _states.length + " states");
    return strings;
  }

  /**
   * get token id of the currently parsed String and reset the Trie to start new word.
   */
  public int getTokenId(){
    if(_killed)return -1;
    if(_state == _state0)return -1;
    assert (!_compressed || (_state < _state0));
    int res =  _state;
    if(!_compressed)_finalStates[_state] = 1;
    _state = _state0;
    return res;
  }

  public void merge(FastTrie other){
    if(other._killed)kill();
    if(_killed)return;
    if(_nstates == 0){
      _states = other._states;
      _nstates = other._nstates;
    } else {
      try {
        mergeStates(0,other, 0);
      } catch( TooManyStatesException e ) {
        kill();
      }
    }
  }


  final private int getTransition(State s, int c) throws TooManyStatesException {
    try{
      assert (c & 0xFF) == c;
      int idx = c >> 4;
      c &= 0x0F;
      if(_compressed){
        _compressed = (false || _compressed);
      }
      assert !_compressed || (s._transitions != null && s._transitions[idx] != null && s._transitions[idx][c] != _state0):"missing transition in compressed Trie!";
      if(!_compressed) {
        if(s._transitions == null)s._transitions = new short[16][];
        if(s._transitions[idx] == null)s._transitions[idx] = new short[16];
        if(s._transitions[idx][c] == _state0) s._transitions[idx][c] = addState(new State());
      }
      return s._transitions[idx][c];
    }catch(NullPointerException e){
      e.printStackTrace();
      throw new Error(e);
    }
  }

  void mergeStates(int myIdx, FastTrie otherTrie, int sIdx) throws TooManyStatesException{
    assert !_compressed;
    State s = _states[myIdx];
    State other = otherTrie._states[sIdx];
    if(otherTrie._finalStates[sIdx] == 1)_finalStates[myIdx] = 1;
    if(other._transitions == null)return;
    for(int i = 0; i < 16; ++i){
      if(other._transitions[i] == null)continue;
      for(int j = 0; j < 16; ++j){
        if(other._transitions[i][j] == 0)continue;
       // System.out.println(_id + " _state = " + _state);
        int x = getTransition(s,((i << 4) + j));
        mergeStates(x,otherTrie, other._transitions[i][j]);
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
        j += t.addCharacter(bs[j]&0xFF)+1;
      }
      res[i++] = t.getTokenId();
    }
    return res;
  }

  static String [] data = new String[] {"J","G","B","B","D","D","I","I","F","F","I","I","I","I","I","H","I","I","I","I","C","A","A","J","J","I","I"};

  public static void main(String [] args) throws SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException{
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
    System.out.println(data.getClass());
    System.out.println(data.getClass().getComponentType().getName());
  }
}
