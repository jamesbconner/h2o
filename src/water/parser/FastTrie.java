package water.parser;

import static org.junit.Assert.*;
import init.H2OSerializable;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import sun.misc.Unsafe;
import water.nbhm.UtilUnsafe;



/**
 * Trie for parsing enums in the FastParser.
 *
 *
 * @author tomasnykodym
 *
 */
public final class FastTrie implements H2OSerializable {
  State _initialState, _state;
  transient AtomicInteger _finalStates = new AtomicInteger(0);
  // Boxed into object so that it is shared among threads on local node
  transient Boolean _killed = new Boolean(false);
  boolean _localKilled;

  public boolean isKilled(){
    if(_localKilled)return true;
    return (_localKilled = _killed);
  }
  boolean _compressed;
  int _compressedFinalStates;

  private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
  private static final long _trnOffset;
  private static final long _idOffset;

  static {                      // <clinit>
    Field f1=null,f2=null;
    try {
      f1 = FastTrie.State.class.getDeclaredField("_transitions");
      f2 = FastTrie.State.class.getDeclaredField("_id");
    } catch( java.lang.NoSuchFieldException e ) { System.err.println("Can't happen");
    }
    _trnOffset = _unsafe.objectFieldOffset(f1);
    _idOffset = _unsafe.objectFieldOffset(f2);
  }

  private static final int _Obase  = _unsafe.arrayBaseOffset(Object[].class);
  private static final int _Oscale = _unsafe.arrayIndexScale(Object[].class);
  private static long rawIndex(final Object[] ary, final int idx) {
    assert idx >= 0 && idx < ary.length;
    return _Obase + idx * _Oscale;
  }

//  static class TooManyStatesException extends Exception{
//    public TooManyStatesException(){super("Too many states in FastTrie");}
//  }
  @Override public FastTrie clone() {
    FastTrie res = new FastTrie();
    res._state = _state;
    res._compressed = _compressed;
    res._killed = _killed;
    res._initialState = _initialState;
    return res;
  }

  public FastTrie(){
    _initialState = new State();
    _state = _initialState;
  }

  /** A wrapper for all the tests we have for fast trie. Just calls all test we
   * have conveniently from one place.
   */
  public static void test() {
  }

  public void kill(){
    assert !_compressed:"should not be killing compressed trie!";
    _killed = true;
    _localKilled = true;
  }

  final static class State implements H2OSerializable {
    int _skip;
    int _id = -1;
    State _transitions[][];

    boolean setFinal(){
      return _unsafe.compareAndSwapInt(this, _idOffset, -1, 1);
    }
    boolean isFinal(){return _id >= 0;}
    boolean addState(int i, int j){
      return _unsafe.compareAndSwapObject(_transitions[i], rawIndex(_transitions[i], j), null, new State());
    }
    boolean addTransitionRange(int i) {
      return _unsafe.compareAndSwapObject(_transitions, rawIndex(_transitions, i), null, new State[16]);
    }
    boolean createTransitions(){
      return _unsafe.compareAndSwapObject(this, _trnOffset, null, new State[16][]);
    }

    public State(){}
  }

  private State getTransition(State s, int c){
    assert (c & 0xFF) == c;
    int i = c >> 4;
    int j = (c & 0x0F);
    if(!_compressed){
      if(s._transitions == null)s.createTransitions();
      if(s._transitions[i] == null)s.addTransitionRange(i);
      if(s._transitions[i][j] == null)s.addState(i,j);
    }
    return s._transitions[i][j];
  }

  @Override
  public String toString() {
    if(_killed)return "FastTrie(killed)";
    LinkedList<State> openedNodes = new LinkedList<State>();
    openedNodes.add(_initialState);
    StringBuilder sb = new StringBuilder();
    while(!openedNodes.isEmpty()){
      State st = openedNodes.pollFirst();
      //sb.append("; { state: " + stidx + (st._isFinal?"*":"") + " skip:" + st._skip + " transitions: ");
      if(st._transitions != null){
        for(int i = 0; i < 16; ++i) {
          if(st._transitions[i] == null)continue;
          for(int j = 0; j < 16; ++j){
            State s = st._transitions[i][j];
            if(s == null)continue;
            openedNodes.push(s);
            sb.append((char)((i << 4) + j) + ":" + s + " ");
          }
        }
      }
      sb.append(" }");
    }
    return sb.toString();
  }

  public int addCharacter(int b) {
    if(_killed)return 0;
    _state = getTransition(_state,b);
    return _state._skip;
  }

  State compressState(State oldS, ArrayList<String> strings, StringBuilder currentString, int skip) {
    // index of the state to be returned
    State s;
    // compute number of successors
    int successors = 0;
    int x = 0; // buffer of the single transition - if any
    int y = 0; // position in the buffer of the single transition, if any
    if (oldS._transitions != null)
FIND_SUCCESSORS:
      for (int i = 0; i < 16; ++i) // buffers
        if (oldS._transitions[i] != null)
          for (int j = 0; j < 16; ++j)
            if ((s = oldS._transitions[i][j]) != null) {
              ++successors;
              if (successors > 1)
                break FIND_SUCCESSORS;
              x = i;
              y = j;
            }
    // never compress state with more than one successors, initial state, or a final state,
    // which has 0 successors
    if ((successors == 1) && (oldS != _initialState) && (!oldS.isFinal())) {
      State nextS = oldS._transitions[x][y];
      currentString.append((char)((x << 4) + y));
      s =  compressState(nextS, strings, currentString, ++skip);
      currentString.setLength(currentString.length()-1);
    } else {
      // we cannot compress the state now, create a new state
      s = new State();
      s._skip = skip;
      if (oldS.isFinal()) {
        strings.add(currentString.toString());
        s._id = _compressedFinalStates++;
      }
      if (successors != 0) {
        s._transitions = new State[16][];
        for (int i = 0; i < 16; ++i) {
          if (oldS._transitions[i] != null) {
            s._transitions[i] = new State[16];
            for (int j = 0; j < 16; ++j) {
              if (oldS._transitions[i][j] != null) {
                currentString.append((char)((i << 4) + j));
                s._transitions[i][j] = compressState(oldS._transitions[i][j], strings, currentString,0);
                currentString.setLength(currentString.length()-1);
              }
            }
          }
        }
      }
    }
    return s;
  }

  String [] compress(){
    if(_killed) return null;
    ArrayList<String> strings = new ArrayList<String>();
    _initialState = compressState(_initialState, strings, new StringBuilder(),0);
    _state = _initialState;
    String [] res =  new String[strings.size()];
    strings.toArray(res);
    return res;
  }

  public static final int MAX_VALS = 256;
  /**
   * get token id of the currently parsed String and reset the Trie to start new word.
   */
  public int getTokenId(){
    if(_killed)return -1;
    assert (!_compressed || (_state.isFinal()));
    if(!_state.isFinal() && _state.setFinal()){
      if(_finalStates.addAndGet(1) > MAX_VALS)
        kill();
    }
    int res = _state._id;
    _state = _initialState;
    return res;
  }

  public void merge(FastTrie other){
    if(other._localKilled)kill();
    if(_localKilled)return;
    if(_initialState != other._initialState)
      mergeStates(_initialState,other, other._initialState);
  }

  void mergeStates(State myS, FastTrie otherTrie, State otherS) {
    assert !_compressed;
    if (otherS.isFinal())
      myS._id = 1;
    if(otherS._transitions == null)
      return;
    for(int i = 0; i < 16; ++i){
      if(otherS._transitions[i] == null)
        continue;
      for(int j = 0; j < 16; ++j){
        if(otherS._transitions[i][j] == null)continue;
        mergeStates(getTransition(myS,((i << 4) + j)),otherTrie, otherS._transitions[i][j]);
      }
    }
  }
}
