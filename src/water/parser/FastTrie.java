package water.parser;

import com.sun.corba.se.spi.activation._ActivatorImplBase;
import init.H2OSerializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;



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
  short _nstates = 1;
  boolean _compressed;
  boolean _killed;
  short _initialState = (short)0;

  static class TooManyStatesException extends Exception{
    public TooManyStatesException(){super("Too many states in FastTrie");}
  }
  @Override public FastTrie clone() {
    FastTrie res = new FastTrie();
    res._state = _state;
    res._states = _states;
    res._nstates = _nstates;
    res._compressed = _compressed;
    res._killed = _killed;
    res._initialState = _initialState;
    return res;
  }
  int _id = (int)Math.random()*100;
  public FastTrie(){
    _states[0] = new State();
  }
  
  /** A wrapper for all the tests we have for fast trie. Just calls all test we
   * have conveniently from one place. 
   */
  public static void test() {
    State.testEmptyState();
    testEmptyTrie();
    testKill();
    testAddState();
    testAddCharacter();
    testGetTokenId();
    testGetTransition();
    testMergeStates();
    testMerge();
    testCompressState();
    System.out.println("ALL OK");
  }
  
  @Test public static void testEmptyTrie() {
    FastTrie t = new FastTrie();
    assertNotNull(t._states);
    assertEquals(false, t._compressed);
    assertEquals(1,t._states.length);
    assertEquals(0, t._initialState);
    assertEquals(1,t._nstates);
    assertEquals(false,t._killed);
  }
  
  private short addState(State s) throws TooManyStatesException {
    if(_nstates == Short.MAX_VALUE)throw new TooManyStatesException();
    if(_nstates == _states.length) {
      _states = Arrays.copyOf(_states, Math.min(Short.MAX_VALUE, _states.length + (_states.length >> 1) + 1));
    }
    _states[_nstates] = s;
    assert _nstates < _states.length:"unexpected number of states:" + _nstates + ", states.length = " + _states.length;
    return _nstates++;
  }
  
  @Test public static void testAddState() {
    FastTrie t = new FastTrie();
    // we have one state, and we will grow.
    State s0 = t._states[0];
    State s1 = new State();
    try {
      assertEquals(1,t.addState(s1));
    } catch( TooManyStatesException ex ) {
      assertTrue(false);
    }
    assertEquals(s0,t._states[0]);
    assertEquals(s1,t._states[1]);
    assertEquals(2,t._states.length);
    assertEquals(2,t._nstates);
    try {
      assertEquals(2,t.addState(s1));
      assertEquals(3,t.addState(s1));
      assertEquals(4,t.addState(s1));
      assertEquals(5,t.addState(s1));
    } catch( TooManyStatesException ex ) {
      assertTrue(false);
    }
    assertEquals(6, t._nstates);
    assertEquals(7, t._states.length);
  }

  public void kill(){
    assert !_compressed:"should not be killing compressed trie!";
    _killed = true;
    _states = null;
  }
  
  @Test public static void testKill() {
    FastTrie t = new FastTrie();
    t.kill();
    assertEquals(true,t._killed);
    assertNull(t._states);
  }

  final static class State implements H2OSerializable {
    short _skip;
    short _transitions[][];
    boolean _isFinal;
    public State(){}
    
    @Test public static void testEmptyState() {
      State s = new State();
      assertEquals(0, s._skip);
      assertNull(s._transitions);
      assertFalse(s._isFinal);
    }
  }


  @Override
  public String toString() {
    if(_killed)return "FastTrie(killed)";
    LinkedList<Short> openedNodes = new LinkedList<Short>();
    openedNodes.add(_initialState);
    StringBuilder sb = new StringBuilder();
    while(!openedNodes.isEmpty()){
      short stidx = openedNodes.pollFirst();
      State st = _states[stidx];
      sb.append("; { state: " + stidx + (st._isFinal?"*":"") + " skip:" + st._skip + " transitions: ");
      if(st._transitions != null){
        for(int i = 0; i < 16; ++i) {
          if(st._transitions[i] == null)continue;
          for(int j = 0; j < 16; ++j){
            short s = st._transitions[i][j];
            if(s == _initialState)continue;
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
    try{
      _state = getTransition(_states[_state],b);
      return _states[_state]._skip;
    }catch(TooManyStatesException e){
      kill();
      return 0;
    }
  }
  
  @Test public static void testAddCharacter() {
    FastTrie t = new FastTrie();
    assertEquals(0, t.addCharacter(5));
    assertEquals(1, t._state);
    t.kill();
    assertEquals(0, t.addCharacter(5));
  }

  int compressState(State oldS, ArrayList<State> states, String [] strings, StringBuilder currentString, short skip) {
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
    if(nsucc != 1 || oldS._isFinal || states.size() <= strings.length){
      State s = new State();
      short res = (short)states.size();
      if(oldS._isFinal) {
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
                s._transitions[i][j] = (short)compressState(_states[nextS],states, strings, currentString, (short)0);
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
      int res =  compressState(_states[nextS], states, strings, currentString, ++skip);
      currentString.setLength(currentString.length()-1);
      return res;
    }
  }
  
  @Test public static void testCompressState() {
        
    
  }

  String [] compress(){
    if(_killed) return null;
    int  nfinalStates = 0;
    for (int i = 0; i < _nstates; ++i)
      nfinalStates += _states[i]._isFinal ? 1 : 0;
    ArrayList<State> newStates = new ArrayList();
    // add nulls for final states to make sure we can add all final states in the beginningof the array
    for(int i = 0; i < nfinalStates; ++i)newStates.add(null);
    // put final states in the beginning...
    String [] strings = new String[nfinalStates];
    int origStates = _states.length;
    compressState(_states[0],newStates, strings, new StringBuilder(),(short)0);
    _states = new State[newStates.size()];
    _states = newStates.toArray(_states);
    _compressed = true;
    _initialState = (short)nfinalStates;
    _state = _initialState;
    _nstates = (short)_states.length;
    System.out.println("Trie compressed  from " + origStates + " to " + _states.length + " states");
    return strings;
  }

  /**
   * get token id of the currently parsed String and reset the Trie to start new word.
   */
  public int getTokenId(){
    if(_killed)return -1;
    assert (_state != _initialState);
    assert (!_compressed || (_state < _initialState));
    int res =  _state;
    _states[_state]._isFinal = true;
    _state = _initialState;
    return res;
  }
  
  @Test public static void testGetTokenId() {
    FastTrie t = new FastTrie();
    t.addCharacter(5);
    assertEquals(1,t._state);
    t.addCharacter(7);
    assertEquals(2,t._state);
    assertEquals(2,t.getTokenId());
    assertEquals(0,t._state);
    assertEquals(true, t._states[2]._isFinal);
    t.kill();
    t.addCharacter(5);
    assertEquals(-1,t.getTokenId());
  }
  

  public void merge(FastTrie other){
    if(other._killed)kill();
    if(_killed)return;
    assert (_nstates >= 1);
    assert (other._nstates >= 1);
    try {
      mergeStates(0,other, 0);
    } catch( TooManyStatesException e ) {
      kill();
    }
  }
  
  @Test public static void testMerge() {
    FastTrie t1 = new FastTrie();
    FastTrie t2 = new FastTrie();
    t2.kill();
    t1.merge(t2);
    assertTrue(t1._killed);
    t2 = new FastTrie();
    t2.addCharacter(5);
    t1.merge(t2);
    assertTrue(t1._killed);
    assertNull(t1._states);
    t1 = new FastTrie();
    t1.merge(t2);
    assertEquals(2,t1._nstates);
  }

  private int getTransition(State s, int c) throws TooManyStatesException {
    assert (c & 0xFF) == c;
    int idx = c >> 4;
    c &= 0x0F;
    assert !_compressed || (s._transitions != null && s._transitions[idx] != null && s._transitions[idx][c] != _initialState):"missing transition in compressed Trie!";
    if (!_compressed) {
      if(s._transitions == null)s._transitions = new short[16][];
      if(s._transitions[idx] == null)s._transitions[idx] = new short[16];
      if(s._transitions[idx][c] == _initialState) s._transitions[idx][c] = addState(new State());
    }
    return s._transitions[idx][c];
  }
  
  @Test public static void testGetTransition() {
    try {
      FastTrie t = new FastTrie();
      assertEquals(1,t.getTransition(t._states[0],5));
      State s = t._states[0];
      assertNotNull(s._transitions);
      assertNotNull(s._transitions[0]);
      assertEquals(1, s._transitions[0][5]);
    } catch (TooManyStatesException e) {
      assertTrue(false);
    }
  }

  void mergeStates(int myIdx, FastTrie otherTrie, int sIdx) throws TooManyStatesException{
    assert !_compressed;
    State s = _states[myIdx];
    State other = otherTrie._states[sIdx];
    if (other._isFinal)
      _states[myIdx]._isFinal = true;
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
  
  @Test public static void testMergeStates() {
    try {
      FastTrie t1 = new FastTrie();
      FastTrie t2 = new FastTrie();
      t1.addCharacter(5);
      t2.addCharacter(5);
      t2.getTokenId();
      assertEquals(2,t1._nstates);
      t1.mergeStates(1,t2,1);
      assertTrue(t1._states[1]._isFinal);
      assertEquals(2,t1._nstates);
      t2.addCharacter(5);
      t2.addCharacter(3);
      t1.mergeStates(1,t2,1);
      assertEquals(3,t1._nstates);
      assertEquals(2,t1.getTransition(t1._states[1],3));
      assertFalse(t1._states[2]._isFinal);
      t2.getTokenId();
      t2.addCharacter(5);
      t2.addCharacter(20);
      t1.mergeStates(1,t2,1);
      assertEquals(4,t1._nstates);
      assertEquals(2,t1.getTransition(t1._states[1],3));
      assertEquals(3,t1.getTransition(t1._states[1],20));
      assertTrue(t1._states[2]._isFinal);
    } catch (TooManyStatesException e) {
      assertTrue(false);
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
    
    test();
    
  /*  
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
    System.out.println(data.getClass().getComponentType().getName()); */
  }
}
