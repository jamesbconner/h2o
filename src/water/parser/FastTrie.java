package water.parser;

import java.util.*;

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
  short _nstates = 1;
  boolean _compressed;

  int max_tokens = 1024;
  byte aMin = Byte.MAX_VALUE,aMax = Byte.MIN_VALUE;
  int nfinalStates = 0;

  public FastTrie(){
    _states[0] = new State();
  }
  final private short addState(State s){
    if(_nstates == _states.length)_states = Arrays.copyOf(_states, _states.length + (_states.length >> 1));
    _states[_nstates] = s;
    return _nstates++;
  }

  final class State {
    byte [] _alpha = new byte[8];
    short [] _succ = new short[8];
    short _skip;
    boolean _final;

    void merge(FastTrie otherTrie, short sIdx){
      State other = otherTrie._states[sIdx];
      _final |= other._final;
      for(int i = 0; i < other._alpha.length; ++i){
        if(other._succ[i] == 0)continue;
        _states[getTransition(other._alpha[i])].merge(otherTrie, other._succ[i]);
      }
    }

    short compress(ArrayList<State> states, short skip){
      int firstSucc = 0;
      for(firstSucc = 0; firstSucc < _succ.length; ++firstSucc)if(_succ[firstSucc] != 0)break;
      int nsucc = _succ.length-firstSucc;
      if(_final || nsucc > 1){
        State s = new State();
        short res = (short)states.size();
        states.add(s);
        s._skip = skip;
        if(nsucc > 0){

          s._succ = new short[nsucc];
          s._alpha = Arrays.copyOfRange(_alpha,firstSucc,firstSucc+nsucc);
          for(int i = 0; i < nsucc; ++i){
            if(_succ[i+firstSucc] == 0)continue;
            s._succ[i] = _states[_succ[i+firstSucc]].compress(states, (short)0);
          }
        }
        return res;
      } else return _states[_succ[firstSucc]].compress(states, ++skip);
    }

    final short getTransition(byte c){
      int firstSucc = 0;
      for(;firstSucc < _succ.length && _succ[firstSucc] == 0; ++firstSucc);
      int idx = Arrays.binarySearch(_alpha, firstSucc, _succ.length, c);
      if(idx >= 0)return _succ[idx];
      if(_compressed)throw new Error("missing transition in compressed trie!");
      // we need a new state
      short s = addState(new State());
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
  public String toString(){
    LinkedList<Short> openedNodes = new LinkedList<Short>();
    openedNodes.add((short)0);
    StringBuilder sb = new StringBuilder();
    while(!openedNodes.isEmpty()){
      short stidx = openedNodes.pollFirst();
      State st = _states[stidx];
      sb.append("state: " + stidx + " skip:" + st._skip +  " final:" + st._final + " transitions: ");
      for(int i = st._succ.length-1; i >= 0; --i){
        short s = st._succ[i];
        if(s == 0)break;
        openedNodes.push(s);
        sb.append((char)st._alpha[i] + ":" + s + " ");
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  public short addByte(byte b){
    assert  0 <= b && b < 128;
    if(nfinalStates < max_tokens)_state = _states[_state].getTransition(b);
    return _states[_state]._skip;
  }

  void compress(){
    ArrayList<State> newStates = new ArrayList<State>();
    _states[0].compress(newStates, (short)0);
    _states = new State[newStates.size()];
    _states = newStates.toArray(_states);
    _compressed = true;
  }

  /**
   * get token id of the currently parsed String and reset the Trie to start new word.
   */
  public int getTokenId(){
    if(nfinalStates >= max_tokens-1)return -1;
    nfinalStates++;
    _states[_state]._final = true;
    int res = _state;
    _state = 0;
    return res;
  }

  public void merge(FastTrie other){
    if(_nstates == 0){
      _states = other._states;
      _nstates = other._nstates;
    }
    _states[0].merge(other, (short)0);
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
    t.compress();
    System.out.println("===================================================================");
    System.out.println(t);
    res2 = addWords(words2, t);
    System.out.println("res7 = " + Arrays.toString(res2));
    System.out.println(t);

  }
}
