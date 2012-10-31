package water.parser;

import java.util.ArrayList;
import java.util.Arrays;

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
  short _nstates;

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
    boolean _final;

    void merge(FastTrie otherTrie, short sIdx){
      State other = otherTrie._states[sIdx];
      _final |= other._final;
      for(int i = 0; i < other._alpha.length; ++i){
        if(other._succ[i] == 0)continue;
        _states[getTransition(other._alpha[i])].merge(otherTrie, other._succ[i]);
      }
    }
    short toPatricianTrieStates(ArrayList<PatricianTrie.State> states, ArrayList<Short> finalStates, short skip){
      int firstSucc = 0;
      for(firstSucc = 0; firstSucc < _succ.length; ++firstSucc)if(_succ[firstSucc] != 0)break;
      int nsucc = _succ.length-firstSucc;
      if(_final || nsucc > 1){
        PatricianTrie.State s = new PatricianTrie.State();
        short res = (short)states.size();
        states.add(s);
        if(nsucc > 0){
          s._skip = skip;
          s._succ = new short[nsucc];
          s._alpha = Arrays.copyOfRange(_alpha,firstSucc,nsucc);
          for(int i = firstSucc; i < nsucc; ++i){
            if(_succ[i] == 0)continue;
            s._succ[i] = _states[_succ[i]].toPatricianTrieStates(states, finalStates, (short)0);
          }
        }
        return res;
      } else return _states[_succ[firstSucc]].toPatricianTrieStates(states, finalStates, ++skip);
    }

    final short getTransition(byte c){
      int firstSucc = 0;
      for(;firstSucc < _succ.length && _succ[firstSucc] == 0; ++firstSucc);
      int idx = Arrays.binarySearch(_alpha, firstSucc, _succ.length, c);
      if(idx >= 0)return _succ[idx];
      // we need a new state
      short s = addState(new State());
      idx = -idx - 1;
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
  void addByte(byte b){
    assert  0 <= b && b < 128;
    if(nfinalStates < max_tokens)_state = _states[_state].getTransition(b);
  }

  PatricianTrie asPatricianTrie(){
    PatricianTrie res = new PatricianTrie();
    ArrayList<PatricianTrie.State> pStates = new ArrayList<FastTrie.PatricianTrie.State>();

    return res;
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

  public static class PatricianTrie {
    short _state;
    short [] _finalStateIdx;
    State [] _states;

    public short addByte(byte b) {
      _state = _states[_state].transition(b);
      return _states[_state]._skip;
    }

    public int getTokenId(){
      int res = Arrays.binarySearch(_finalStateIdx, _state);
      _state = 0;
      return res;
    }

    final static class State {
      short _skip;
      byte  [] _alpha;
      short [] _succ;
      public short transition(byte b){
        return _succ[Arrays.binarySearch(_alpha, b)];
      }
    };
  }

  public static int [] addWords (String [] words, FastTrie t){
    int [] res = new int[words.length];
    int i = 0;
    for(String w:words){
      for(byte b:w.getBytes())t.addByte(b);
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
  }
}
