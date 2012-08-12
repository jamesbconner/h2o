package hexlytics.tests;

import hexlytics.RandomTree;
import hexlytics.data.Data;
import hexlytics.data.Data.Row;
import hexlytics.data.DataAdapter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import water.DKV;
import water.DRemoteTask;
import water.H2O;
import water.H2ONode;
import water.Key;
import water.RemoteTask;
import water.UDP;
import water.Value;
import water.ValueArray;
import water.csv.CSVParserKV;

/**
 * Distributed RF implementation for poker Data set. 
 * 
 * @author tomas
 * 
 */
public class PokerDRF extends DRemoteTask {

  int _myNodeId;
  int _totalTrees;
  int _nTreesPerBuilder;
    
  PokerValidator _val;
  
  boolean _validatorRunning;

  String _nodePrefix = null;
  Key[] _nextTreeKeys;

  boolean _done;
   
  long _error; // total number of misclassified records (from validation data)
  long _nrecords; // number of validation records
  double _finalError; 
  
  public PokerDRF (){}
  
  Key _rootKey;
  
  public PokerDRF(Key k, int nTreesPerNode, String keyPrefix) {
    Key [] keys = new Key[H2O.CLOUD._memary.length];
    int kIdx = 0;
    for(H2ONode node:H2O.CLOUD._memary){      
      keys[kIdx] = Key.make("PokerRF" + kIdx,(byte)1,Key.DFJ_INTERNAL_USER, H2O.CLOUD._memary[kIdx]);      
      Value v = new Value(keys[kIdx],1024);
      byte [] mem = v.mem();
      UDP.set4(mem, 0, kIdx);
      UDP.set4(mem, 4, keys.length);
      UDP.set4(mem, 8, nTreesPerNode);
      UDP.set4(mem, 12, k._kb.length);
      System.arraycopy(k._kb, 0, mem, 16, k._kb.length);
      byte [] keyPrefixBytes = keyPrefix.getBytes();
      UDP.set4(mem, 16 + k._kb.length, keyPrefixBytes.length);
      System.arraycopy(keyPrefixBytes, 0, mem, 16 + k._kb.length + 4, keyPrefixBytes.length);      
      DKV.put(keys[kIdx], v);
      ++kIdx;
      _rootKey = k;
    }        
  }
  
  class ProgressMonitor implements Runnable {
    int _nTreesComputed;
    
    double _currentError;
    
    Key [] _resultKeys;
    long [] _valRecordsPerNode;
    ArrayList<ArrayList<long[]>>  _errors;
    ProgressMonitor(Key [] resultKeys){
      _resultKeys = resultKeys;
      _errors = new ArrayList<ArrayList<long[]>>();
      for(int i = 0; i < resultKeys.length; ++i)
        _errors.add(new ArrayList<long[]>());
    }
    
    @Override
    public void run() {
      while(true){
        boolean changed = false;
        for(int i = 0; i < _resultKeys.length; ++i){
          Key k = _resultKeys[i];
          Value v = DKV.get(k);
          if(v == null) continue;
          int ntrees = (int)v.length()/20;
          if(_errors.get(i).size() < ntrees) { // did the node process any more trees since last time?
            long [] newErr = new long[2];
            byte [] mem = v.get(20);
            assert UDP.get4(mem, 0) == ntrees;
            newErr[0] = UDP.get8(mem, 4);
            newErr[1] = UDP.get8(mem, 12);
            changed = (_errors.get(i).size() == _nTreesComputed);
            _errors.get(i).add(newErr);            
          }
          DKV.remove(k);          
        }
        if(changed){
          int n = Integer.MAX_VALUE;        
          for(int i = 0; i < _resultKeys.length; ++i){
            if(_errors.get(i).size() < n) n = _errors.get(i).size(); 
          }      
          if(n > _nTreesComputed){
            long nErrors = 0;
            long nRecords = 0;
            for(int i = 0; i < _resultKeys.length; ++i){
              long [] x = _errors.get(i).get(n);
              nErrors += x[1];
              nErrors += x[1];
            }
          }
        }
        
        try {
          Thread.sleep(10*1000);
        } catch (InterruptedException e) {          
        }
      }      
    }
    
  }
  
  public void doRun(){
    if(_rootKey == null) return;
    Value v = DKV.get(_rootKey);
    if(v == null) return;
    Key [] keys;
    if(v instanceof ValueArray){
      keys = new Key[(int)v.chunks()];
      for(long i = 0; i < keys.length; ++i){
        keys[(int)i] = v.chunk_get(i);
      }      
    } else {
      keys = new Key[]{_rootKey};
    }
    long startTime = System.currentTimeMillis();
    rexec(keys);
    _finalError = (double)_error / (double)_nrecords;
    long runTime = System.currentTimeMillis() - startTime;
    System.out.println("DRF computed in " + (runTime / 1000) + " seconds with error = " + _finalError);
  }
  
  public class PokerValidator implements Runnable {
    int _nProcessedTrees;
    int[] _nProcessedTreesPerNode;    
    int[][] _classVoteCounts;    
    Data _data;       
    
    public PokerValidator(Data data) {
      _data = data;
    }

    void computeError(){
      // compute the classification and errors
      int i = 0;
      _error = 0;
      for (Row r : _data) {
        int answer = 0;
        int nvotes = 0;
        for (int j = 0; j < _classVoteCounts[i].length; ++j) 
          if (_classVoteCounts[i][j] > nvotes) answer = j;        
        if (r.classOf != answer)
          ++_error;
        ++i;
      }
    }
    
    /** Get trees one by one and validate them on given data. */   
    public void run() {
      Key k = Key.make(_nodePrefix + _myNodeId + "_error");
      synchronized (this) {
        _validatorRunning = true;
      }
      // first compute the votes of each tree
      int treesValidated = 0;
      while (treesValidated < _totalTrees) {
        try {
          RandomTree tree = null;
          for (int i = 0; i < _nextTreeKeys.length; ++i) {
            if (_nextTreeKeys[i] == null)
              continue;
            Value v = DKV.get(_nextTreeKeys[i]);
            if (v == null)
              continue;
            tree = new RandomTree(v.get(), 0);
            if(!_nextTreeKeys[i].home())v.free_mem(); // don't accumulate trees belonging to others!
            _nextTreeKeys[i] = (++_nProcessedTreesPerNode[i] == _nTreesPerBuilder) ? null
                : Key.make(_nodePrefix + i + "_" + _nProcessedTreesPerNode[i]);
            break;
          }
          if (tree != null) {
            // we have a correct tree, validate it on data            
            int rCounter = 0;
            for (Row r : _data)
              ++_classVoteCounts[rCounter++][tree.classify(r)];
            ++treesValidated;
          } else {
            Thread.sleep(1000);
          }
        } catch (InterruptedException ex) {
        }
        computeError();
        // publish my error
        Value v = DKV.get(k);
        
        Value newV = new Value(k,treesValidated*8+4+8);
        
        byte [] newMem = newV.mem();
        UDP.set4(newMem,0,treesValidated);
        UDP.set8(newMem,4,_nrecords);                
        UDP.set8(newMem,12,_error);
        if(v != null) System.arraycopy(v.get(), 12, newMem, 20, (treesValidated-1)*8);
        DKV.put(k, newV);
      }          
      // publish my current error      
      
      _classVoteCounts = null;
      synchronized (this) {
        _validatorRunning = false;
        this.notify();
      }
    }
  }

  @Override
  public void map(Key k) {
    DataAdapter builderData = new DataAdapter("poker", new String[] { "0", "1",
        "2", "3", "4", "5", "6", "7", "8", "9", "10" }, "10");
    DataAdapter validatorData = new DataAdapter("poker", new String[] { "0",
        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" }, "10");
    int[] r = new int[11];
    Value inputVal = DKV.get(k);
    if (inputVal == null)
      return; // nothing to do...
    byte[] inputData = inputVal.get();
    _myNodeId = UDP.get4(inputData, 0);
    int nNodes = UDP.get4(inputData, 4);
    _nTreesPerBuilder = UDP.get4(inputData, 8);
    _totalTrees = nNodes * _nTreesPerBuilder;
    int keyLen = UDP.get4(inputData, 12);
    byte[] kb = Arrays.copyOfRange(inputData, 16, keyLen);
    Key dataRootKey = Key.make(kb);
    Value dataRootValue = DKV.get(dataRootKey);
    if (dataRootValue == null)
      return;
    int nodePrefixLen = UDP.get4(inputData, 16 + keyLen);
    _nodePrefix = new String(inputData, 20 + keyLen, nodePrefixLen);
    long recCounter = 0;
    double[] v = new double[11];

    for (long i = 0; i < dataRootValue.chunks(); ++i) {
      CSVParserKV<int[]> p1 = null;
      Value val = null;
      if (dataRootValue instanceof ValueArray) {
        Key chunk = dataRootValue.chunk_get(i);
        if (!chunk.home())
          continue; // only compute our own keys
        val = DKV.get(chunk);
        if(val == null)continue;
        p1 = new CSVParserKV<int[]>(chunk, 1, r, null);
      } else {
        p1 = new CSVParserKV<int[]>(dataRootValue.get(), r, null);
      }
      for (int[] x : p1) {
        for (int j = 0; j < 11; i++)
          v[j] = x[j];
        if (++recCounter % 3 != 0)
          builderData.addRow(v);
        else {
          validatorData.addRow(v);
          ++_nrecords;
        }
      }
      if(val != null)val.free_mem(); // remove the csv src from mem      
    }

    builderData.freeze();
    validatorData.freeze();
    
    Data bD = Data.make(builderData.shrinkWrap());
    _val = new PokerValidator(Data.make(validatorData.shrinkWrap()));
    builderData = null;
    validatorData = null;
    _val.run();

    
    // now build the trees
    for (int i = 0; i < _nTreesPerBuilder; i++) {
      RandomTree rf = new RandomTree();
      rf.compute(bD);
      Key key = Key.make(_nodePrefix + _myNodeId + "_" + i);
      Value val = new Value(key, rf.serializedSize());
      rf.serialize(val.mem(), 0);
      DKV.put(key, val); // publish the tree to the validators
    }
    // wait for the validator to finish
    synchronized (this) {
      while (_validatorRunning) {
        try {
          this.wait();
        } catch (InterruptedException e) {
        }
      }
    }
    _done = true;
  }

  @Override
  public void reduce(RemoteTask drt) {
//    PokerDRF other = (PokerDRF)drt;
//    _error += other._error;
//    _nrecords += other._nrecords;
  }

  @Override
  protected int wire_len() {
    return 0;
 //   return _done?16:0;    
  }

  @Override
  protected int write(byte[] buf, int off) {
//    buf[off++] = (byte)(_done?1:0);
//    if(_done){
//      UDP.set8(buf, off, _nrecords);
//      UDP.set8(buf, off+16, _error);
//      return off+32;
//    }
    return off;
  }

  @Override
  protected void write(DataOutputStream dos) throws IOException {
//    dos.write((byte)(_done?1:0));
//    if(_done){
//      dos.writeLong(_nrecords);
//      dos.writeLong(_error);      
//    }
  }

  @Override
  protected void read(byte[] buf, int off) {
//    if(buf[off++] == 1){
//     _nrecords = UDP.get8(buf, off);
//     _error = UDP.get8(buf, off+16);
//    }
  }

  @Override
  protected void read(DataInputStream dis) throws IOException {
//    if(dis.read() == 1){
//      _nrecords = dis.readLong();
//      _error = dis.readLong();
//    }
  }

}
