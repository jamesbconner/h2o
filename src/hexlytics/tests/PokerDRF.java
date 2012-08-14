package hexlytics.tests;

import hexlytics.RandomForest;
import hexlytics.Tree;
import hexlytics.RFBuilder.Director;
import hexlytics.RFBuilder.Message;
import hexlytics.RFBuilder.TreeBuilder;
import hexlytics.RFBuilder.TreeValidator;
import hexlytics.data.Data;
import hexlytics.data.Data.Row;
import hexlytics.data.DataAdapter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
public class PokerDRF extends DRemoteTask implements Director {

  private static final long serialVersionUID = 1976547559782826435L;
  int _myNodeId;
  int _totalTrees;
  int _nTreesPerBuilder;
  int _nNodes;
  TreeBuilder _treeBldr;

  PokerValidator _val;

  public static String _nodePrefix = null;

  long _error; // total number of misclassified records (from validation data)
  long _nrecords; // number of validation records

  public static String webrun(Key k, int n) {
    PokerDRF pkr = new PokerDRF(k, (n / H2O.CLOUD._memary.length), "DRF"
        + Math.random() + "_" + k.toString());
    long t = System.currentTimeMillis();
    pkr.doRun();
    return "DRF finished in " + (System.currentTimeMillis() - t) / 1000 + "s, "
        + pkr.ntreesComputed() + " trees computed with error = " + pkr.error();
  }

  static void sleep(){
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }
  }
  public PokerDRF() {
  }

  Key _rootKey;
  Key[] _compKeys;
  Key[] _resultKeys;

  ProgressMonitor _progress;

  public PokerDRF(Key k, int nTreesPerNode, String keyPrefix) {
    _compKeys = new Key[H2O.CLOUD._memary.length];
    _resultKeys = new Key[H2O.CLOUD._memary.length];
    int kIdx = 0;
    _nTreesPerBuilder = nTreesPerNode;
    _totalTrees = nTreesPerNode * H2O.CLOUD._memary.length;
    _nNodes = H2O.CLOUD._memary.length;
    for (H2ONode node : H2O.CLOUD._memary) {
      _compKeys[kIdx] = Key.make("PokerRF" + kIdx, (byte) 1,
          Key.DFJ_INTERNAL_USER, H2O.CLOUD._memary[kIdx]);
      _resultKeys[kIdx] = Key.make(keyPrefix + kIdx + "_error");
      if (DKV.get(_resultKeys[kIdx]) != null)
        DKV.remove(_resultKeys[kIdx]);
      Value v = new Value(_compKeys[kIdx], 1024);
      byte[] mem = v.mem();
      UDP.set4(mem, 0, kIdx);
      UDP.set4(mem, 4, _compKeys.length);
      UDP.set4(mem, 8, nTreesPerNode);
      UDP.set4(mem, 12, k._kb.length);
      System.arraycopy(k._kb, 0, mem, 16, k._kb.length);
      byte[] keyPrefixBytes = keyPrefix.getBytes();
      UDP.set4(mem, 16 + k._kb.length, keyPrefixBytes.length);
      System.arraycopy(keyPrefixBytes, 0, mem, 16 + k._kb.length + 4,
          keyPrefixBytes.length);
      DKV.put(_compKeys[kIdx], v);
      ++kIdx;
      _rootKey = k;
    }
  }

  class ProgressMonitor implements Runnable {
    int _nTreesComputed;
    volatile boolean _done;

    double _currentError;

    int[] _nProcessedTrees;
    long[][] _errorHistory; // errors per node and # processed trees
    long[] _valRecordsPerNode;
    long _nRecords;

    ProgressMonitor(Key[] resultKeys) {
      _resultKeys = resultKeys;
      _errorHistory = new long[resultKeys.length][_totalTrees];
      _nProcessedTrees = new int[resultKeys.length];
    }

    public void run() {
      while (!_done) {
        sleep();
        Message.Text t = Message.Text.readNext();
        if(t != null)System.out.println(t);                
      }      
    }
  }

  public int ntreesComputed() {
    return _progress._nTreesComputed / _nNodes;
  }

  public double error() {
    return _progress._currentError;
  }

  public void doRun() {
    if (_rootKey == null)
      return;
    long startTime = System.currentTimeMillis();

    _progress = new ProgressMonitor(_resultKeys);
    Thread t = new Thread(_progress);
    t.start();
    rexec(_compKeys);
    _progress._done = true;    
    long runTime = System.currentTimeMillis() - startTime;
    System.out.println("DRF computed in " + (runTime / 1000)
        + " seconds with error = " + _progress._currentError);
  }

  public static Tree getTreeFromKV(Key k) {
    Value v = DKV.get(k);
    if (v == null)
      return null;
    byte[] m = v.get();
    v.free_mem();
    return (m == null) ? null : new Tree(m, 0);
  }

  public class PokerValidator implements Runnable {
    int _nProcessedTrees;
    int[] _nProcessedTreesPerNode;
    int[][] _classVoteCounts;
    Data _data;

    TreeValidator _validator;

    Key[] _nextTreeKeys;
    boolean _done;

    public PokerValidator(Data data) {
      _data = data;
      _classVoteCounts = new int[_data.rows()][10];
      _nextTreeKeys = new Key[_nNodes];
      _nProcessedTreesPerNode = new int[_nNodes];
      _validator = new TreeValidator(data, PokerDRF.this);
      for (int i = 0; i < _nNodes; ++i) {
        _nProcessedTreesPerNode[i] = 0;
        _nextTreeKeys[i] = Key.make(_nodePrefix + i + "_0");
      }
    }

    long computeError() {
      // compute the classification and errors
      int i = 0;
      long error = 0;
      for (Row r : _data) {
        int answer = 0;
        int nvotes = 0;
        for (int j = 0; j < _classVoteCounts[i].length; ++j)
          if (_classVoteCounts[i][j] > nvotes) {
            nvotes = _classVoteCounts[i][j];
            answer = j;
          }
        if (r.classOf() != answer)
          ++error;
        ++i;
      }
      return error;
    }

    /** Get trees one by one and validate them on given data. */
    public void run() {      
      // first compute the votes of each tree      
      while (_validator.rf_.trees().size() < _totalTrees) {
          Message.Tree msg = Message.Tree.readNext();
          if(msg == null){
            sleep();
            continue;
          }                      
          _validator.validate(msg.tree_);          
      }
      _classVoteCounts = null;
      synchronized (this) {
        _done = true;
        this.notify();
      }
    }
  }

  @Override
  public void map(Key k) {
    
    DataAdapter builderData = new DataAdapter("poker", new String[] { "0", "1",
        "2", "3", "4", "5", "6", "7", "8", "9", "10" }, "10",10);
    DataAdapter validatorData = new DataAdapter("poker", new String[] { "0",
        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" }, "10",10);
    int[] r = new int[11];
    Value inputVal = DKV.get(k);
    if (inputVal == null)
      return; // nothing to do...
    byte[] inputData = inputVal.get();
    _myNodeId = UDP.get4(inputData, 0);
    _nNodes = UDP.get4(inputData, 4);
    _nTreesPerBuilder = UDP.get4(inputData, 8);
    _totalTrees = _nNodes * _nTreesPerBuilder;
    int keyLen = UDP.get4(inputData, 12);
    byte[] kb = Arrays.copyOfRange(inputData, 16, 16 + keyLen);
    Key dataRootKey = Key.make(kb);
    Value dataRootValue = DKV.get(dataRootKey);
    if (dataRootValue == null)
      return;
    int nodePrefixLen = UDP.get4(inputData, 16 + keyLen);
    _nodePrefix = new String(inputData, 20 + keyLen, nodePrefixLen);
    
    long recCounter = 0;
    double[] v = new double[11];
    System.out.print("Parsing the data:");
    for (long i = 0; i < dataRootValue.chunks(); ++i) {

      CSVParserKV<int[]> p1 = null;
      Value val = null;
      if (dataRootValue instanceof ValueArray) {
        Key chunk = dataRootValue.chunk_get(i);
        if (!chunk.home())
          continue; // only compute our own keys
        val = DKV.get(chunk);
        if (val == null)
          continue;
        System.out.print(" " + i);
        p1 = new CSVParserKV<int[]>(chunk, 1, r, null);
      } else {
        p1 = new CSVParserKV<int[]>(dataRootValue.get(), r, null);
      }
      for (int[] x : p1) {
        for (int j = 0; j < 11; j++)
          v[j] = x[j];
        if (++recCounter % 3 != 0)
          builderData.addRow(v);
        else {
          validatorData.addRow(v);
          ++_nrecords;
        }
      }
      if (val != null)
        val.free_mem(); // remove the csv src from mem
    }

    builderData.freeze();
    validatorData.freeze();

    System.out.println("\nShrinking the data:");
    Data bD = Data.make(builderData.shrinkWrap());
    _val = new PokerValidator(Data.make(validatorData.shrinkWrap()));
    builderData = null;
    validatorData = null;
    Thread t = new Thread(_val);
    t.start();    
    _treeBldr = new TreeBuilder(bD, this, _nTreesPerBuilder);
    _treeBldr.run();

    // wait for the validator to finish
    synchronized (_val) {
      while (!_val._done) {
        try {
          _val.wait();
        } catch (InterruptedException e) {
        }
      }
    }
  }

  @Override
  public void reduce(RemoteTask drt) {
    // PokerDRF other = (PokerDRF)drt;
    // _error += other._error;
    // _nrecords += other._nrecords;
  }

  @Override
  protected int wire_len() {
    return 0;
    // return _done?16:0;
  }

  @Override
  protected int write(byte[] buf, int off) {
    // buf[off++] = (byte)(_done?1:0);
    // if(_done){
    // UDP.set8(buf, off, _nrecords);
    // UDP.set8(buf, off+16, _error);
    // return off+32;
    // }
    return off;
  }

  @Override
  protected void write(DataOutputStream dos) throws IOException {
    // dos.write((byte)(_done?1:0));
    // if(_done){
    // dos.writeLong(_nrecords);
    // dos.writeLong(_error);
    // }
  }

  @Override
  protected void read(byte[] buf, int off) {
    // if(buf[off++] == 1){
    // _nrecords = UDP.get8(buf, off);
    // _error = UDP.get8(buf, off+16);
    // }
  }

  @Override
  protected void read(DataInputStream dis) throws IOException {
    // if(dis.read() == 1){
    // _nrecords = dis.readLong();
    // _error = dis.readLong();
    // }
  }

  @Override
  public void onTreeBuilt(Tree tree) {
    new Message.Tree(_treeBldr.size(), tree).send();    
  }

  @Override
  public void onBuilderTerminated() {

  }

  @Override
  public void onAggregatorChange() {
    // TODO Auto-generated method stub

  }

  @Override
  public void onTreeValidated(Tree tree, int rows, int[] badRows, int[] badVotes) {
    // TODO Auto-generated method stub

  }

  @Override
  public void onValidatorTerminated() {
    // TODO Auto-generated method stub

  }

  @Override
  public void report(String what) {
    Message m = new Message.Text(what);
    m.send();    
  }

  @Override
  public String nodeName() {
    return _nodePrefix + _myNodeId;
  }

  @Override
  public void error(long error) {
    // TODO Auto-generated method stub

  }

}
