package hexlytics.tests;

import hexlytics.Tree;
import hexlytics.RFBuilder.Director;
import hexlytics.RFBuilder.Message;
import hexlytics.RFBuilder.Message.ValidationError;
import hexlytics.RFBuilder.TreeBuilder;
import hexlytics.RFBuilder.TreeValidator;
import hexlytics.data.Data;
import hexlytics.data.DataAdapter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import water.DKV;
import water.DRemoteTask;
import water.H2O;
import water.H2ONode;
import water.Key;
import water.RemoteTask;
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

  private static String _nodePrefix = null;
  public static int _nodeId;

  TreeBuilder _treeBldr;
  PokerValidator _val;
  Key[] _compKeys;
  int _totalTrees;
  int _nNodes;
  long _error; // total number of misclassified records (from validation data)
  long _nrecords; // number of validation records
  ProgressMonitor _progress;

  public static String nodePrefix(int nodeIdx) {
    return "P" + _nodePrefix + "N" + nodeIdx + "_";
  }


  public static String webrun(Key k, int n) {
    PokerDRF pkr = new PokerDRF(k, n, "[" + (int) (1000000 * Math.random())
        + "]_");
    long t = System.currentTimeMillis();
    pkr.doRun();
    return "DRF computed. " + pkr._nrecords
        + " records processed in " + ((System.currentTimeMillis() - t) / 1000) + " seconds, error = "
        + (double) pkr._error / (double) pkr._nrecords;
    
  }

  static void sleep() {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }
  }

  public PokerDRF() {
  }

 
  public PokerDRF(Key k, int ntrees, String keyPrefix) {
    _totalTrees = ntrees;
    _nNodes = H2O.CLOUD._memary.length;
    int nTreesPerNode = _totalTrees / _nNodes;
    int kIdx = 0;
    _compKeys = new Key[H2O.CLOUD._memary.length];
    for (H2ONode node : H2O.CLOUD._memary) {
      // home each key to designated computation node
      _compKeys[kIdx] = Key.make("PokerRF" + kIdx, (byte) 1,
          Key.DFJ_INTERNAL_USER, node);
      // store the keys driving (distributing) the application
      Message.Init initMsg = new Message.Init(keyPrefix, _compKeys[kIdx], k,
          nTreesPerNode, ntrees);
      initMsg.send();
      ++kIdx;
    }
  }

  class ProgressMonitor implements Runnable {
    int _nTreesComputed;
    volatile boolean _done;
    double _currentError;
    long _nRecords;

    ProgressMonitor() {   }

    public void run() {
      while (!_done) {
        sleep();
        Message.Text t = Message.Text.readNext();
        if (t != null)
          System.out.println(t);
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
    long startTime = System.currentTimeMillis();
    _progress = new ProgressMonitor();
    Thread t = new Thread(_progress);
    t.start();
    rexec(_compKeys);
    _progress._done = true;
    // read the errors
    long errors = 0;
    long nrecords = 0;
    for (int i = 0; i < _nNodes; ++i) {
      ValidationError err = ValidationError.readFrom(i);
      if (err == null)
        System.err.println("Error: missing error report from node " + i);
      else {
        errors += err.err_;
        nrecords += err.nrecords_;
      }
    }
    long runTime = System.currentTimeMillis() - startTime;
    _nrecords = nrecords;
    _error = errors;
    System.out.println("DRF computed. " + nrecords + " records processed in "
        + (runTime / 1000) + " seconds, error = " + (double) errors
        / (double) nrecords);

  }

  public class PokerValidator implements Runnable {
    Data _data;
    TreeValidator _validator;
    volatile boolean _done;

    public PokerValidator(Data data) {
      _data = data;
      _validator = new TreeValidator(data, PokerDRF.this);
    }

    /** Get trees one by one and validate them on given data. */
    public void run() {
      // first compute the votes of each tree
      while (_validator.rf_.trees().size() < _totalTrees) {
        Message.Tree msg = Message.Tree.readNext();
        if (msg == null) {
          sleep();
          continue;
        }
        _validator.validate(msg.tree_);
      }
      _validator.terminate();
      synchronized (this) {
        _done = true;
        this.notify();
      }
    }
  }

  @Override
  public void map(Key k) {
    DataAdapter builderData = new DataAdapter("poker", new String[] { "0", "1",
        "2", "3", "4", "5", "6", "7", "8", "9", "10" }, "10", 10);
    DataAdapter validatorData = new DataAdapter("poker", new String[] { "0",
        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" }, "10", 10);
    Message.Init initMsg = Message.Init.read(k);
    int[] r = new int[11];
    _nodePrefix = initMsg._nodePrefix;
    _nodeId = H2O.CLOUD.nidx(H2O.CLOUD.SELF);
    _totalTrees = initMsg._totalTrees;

    Value dataRootValue = DKV.get(Key.make(initMsg._kb));
    double[] v = new double[11];
    report("Parsing the data");
    int recCounter = 0;
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
        ++_nrecords;
        if (++recCounter % 3 != 0)
          builderData.addRow(v);
        else {
          validatorData.addRow(v);          
        }
      }
      if (val != null)
        val.free_mem(); // remove the csv src from mem
    }
    builderData.freeze();
    validatorData.freeze();
    report("done parsing, " + _nrecords + " read.");
    report("Shrinking the data");
    Data bD = Data.make(builderData.shrinkWrap());
    _val = new PokerValidator(Data.make(validatorData.shrinkWrap()));
    builderData = null;
    validatorData = null;
    Thread t = new Thread(_val);
    t.start();
    _treeBldr = new TreeBuilder(bD, this, initMsg._nTrees);
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
  }

  @Override
  protected int wire_len() {
    return 0;
  }

  @Override
  protected int write(byte[] buf, int off) {
    return off;
  }

  @Override
  protected void write(DataOutputStream dos) throws IOException {
  }

  @Override
  protected void read(byte[] buf, int off) {
  }

  protected void read(DataInputStream dis) throws IOException {
  }

  public void onTreeBuilt(Tree tree) {
    new Message.Tree(_treeBldr.size(), tree).send();
  }

  public void onBuilderTerminated() {
  }

   
  public void onValidatorTerminated() {
  }

  public void report(String what) {
    new Message.Text(what).send();
  }

  public String nodeName() {
    return "Node" + _nodeId;
  }

 
  public void error(long error) {    
    new Message.ValidationError(error, _nrecords).send();    
  }
}
