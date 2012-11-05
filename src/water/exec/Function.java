
package water.exec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import water.*;
import water.exec.Expr.Result;

/** A class that represents the function call. 
 * 
 * Checks arguments in a proper manner using the argchecker instances and
 * executes the function. Subclasses should only override the doEval abstract
 * method.
 *
 * @author peta
 */

public abstract class Function {

  // ArgCheck ------------------------------------------------------------------
  
  public abstract class ArgCheck {
    public final String _name;
    public final Result _defaultValue;
    
    protected ArgCheck() {
      _name = null; // required
      _defaultValue = null;
    }
    
    protected ArgCheck(String name) {
      _name = name;
      _defaultValue = null;
    }
    
    protected ArgCheck(String name, double defaultValue) {
      _name = name;
      _defaultValue = Result.scalar(defaultValue);
    }

    protected ArgCheck(String name, String defaultValue) {
      _name = name;
      _defaultValue = Result.string(defaultValue);
    }
    
    
    public abstract void checkResult(Result r) throws Exception;
  }

  // ArgScalar -----------------------------------------------------------------
  
  public class ArgValue extends ArgCheck {

    public ArgValue() { }
    public ArgValue(String name) { super(name); }
    
    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtKey)
        throw new Exception("Expected value (possibly multiple columns)");
    }
  }
  
  
  // ArgScalar -----------------------------------------------------------------
  
  public class ArgScalar extends ArgCheck {

    public ArgScalar() { }
    public ArgScalar(String name) { super(name); }
    public ArgScalar(String name, double defaultValue) { super(name,defaultValue); }
    
    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtNumberLiteral)
        throw new Exception("Expected number literal");
    }
  }

  // ArgInt --------------------------------------------------------------------
  
  public class ArgInt extends ArgCheck {

    public ArgInt() { }
    public ArgInt(String name) { super(name); }
    public ArgInt(String name, long defaultValue) { super(name,defaultValue); }
    
    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtNumberLiteral)
        throw new Exception("Expected number");
      if ((long) r._const != r._const)
        throw new Exception("Expected integer number");
    }
  }

  // ArgIntPositive-------------------------------------------------------------
  
  public class ArgIntPositive extends ArgCheck {

    public ArgIntPositive() { }
    public ArgIntPositive(String name) { super(name); }
    public ArgIntPositive(String name, long defaultValue) { super(name,defaultValue); }
    
    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtNumberLiteral)
        throw new Exception("Expected number");
      if ((long) r._const != r._const)
        throw new Exception("Expected integer number");
      if (r._const < 0)
        throw new Exception("Expected positive argument");
    }
  }
  
  // ArgString -----------------------------------------------------------------
  
  public class ArgString extends ArgCheck {

    public ArgString() { }
    public ArgString(String name) { super(name); }
    public ArgString(String name, String defaultValue) { super(name,defaultValue); }
    
    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtStringLiteral)
        throw new Exception("Expected string literal");
    }
  }
  
  // ArgColIdent ---------------------------------------------------------------
  
  public class ArgColIdent extends ArgCheck {

    public ArgColIdent() { }
    public ArgColIdent(String name) { super(name); }
    public ArgColIdent(String name, String defaultValue) { super(name,defaultValue); }
    public ArgColIdent(String name, int defaultValue) { super(name,String.valueOf(defaultValue)); }

    @Override public void checkResult(Result r) throws Exception {
      if (r._type == Result.Type.rtStringLiteral)
        return;
      if (r._type == Result.Type.rtNumberLiteral)
        if (Math.ceil(r._const) == Math.floor(r._const))
          return;
      throw new Exception("String or integer expected, float found.");
    }
    
  }
  
  
  // ArgSingleColumn -----------------------------------------------------------
  
  public class ArgVector extends ArgCheck {
    public ArgVector() { }
    public ArgVector(String name) { super(name); }

    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtKey)
        throw new Exception("Expected vector (value)");
      if (r.rawColIndex() >= 0) // that is we are selecting single column
        return;
      ValueArray va = (ValueArray) DKV.get(r._key);
      if (va.num_cols()!=1)
        throw new Exception("Expected single column vector, but "+va.num_cols()+" columns found.");
    }
  }
  
  // Function implementation ---------------------------------------------------
    
  private ArrayList<ArgCheck> _argCheckers = new ArrayList();
  private HashMap<String,Integer> _argNames = new HashMap();

  public final String _name;
  
  protected void addChecker(ArgCheck checker) {
    if (checker._name!=null)
      _argNames.put(checker._name,_argCheckers.size());
    _argCheckers.add(checker);
  }

  public ArgCheck checker(int index) {
    return _argCheckers.get(index);  
  }
  
  public int numArgs() {
    return _argCheckers.size();
  }
  
  public int argIndex(String name) {
    Integer i = _argNames.get(name); 
    return i == null ? -1 : i; 
  }
  
  public Function(String name) {
    _name = name;
    assert (FUNCTIONS.get(name) == null);
    FUNCTIONS.put(name,this);
  }
  
  
  public abstract Result eval(Result... args) throws Exception;
  
  // static list of all functions 
  
  public static final HashMap<String,Function> FUNCTIONS = new HashMap();

  public static void initializeCommonFunctions() {
    new Min("min");
    new Max("max");
    new Sum("sum");
    new Mean("mean");
    new Filter("filter");
    new Slice("slice");
    new RandBitVect("randomBitVector");
  }
}

// Min -------------------------------------------------------------------------

class Min extends Function {
  
  static class MRMin extends Helpers.ScallarCollector {

    @Override protected void collect(double x) { if (x < _result) _result = x; }

    @Override protected void reduce(double x) { if (x < _result) _result = x; }
    
    public MRMin(Key k, int col) { super(k,col,Double.MAX_VALUE); }
  }
  
  public Min(String name) {
    super(name);
    addChecker(new ArgVector("src"));
  }

  @Override public Result eval(Result... args) throws Exception {
    MRMin task = new MRMin(args[0]._key, args[0].rawColIndex());
    task.invoke(args[0]._key);
    return Result.scalar(task.result());
  }
}

// Max -------------------------------------------------------------------------

class Max extends Function {
  
  static class MRMax extends Helpers.ScallarCollector {

    @Override protected void collect(double x) { if (x > _result) _result = x; }

    @Override protected void reduce(double x) { if (x > _result) _result = x; }
    
    public MRMax(Key k, int col) { super(k,col,-Double.MAX_VALUE); }
  }
  
  public Max(String name) {
    super(name);
    addChecker(new ArgVector("src"));
  }

  @Override public Result eval(Result... args) throws Exception {
    MRMax task = new MRMax(args[0]._key, args[0].rawColIndex());
    task.invoke(args[0]._key);
    return Result.scalar(task.result());
  }
}

// Sum -------------------------------------------------------------------------

class Sum extends Function {
  
  static class MRSum extends Helpers.ScallarCollector {

    @Override protected void collect(double x) { _result += x; }

    @Override protected void reduce(double x) { _result += x; }
    
    public MRSum(Key k, int col) { super(k,col,0); }
  }
  
  public Sum(String name) {
    super(name);
    addChecker(new ArgVector("src"));
  }

  @Override public Result eval(Result... args) throws Exception {
    MRSum task = new MRSum(args[0]._key, args[0].rawColIndex());
    task.invoke(args[0]._key);
    return Result.scalar(task.result());
  }
}

// Mean ------------------------------------------------------------------------

class Mean extends Function {
  
  static class MRMean extends Helpers.ScallarCollector {

    @Override protected void collect(double x) { _result += x; }

    @Override protected void reduce(double x) { _result += x; }
    
    @Override public double result() {
      ValueArray va = (ValueArray) DKV.get(_key);
      return _result / va.num_rows();
    }
    
    public MRMean(Key k, int col) { super(k,col,0); }
  }
  
  public Mean(String name) {
    super(name);
    addChecker(new ArgVector("src"));
  }

  @Override public Result eval(Result... args) throws Exception {
    MRMean task = new MRMean(args[0]._key, args[0].rawColIndex());
    task.invoke(args[0]._key);
    return Result.scalar(task.result());
  }
}

// Filter ----------------------------------------------------------------------

class Filter extends Function {
  
  public Filter(String name) {
    super(name);
    addChecker(new ArgValue("src"));
    addChecker(new ArgVector("bitVect"));
  }
  
  @Override public Result eval(Result... args) throws Exception {
    Result r = Result.temporary();
    BooleanVectorFilter filter = new BooleanVectorFilter(r._key,args[1]._key, args[1].colIndex());
    filter.invoke(args[0]._key);
    ValueArray va = (ValueArray) DKV.get(args[0]._key);
    va = VABuilder.updateRows(va, r._key, filter._filteredRows);
    DKV.put(va._key,va);
    return r;
  }
}

class Slice extends Function {

  public Slice(String name) {
    super(name);
    addChecker(new ArgValue("src"));
    addChecker(new ArgIntPositive("start"));
    addChecker(new ArgIntPositive("count",-1));
  }
  
  @Override public Result eval(Result... args) throws Exception {
    // additional arg checking
    ValueArray ary = (ValueArray) DKV.get(args[0]._key);
    long start = (long) args[1]._const;
    long length = (long) args[2]._const;
    if (start >= ary.num_rows())
      throw new Exception("Start of the slice must be withtin the source data frame.");
    if (length == -1)
      length = ary.num_rows() - start;
    if (start+length > ary.num_rows())
      throw new Exception("Start + offset is out of bounds.");
    Result r = Result.temporary();
    ValueArray va = (ValueArray) DKV.get(args[0]._key);
    va = VABuilder.updateRows(va, r._key, length);
    DKV.put(va._key,va);
    DKV.write_barrier();
    SliceFilter filter = new SliceFilter(args[0]._key,start,length);
    filter.invoke(r._key);
    assert (filter._filteredRows == length);
    return r;
  }
  
}


// RandBitVect -----------------------------------------------------------------

class RandBitVect extends Function {
  
  static class RandVectBuilder extends MRTask {

    Key _key;
    long _selected;
    long _size;
    long _createdSelected;
    
    public RandVectBuilder(Key k, long selected) {
      _key = k;
      _selected = selected;
      ValueArray va = (ValueArray) DKV.get(k);
      _size = va.length();
    }
    
    @Override public void map(Key key) {
      byte[] bits = MemoryManager.allocateMemory(VABuilder.chunkSize(key, _size));
      int rows = bits.length / 8;
      long start = ValueArray.getOffset(key) / 8;
      double expectedBefore = start * ( (double)_selected / (_size / 8));
      double expectedAfter = (start + rows) * ((double)  _selected / (_size / 8));
      int create = (int) (Math.round(expectedAfter) - Math.round(expectedBefore));
      //System.out.println("RVB: before "+ expectedBefore+" after "+expectedAfter+" to be created "+create+" on rows "+rows);
      _createdSelected += create;
      boolean[] t = new boolean[rows];
      for (int i = 0; i < create; ++i)
        t[i] = true;
      Random r = new Random();
      for (int i = 0; i < rows; ++i) {
        int j = r.nextInt(rows);
        boolean x = t[i];
        t[i] = t[j];
        t[j] = x;
      }
      int offset = 0;
      for (int i = 0; i < rows; ++i)
        offset += UDP.set8d(bits,offset, t[i] ? 1 : 0);
      DKV.put(key, new Value(key,bits));
    }

    @Override  public void reduce(DRemoteTask drt) {
      RandVectBuilder other = (RandVectBuilder) drt;
      _createdSelected += other._createdSelected;
    }
    
  }
  

  public RandBitVect(String name) {
    super(name);
    addChecker(new ArgIntPositive("size"));
    addChecker(new ArgIntPositive("selected"));
  }
  
  @Override  public Result eval(Result... args) throws Exception {
    Result r = Result.temporary();
    long size = (long) args[0]._const;
    long selected = (long) args[1]._const;
    if (selected > size) 
      throw new Exception("Number of selected rows must be smaller or equal than total number of rows for a random bit vector");
    double min = 0;
    double max = 1;
    double mean = selected / size;
    double var = Math.sqrt((1 - mean) * ( 1-mean) * selected + (mean*mean*(size-selected)) / size);
    VABuilder b = new VABuilder("",size).addDoubleColumn("bits",min,max,mean,var).createAndStore(r._key);
    RandVectBuilder rvb = new RandVectBuilder(r._key,selected);
    rvb.invoke(r._key);
    assert (rvb._createdSelected == selected) : rvb._createdSelected + " != " + selected;
    return r;
  }
  
}

// GLM -------------------------------------------------------------------------

class GLM extends Function {

  public GLM(String name) {
    super(name);
    addChecker(new ArgValue("key"));
    addChecker(new ArgColIdent("Y"));
    // no support for X
    // no support for negX
    addChecker(new ArgString("family","gaussian"));
    addChecker(new ArgScalar("xval",0));
    addChecker(new ArgScalar("threshold",0.5));
    addChecker(new ArgString("norm","NONE"));
    addChecker(new ArgScalar("lambda",0.1));
    addChecker(new ArgScalar("rho",1.0));
    addChecker(new ArgScalar("alpha",1.0));
  }

  @Override public Result eval(Result... args) throws Exception {
        
    
    throw new Exception("not implemented yet!");
  }

  
}