
package water.exec;

import java.util.ArrayList;
import java.util.HashMap;
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
  
  public class ArgScalar extends ArgCheck {

    public ArgScalar() { }
    public ArgScalar(String name) { super(name); }
    public ArgScalar(String name, double defaultValue) { super(name,defaultValue); }
    
    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtNumberLiteral)
        throw new Exception("Expected number literal");
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
  
  // ArgSingleColumn -----------------------------------------------------------
  
  public class ArgVector extends ArgCheck {
    public ArgVector() { }
    public ArgVector(String name) { super(name); }

    @Override public void checkResult(Result r) throws Exception {
      if (r._type != Result.Type.rtKey)
        throw new Exception("Expected vector (value)");
      if (r.colIndex() >= 0) // that is we are selecting single column
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
    MRMin task = new MRMin(args[0]._key, args[0].colIndex());
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
    MRMax task = new MRMax(args[0]._key, args[0].colIndex());
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
    MRSum task = new MRSum(args[0]._key, args[0].colIndex());
    task.invoke(args[0]._key);
    return Result.scalar(task.result());
  }
}

// Mean -------------------------------------------------------------------------

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
    MRMean task = new MRMean(args[0]._key, args[0].colIndex());
    task.invoke(args[0]._key);
    return Result.scalar(task.result());
  }
}
