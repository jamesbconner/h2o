
package water.exec;

import java.util.HashMap;
import water.*;
import water.exec.Expr.Result;

/**
 *
 * @author peta
 */
public abstract class Function {
  
    public static final HashMap<String,Function> FUNCTIONS = new HashMap();
  
    public static class ArgChecker {
      public void check(Result arg) throws Exception {
        // pass
      }
    }
    
    public static class SingleColumn extends ArgChecker {
      @Override public void check(Result arg) throws Exception {
        if (arg.isConstant())
          throw new Exception("Expected single column vector, but scalar constant found.");
        if (arg.colIndex()>=0) // we have selected the arg properly
          return;
        ValueArray va = (ValueArray) DKV.get(arg._key);
        if (va.num_cols()!=1)
          throw new Exception("Expected single column vector, but "+va.num_cols()+" columns found.");
      }
    }
    
    public final ArgChecker[] _argCheckers;
    public final String _name;
    
    public Function(String name, ArgChecker[] argCheckers) {
      _argCheckers = argCheckers;
      _name = name;
      FUNCTIONS.put(name,this);
    }
    
    public int numArgs() {
      return _argCheckers.length;
    }
    
    protected abstract Result doEval(Result... args);

    public final Result eval(Result... args) throws Exception {
      assert (args.length == _argCheckers.length);
      // now check all the arguments
      for (int i = 0; i < args.length; ++i)
        try {
          _argCheckers[i].check(args[i]);
        } catch (Exception e) {
          throw new Exception("Arguent "+i+": "+e.getMessage());
        }
      // run the evaluation
      return doEval(args);
    }
    
    public static void initializeCommonFunctions() {
      new Min("min");
      new Max("max");
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
    super(name,new ArgChecker[] { new SingleColumn() });
  }

  @Override protected Result doEval(Result... args) {
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
    super(name,new ArgChecker[] { new SingleColumn() });
  }

  @Override protected Result doEval(Result... args) {
    MRMax task = new MRMax(args[0]._key, args[0].colIndex());
    task.invoke(args[0]._key);
    return Result.scalar(task.result());
  }
}
