package water.exec;
import java.io.IOException;
import water.*;
import water.parser.ParseDataset;
import java.util.Arrays;
import water.exec.RLikeParser.Token;


// =============================================================================
// Expression
// =============================================================================

public abstract class Expr {
  public static class Result {
    public final Key _key;
    private int _refCount;
    private boolean _copied;
    private int _colIndex;
    
    private Result(Key k, int refCount) {
      _key = k;
      _refCount = refCount;
      _colIndex = 0;
    }
    
    public static Result temporary(Key k) {
      return new Result(k, 1);
    }
    
    public static Result temporary() {
      return new Result(Key.make(),1);
    }
    
    public static Result permanent(Key k) {
      return new Result(k, -1);
    }
    
    public void dispose() {
      --_refCount;
      if (_refCount == 0) 
        if (_copied)
          DKV.remove(_key); // remove only the array header
        else   
          UKV.remove(_key);
    }
    
    public boolean isTemporary() {
      return _refCount>=0; 
    }
    
    public boolean canShallowCopy() {
      return false; // shallow copy does not seem to be possible at the moment - arraylets are fixed to their key
      //return isTemporary() && (_copied == false);
    }
    
    public int colIndex() {
      return _colIndex;
    }
    
    public void setColIndex(int index) {
      _colIndex = index;
    }
    
  }

  
  public abstract Result eval() throws EvaluationException;
  
  /** Use this method to get ValueArrays as it is typechecked. 
   * 
   * @param k
   * @return
   * @throws EvaluationException 
   */
  public static ValueArray getValueArray(Key k) throws EvaluationException {
    Value v = DKV.get(k);
    if (v == null)
      throw new EvaluationException("Key "+k.toString()+" not found");
    if (!(v instanceof ValueArray))
      throw new EvaluationException("Key "+k.toString()+" does not contain an array, while array is expected.");
    return (ValueArray) v;
  }
  
  
  /** Assigns (copies) the what argument to the given key. 
   * 
   * TODO at the moment, only does deep copy. 
   * 
   * @param to
   * @param what
   * @throws EvaluationException 
   */
  public static void assign(final Key to, Result what) throws EvaluationException {
    if (what.canShallowCopy()) {
      assert (false); // we do not support shallow copy now (TODO)
      ValueArray v = (ValueArray) DKV.get(what._key);
      if (v == null)
        throw new EvaluationException("Key "+what._key+" not found");
      byte[] bits = v.get();
      ValueArray r = new ValueArray(to,MemoryManager.arrayCopyOfRange(bits, 0, bits.length)); // we must copy it because of the memory managed
      DKV.put(to,r);
      what._copied = true; // TODO do we need to sync this? 
    } else {
      ValueArray v = (ValueArray) DKV.get(what._key);
      byte[] bits = v.get();
      ValueArray r = new ValueArray(to,MemoryManager.arrayCopyOfRange(bits, 0, bits.length)); // we must copy it because of the memory managed
      DKV.put(to,r);
      MRTask copyTask = new MRTask() {
        @Override
        public void map(Key key) {
          byte[] bits = DKV.get(key).get();
          long offset = ValueArray.getOffset(key);
          Key k = ValueArray.make_chunkkey(to, offset);
          Value v = new Value(k,MemoryManager.arrayCopyOfRange(bits, 0, bits.length));
          DKV.put(k,v);
        }
        @Override public void reduce(DRemoteTask drt) {
          // pass
        }
      } ;
      copyTask.invoke(what._key);
    }
  }
  
}

// =============================================================================
// KeyLiteral
// =============================================================================

class KeyLiteral extends Expr {
  private final Key key_;
  public KeyLiteral(String id) {
    key_ = Key.make(id);
  }
  
  @Override public Result eval() throws EvaluationException {
    return Result.permanent(key_);
  }
}

// =============================================================================
// FloatLiteral 
// =============================================================================

class FloatLiteral extends Expr {
  public static final ValueArray.Column C = new ValueArray.Column();
  public static final ValueArray.Column[] CC = new ValueArray.Column[]{C};
  static {
    C._name = "";
    C._scale = 1;
    C._size = -8;
    C._domain = new ParseDataset.ColumnDomain();
    C._domain.kill();
  }
  public final double _d;
  public FloatLiteral( double d ) { _d=d; }

  @Override public Expr.Result eval() throws EvaluationException {
    Expr.Result res = Expr.Result.temporary();
    // The 1 tiny arraylet
    Key key2 = ValueArray.make_chunkkey(res._key,0);
    byte[] bits = new byte[8];
    UDP.set8d(bits,0,_d);
    Value val = new Value(key2,bits);
    DKV.put(key2,val);

    // The metadata
    C._min = C._max = _d;
    ValueArray ary = ValueArray.make(res._key,Value.ICE,res._key,Double.toString(_d),1,8,CC);
    DKV.put(res._key,ary);
    return res;
  }
}


// =============================================================================
// AssignmentOperator 
// =============================================================================

class AssignmentOperator extends Expr {

  private final Key _lhs;
  private final Expr _rhs;
  
  public AssignmentOperator(Key lhs, Expr rhs) {
    _lhs = lhs;
    _rhs = rhs;
  }
  
  @Override public Result eval() throws EvaluationException {
    Result rhs = _rhs.eval();
    Expr.assign(_lhs,rhs);
    rhs.dispose();
    return Result.permanent(_lhs);
  }
}

// =============================================================================
// ColumnSelector
// =============================================================================

class ColumnSelector extends Expr {
  private final Expr _expr;
  private final int _colIndex;
  
  public ColumnSelector(Expr expr, int colIndex) {
    _expr = expr;
    _colIndex = colIndex;
  }

  @Override public Result eval() throws EvaluationException {
    Result result = _expr.eval();
    ValueArray v = getValueArray(result._key);
    if (v.num_cols() <= _colIndex)
      throw new EvaluationException("Column "+_colIndex+" not present in expression (has "+v.num_cols()+")");
    result.setColIndex(_colIndex);
    return result;
  }
}

// =============================================================================
// StringColumnSelector
// =============================================================================

class StringColumnSelector extends Expr {
  private final Expr _expr;
  private final String _colName;
  
  public StringColumnSelector(Expr expr, String colName) {
    _expr = expr;
    _colName = colName;
  }

  @Override public Result eval() throws EvaluationException {
    Result result = _expr.eval();
    ValueArray v = getValueArray(result._key);
    for (int i = 0; i < v.num_cols(); ++i) {
      if (v.col_name(i).equals(_colName)) {
        result.setColIndex(i);
        return result;        
      }
    }
    throw new EvaluationException("Column "+_colName+" not present in expression");
  }
  
}


// =============================================================================
// Binary Operator
// =============================================================================

class BinaryOperator extends Expr {

  public static final ValueArray.Column C = new ValueArray.Column();
  public static final ValueArray.Column[] CC = new ValueArray.Column[]{C};
  static {
    C._name = "";
    C._scale = 1;
    C._size = -8;
    C._domain = new ParseDataset.ColumnDomain();
    C._domain.kill();
  }
  
  private final Expr left_;
  private final Expr right_;
  private final Token.Type type_;
  
  
  public BinaryOperator(Token.Type type, Expr left, Expr right) {
    left_ = left;
    right_ = right;
    type_ = type;
    
  }
  
  @Override public Result eval() throws EvaluationException {
    Result res = Result.temporary();
    // get the keys and the values    
    Result kl = left_.eval();
    Result kr = right_.eval();
    ValueArray vl = getValueArray(kl._key);
    ValueArray vr = getValueArray(kr._key);
    // now do the typechecking on them
    if (vl.num_rows() != vr.num_rows())
      throw new EvaluationException("Left and right arguments do not have matching row sizes");
    // we do not need to check the columns here - the column selector operator does this for us
    // one step ahead
    ValueArray result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",vl.num_rows(),8,CC);
    DKV.put(res._key,result);
    MRVectorBinaryOperator op;
    switch (type_) {
      case ttOpAdd:
        op = new AddOperator(kl._key,kr._key,res._key,0,0);
        break;
      case ttOpSub:
        op = new SubOperator(kl._key,kr._key,res._key,0,0);
        break;
      case ttOpMul:
        op = new MulOperator(kl._key,kr._key,res._key,0,0);
        break;
      case ttOpDiv:
        op = new DivOperator(kl._key,kr._key,res._key,0,0);
        break;
      default:
        throw new EvaluationException("Unknown operator to be used for binary operator evaluation: "+type_.toString());
    }
    op.invoke(res._key);
    C._min = op.min_;
    C._max = op.max_;
    result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",vl.num_rows(),8,CC);
    DKV.put(res._key,result); // reinsert with min / max
    kl.dispose();
    kr.dispose();
    return res;
  }
  
}


