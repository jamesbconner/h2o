package water.exec;
import water.*;
import water.exec.RLikeParser.Token;
import water.parser.ParseDataset;


// =============================================================================
// Expression
// =============================================================================

public abstract class Expr {
  // TODO these might go someplace else in the future, or get completely rid of
  public static final ValueArray.Column C = new ValueArray.Column();
  public static final ValueArray.Column[] CC = new ValueArray.Column[]{C};
  static {
    C._name = "";
    C._scale = 1;
    C._size = -8;
    C._domain = new ParseDataset.ColumnDomain();
    C._domain.kill();
  }

  public static class Result {
    public final Key _key;
    private int _refCount;
    private boolean _copied;
    private int _colIndex;
    public final double _const;
    
    private Result(Key k, int refCount) {
      _key = k;
      _refCount = refCount;
      _colIndex = 0;
      _const = 0;
    }
    
    private Result(double value) {
      _key = null;
      _refCount = 1;
      _colIndex = 0;
      _const = value;
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
    
    public static Result scalar(double v) {
      return new Result(v);
    }
    
    
    public void dispose() {
      if (_key == null)
        return;
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
    
    public boolean isConstant() {
      return _key == null;
    }
    
  }

  
  public abstract Result eval() throws EvaluationException;

  /** Position of the expression in the parsed string.
   */
  public final int _pos;

  protected Expr(int pos) {
    _pos = pos;
  }
  
  /** Use this method to get ValueArrays as it is typechecked. 
   * 
   * @param k
   * @return
   * @throws EvaluationException 
   */
  public ValueArray getValueArray(Key k) throws EvaluationException {
    Value v = DKV.get(k);
    if (v == null)
      throw new EvaluationException(_pos, "Key "+k.toString()+" not found");
    if (!(v instanceof ValueArray))
      throw new EvaluationException(_pos, "Key "+k.toString()+" does not contain an array, while array is expected.");
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
  public static void assign(int pos, final Key to, Result what) throws EvaluationException {
    if (what.isConstant()) { // assigning to a constant creates a vector of size 1 
      // The 1 tiny arraylet
      Key key2 = ValueArray.make_chunkkey(to,0);
      byte[] bits = new byte[8];
      UDP.set8d(bits,0,what._const);
      Value val = new Value(key2,bits);
      DKV.put(key2,val);
      // The metadata
      C._min = C._max = C._mean = what._const;
      C._sigma = 0;
      ValueArray ary = ValueArray.make(to,Value.ICE,to,Double.toString(what._const),1,8,CC);
      DKV.put(to,ary);
    } else if (what.canShallowCopy()) {
      assert (false); // we do not support shallow copy now (TODO)
      ValueArray v = (ValueArray) DKV.get(what._key);
      if (v == null)
        throw new EvaluationException(pos, "Key "+what._key+" not found");
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
  public KeyLiteral(int pos, String id) {
    super(pos);
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
  public final double _d;
  public FloatLiteral(int pos, double d ) {
    super(pos);
    _d=d;
  }

  @Override public Expr.Result eval() throws EvaluationException {
    return Expr.Result.scalar(_d);
  }
}


// =============================================================================
// AssignmentOperator 
// =============================================================================

class AssignmentOperator extends Expr {

  private final Key _lhs;
  private final Expr _rhs;
  
  public AssignmentOperator(int pos, Key lhs, Expr rhs) {
    super(pos);
    _lhs = lhs;
    _rhs = rhs;
  }
  
  @Override public Result eval() throws EvaluationException {
    Result rhs = _rhs.eval();
    Expr.assign(_pos, _lhs,rhs);
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
  
  public ColumnSelector(int pos, Expr expr, int colIndex) {
    super(pos);
    _expr = expr;
    _colIndex = colIndex;
  }

  @Override public Result eval() throws EvaluationException {
    Result result = _expr.eval();
    ValueArray v = getValueArray(result._key);
    if (v.num_cols() <= _colIndex)
      throw new EvaluationException(_pos, "Column "+_colIndex+" not present in expression (has "+v.num_cols()+")");
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
  
  public StringColumnSelector(int pos, Expr expr, String colName) {
    super(pos);
    _expr = expr;
    _colName = colName;
  }

  @Override public Result eval() throws EvaluationException {
    Result result = _expr.eval();
    ValueArray v = getValueArray(result._key);
    if (v==null) 
      throw new EvaluationException(_pos,"Key "+result._key.toString()+" not found");
    for (int i = 0; i < v.num_cols(); ++i) {
      if (v.col_name(i).equals(_colName)) {
        result.setColIndex(i);
        return result;        
      }
    }
    throw new EvaluationException(_pos, "Column "+_colName+" not present in expression");
  }
  
}

// =============================================================================
// UnaryOperator
// =============================================================================

class UnaryOperator extends Expr {
  private final Expr opnd_;
  private final Token.Type type_;
  
  public UnaryOperator(int pos, Token.Type type, Expr opnd) {
    super(pos);
    type_ = type;
    opnd_ = opnd;  
  }
  
  private Result evalConst(Result o) throws EvaluationException {
    switch (type_) {
      case ttOpSub:
        return Result.scalar(-o._const);
      default:
        throw new EvaluationException(_pos, "Operator "+type_.toString()+" not applicable to given operand.");
    }    
  }
  
  private Result evalVect(Result o) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray opnd = getValueArray(o._key);
    // we do not need to check the columns here - the column selector operator does this for us
    // one step ahead
    ValueArray result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",opnd.num_rows(),8,CC);
    DKV.put(res._key,result);
    MRVectorUnaryOperator op;
    switch (type_) {
      case ttOpSub:
        op = new UnaryMinus(o._key,res._key,o.colIndex());
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: "+type_.toString());
    }
    op.invoke(res._key);
    C._min = op.min_;
    C._max = op.max_;
    C._mean = op.tot_ / opnd.num_rows();
    result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",opnd.num_rows(),8,CC);
    DKV.put(res._key,result); // reinsert with min / max
    o.dispose();
    return res;
  }

  @Override public Result eval() throws EvaluationException {
    // get the keys and the values    
    Result op = opnd_.eval();
    if (op.isConstant())
      return evalConst(op);
    else
      return evalVect(op);
  }
  
}



// =============================================================================
// Binary Operator
// =============================================================================

class BinaryOperator extends Expr {
  
  private final Expr left_;
  private final Expr right_;
  private final Token.Type type_;
  
  
  public BinaryOperator(int pos, Token.Type type, Expr left, Expr right) {
    super(pos);
    left_ = left;
    right_ = right;
    type_ = type;
    
  }

  private Result evalConstConst(Result l, Result r) throws EvaluationException {
    switch (type_) {
      case ttOpAdd:
        return Result.scalar(l._const + r._const);
      case ttOpSub:
        return Result.scalar(l._const - r._const);
      case ttOpMul:
        return Result.scalar(l._const * r._const);
      case ttOpDiv:
        return Result.scalar(l._const / r._const);
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: "+type_.toString());
    }
  }
  
  private Result evalVectVect(Result l, Result r) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray vl = getValueArray(l._key);
    ValueArray vr = getValueArray(r._key);
    long resultRows = Math.max(vl.num_rows(), vr.num_rows());
    // we do not need to check the columns here - the column selector operator does this for us
    // one step ahead
    ValueArray result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",resultRows,8,CC);
    DKV.put(res._key,result);
    MRVectorBinaryOperator op;
    switch (type_) {
      case ttOpAdd:
        op = new AddOperator(l._key,r._key,res._key,l.colIndex(),r.colIndex());
        break;
      case ttOpSub:
        op = new SubOperator(l._key,r._key,res._key,l.colIndex(),r.colIndex());
        break;
      case ttOpMul:
        op = new MulOperator(l._key,r._key,res._key,l.colIndex(),r.colIndex());
        break;
      case ttOpDiv:
        op = new DivOperator(l._key,r._key,res._key,l.colIndex(),r.colIndex());
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: "+type_.toString());
    }
    op.invoke(res._key);
    C._min = op.min_;
    C._max = op.max_;
    C._mean = op.tot_ / resultRows;
    result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",resultRows,8,CC);
    DKV.put(res._key,result); // reinsert with min / max
    l.dispose();
    r.dispose();
    return res;
  }

  private Result evalConstVect(final Result l, Result r) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray vr = getValueArray(r._key);
    MRVectorUnaryOperator op;
    switch (type_) {
      case ttOpAdd:
        op = new RightAdd(r._key,res._key,r.colIndex(),l._const);
        break;
      case ttOpSub:
        op = new RightSub(r._key,res._key,r.colIndex(),l._const);
        break;
      case ttOpMul:
        op = new RightMul(r._key,res._key,r.colIndex(),l._const);
        break;
      case ttOpDiv:
        op = new RightDiv(r._key,res._key,r.colIndex(),l._const);
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: "+type_.toString());
    }
    ValueArray result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",vr.num_rows(),8,CC);
    DKV.put(res._key,result);
    op.invoke(res._key);
    C._min = op.min_;
    C._max = op.max_;
    C._mean = op.tot_ / vr.num_rows();
    result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",vr.num_rows(),8,CC);
    DKV.put(res._key,result); // reinsert with min / max
    l.dispose();
    r.dispose();
    return res;
  }

  private Result evalVectConst(Result l, final Result r) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray vl = getValueArray(l._key);
    MRVectorUnaryOperator op;
    switch (type_) {
      case ttOpAdd:
        op = new LeftAdd(l._key,res._key,l.colIndex(),r._const);
        break;
      case ttOpSub:
        op = new LeftSub(l._key,res._key,l.colIndex(),r._const);
        break;
      case ttOpMul:
        op = new LeftMul(l._key,res._key,l.colIndex(),r._const);
        break;
      case ttOpDiv:
        op = new LeftDiv(l._key,res._key,l.colIndex(),r._const);
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: "+type_.toString());
    }
    ValueArray result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",vl.num_rows(),8,CC);
    DKV.put(res._key,result);
    op.invoke(res._key);
    C._min = op.min_;
    C._max = op.max_;
    C._mean = op.tot_ / vl.num_rows();
    result = ValueArray.make(res._key,Value.ICE,res._key,"temp result",vl.num_rows(),8,CC);
    DKV.put(res._key,result); // reinsert with min / max
    l.dispose();
    r.dispose();
    return res;
  }
  
  
  @Override public Result eval() throws EvaluationException {
    // get the keys and the values    
    Result kl = left_.eval();
    Result kr = right_.eval();
    if (kl.isConstant()) {
      if (kr.isConstant())
        return evalConstConst(kl, kr);
      else
        return evalConstVect(kl, kr);
    } else {
      if (kr.isConstant())
        return evalVectConst(kl, kr);
      else 
        return evalVectVect(kl, kr);
    }
  }
  
}


