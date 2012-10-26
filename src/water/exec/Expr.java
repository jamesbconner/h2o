package water.exec;

import java.util.ArrayList;
import water.*;
import water.exec.RLikeParser.Token;
import water.parser.ParseDataset;

// =============================================================================
// Expression
// =============================================================================
public abstract class Expr {

  // ---------------------------------------------------------------------------
  // Result
  
  public static class Result {

    public final Key _key;
    private int _refCount;
    boolean _copied;
    private int _colIndex;
    public final double _const;

    private Result(Key k, int refCount) {
      _key = k;
      _refCount = refCount;
      _colIndex = -1;
      _const = 0;
    }

    private Result(double value) {
      _key = null;
      _refCount = 1;
      _colIndex = -1;
      _const = value;
    }

    public static Result temporary(Key k) { return new Result(k, 1); }

    public static Result temporary() { return new Result(Key.make(), 1); }

    public static Result permanent(Key k) { return new Result(k, -1); }

    public static Result scalar(double v) { return new Result(v); }

    public void dispose() {
      if( _key == null )
        return;
      --_refCount;
      if( _refCount == 0 )
        if( _copied )
          DKV.remove(_key); // remove only the array header
        else
          UKV.remove(_key);
    }

    public boolean isTemporary() { return _refCount >= 0; }

    public boolean canShallowCopy() { return false; }
    // shallow copy does not seem to be possible at the moment - arraylets are fixed to their key
    //return isTemporary() && (_copied == false);

    public int colIndex() { return _colIndex; }

    public void setColIndex(int index) { _colIndex = index; }

    public boolean isConstant() { return _key == null; }
  }

  public abstract Result eval() throws EvaluationException;
  /**
   * Position of the expression in the parsed string.
   */
  public final int _pos;

  protected Expr(int pos) { _pos = pos; }

  /**
   * Use this method to get ValueArrays as it is typechecked.
   *
   * @param k
   * @return
   * @throws EvaluationException
   */
  public ValueArray getValueArray(Key k) throws EvaluationException {
    Value v = DKV.get(k);
    if( v == null )
      throw new EvaluationException(_pos, "Key " + k.toString() + " not found");
    if( !(v instanceof ValueArray) )
      throw new EvaluationException(_pos, "Key " + k.toString() + " does not contain an array, while array is expected.");
    return (ValueArray) v;
  }
  
}

// =============================================================================
// KeyLiteral
// =============================================================================
class KeyLiteral extends Expr {

  private final Key _key;

  public KeyLiteral(int pos, String id) {
    super(pos);
    _key = Key.make(id);
  }

  @Override
  public Result eval() throws EvaluationException { return Result.permanent(_key); }
}

// =============================================================================
// FloatLiteral 
// =============================================================================
class FloatLiteral extends Expr {

  public final double _d;

  public FloatLiteral(int pos, double d) {
    super(pos);
    _d = d;
  }

  @Override
  public Expr.Result eval() throws EvaluationException { return Expr.Result.scalar(_d); }
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

  @Override
  public Result eval() throws EvaluationException {
    Result rhs = _rhs.eval();
    try {
      Helpers.assign(_pos, _lhs, rhs);
      Helpers.calculateSigma(_lhs, 0);
    } finally {
      rhs.dispose();
    }
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

  @Override
  public Result eval() throws EvaluationException {
    Result result = _expr.eval();
    ValueArray v = getValueArray(result._key);
    if( v.num_cols() <= _colIndex ) {
      result.dispose();
      throw new EvaluationException(_pos, "Column " + _colIndex + " not present in expression (has " + v.num_cols() + ")");
    }
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

  @Override
  public Result eval() throws EvaluationException {
    Result result = _expr.eval();
    ValueArray v = getValueArray(result._key);
    if( v == null ) {
      result.dispose();
      throw new EvaluationException(_pos, "Key " + result._key.toString() + " not found");
    }
    for( int i = 0; i < v.num_cols(); ++i ) {
      if( v.col_name(i).equals(_colName) ) {
        result.setColIndex(i);
        return result;
      }
    }
    result.dispose();
    throw new EvaluationException(_pos, "Column " + _colName + " not present in expression");
  }
}

// =============================================================================
// UnaryOperator
// =============================================================================
class UnaryOperator extends Expr {

  private final Expr _opnd;
  private final Token.Type _type;

  public UnaryOperator(int pos, Token.Type type, Expr opnd) {
    super(pos);
    _type = type;
    _opnd = opnd;
  }

  private Result evalConst(Result o) throws EvaluationException {
    switch( _type ) {
      case ttOpSub:
        return Result.scalar(-o._const);
      default:
        throw new EvaluationException(_pos, "Operator " + _type.toString() + " not applicable to given operand.");
    }
  }

  private Result evalVect(Result o) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray opnd = getValueArray(o._key);
    if (o.colIndex() == -1) {
      o.setColIndex(0);
      if (opnd.num_cols()!=1)
        throw new EvaluationException(_pos, "Column must be specified for the operand");
    }
    // we do not need to check the columns here - the column selector operator does this for us
    // one step ahead
    VABuilder b = new VABuilder("temp",opnd.num_rows()).addDoubleColumn("0").createAndStore(res._key);
    MRVectorUnaryOperator op;
    switch( _type ) {
      case ttOpSub:
        op = new UnaryMinus(o._key, res._key, o.colIndex());
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: " + _type.toString());
    }
    op.invoke(res._key);
    b.setColumnStats(0,op._min, op._max, op._tot / opnd.num_rows()).createAndStore(res._key).createAndStore(res._key);
    return res;
  }

  @Override
  public Result eval() throws EvaluationException {
    // get the keys and the values    
    Result op = _opnd.eval();
    try {
      if( op.isConstant() )
        return evalConst(op);
      else
        return evalVect(op);
    } finally {
      op.dispose();
    }
  }
}

// =============================================================================
// Binary Operator
// =============================================================================
class BinaryOperator extends Expr {

  private final Expr _left;
  private final Expr _right;
  private final Token.Type _type;

  public BinaryOperator(int pos, Token.Type type, Expr left, Expr right) {
    super(pos);
    _left = left;
    _right = right;
    _type = type;

  }

  private Result evalConstConst(Result l, Result r) throws EvaluationException {
    switch( _type ) {
      case ttOpAdd:
        return Result.scalar(l._const + r._const);
      case ttOpSub:
        return Result.scalar(l._const - r._const);
      case ttOpMul:
        return Result.scalar(l._const * r._const);
      case ttOpDiv:
        return Result.scalar(l._const / r._const);
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: " + _type.toString());
    }
  }

  private Result evalVectVect(Result l, Result r) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray vl = getValueArray(l._key);
    ValueArray vr = getValueArray(r._key);
    if (l.colIndex() == -1) {
      l.setColIndex(0);
      if (vl.num_cols()!=1)
        throw new EvaluationException(_pos, "Column must be specified for left operand");
    }
    if (r.colIndex() == -1) {
      r.setColIndex(0);
      if (vr.num_cols()!=1)
        throw new EvaluationException(_pos, "Column must be specified for right operand");
    }
    long resultRows = Math.max(vl.num_rows(), vr.num_rows());
    // we do not need to check the columns here - the column selector operator does this for us
    // one step ahead
    VABuilder b = new VABuilder("temp",resultRows).addDoubleColumn("0").createAndStore(res._key);
    MRVectorBinaryOperator op;
    switch( _type ) {
      case ttOpAdd:
        op = new AddOperator(l._key, r._key, res._key, l.colIndex(), r.colIndex());
        break;
      case ttOpSub:
        op = new SubOperator(l._key, r._key, res._key, l.colIndex(), r.colIndex());
        break;
      case ttOpMul:
        op = new MulOperator(l._key, r._key, res._key, l.colIndex(), r.colIndex());
        break;
      case ttOpDiv:
        op = new DivOperator(l._key, r._key, res._key, l.colIndex(), r.colIndex());
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: " + _type.toString());
    }
    op.invoke(res._key);
    b.setColumnStats(0,op._min, op._max, op._tot / resultRows).createAndStore(res._key);
    return res;
  }

  private Result evalConstVect(final Result l, Result r) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray vr = getValueArray(r._key);
    if (r.colIndex() == -1) {
      r.setColIndex(0);
      if (vr.num_cols()!=1)
        throw new EvaluationException(_pos, "Column must be specified for right operand");
    }
    MRVectorUnaryOperator op;
    switch( _type ) {
      case ttOpAdd:
        op = new RightAdd(r._key, res._key, r.colIndex(), l._const);
        break;
      case ttOpSub:
        op = new RightSub(r._key, res._key, r.colIndex(), l._const);
        break;
      case ttOpMul:
        op = new RightMul(r._key, res._key, r.colIndex(), l._const);
        break;
      case ttOpDiv:
        op = new RightDiv(r._key, res._key, r.colIndex(), l._const);
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: " + _type.toString());
    }
    VABuilder b = new VABuilder("temp",vr.num_rows()).addDoubleColumn("0").createAndStore(res._key);
    op.invoke(res._key);
    b.setColumnStats(0,op._min, op._max, op._tot / vr.num_rows()).createAndStore(res._key);
    return res;
  }

  private Result evalVectConst(Result l, final Result r) throws EvaluationException {
    Result res = Result.temporary();
    ValueArray vl = getValueArray(l._key);
    if (l.colIndex() == -1) {
      l.setColIndex(0);
      if (vl.num_cols()!=1)
        throw new EvaluationException(_pos, "Column must be specified for left operand");
    }
    MRVectorUnaryOperator op;
    switch( _type ) {
      case ttOpAdd:
        op = new LeftAdd(l._key, res._key, l.colIndex(), r._const);
        break;
      case ttOpSub:
        op = new LeftSub(l._key, res._key, l.colIndex(), r._const);
        break;
      case ttOpMul:
        op = new LeftMul(l._key, res._key, l.colIndex(), r._const);
        break;
      case ttOpDiv:
        op = new LeftDiv(l._key, res._key, l.colIndex(), r._const);
        break;
      default:
        throw new EvaluationException(_pos, "Unknown operator to be used for binary operator evaluation: " + _type.toString());
    }
    VABuilder b = new VABuilder("temp", vl.num_rows()).addDoubleColumn("0").createAndStore(res._key);
    op.invoke(res._key);
    b.setColumnStats(0,op._min, op._max, op._tot / vl.num_rows()).createAndStore(res._key);
    return res;
  }

  @Override
  public Result eval() throws EvaluationException {
    // get the keys and the values    
    Result kl = _left.eval();
    Result kr = _right.eval();
    try {
      if( kl.isConstant() ) {
        if( kr.isConstant() )
          return evalConstConst(kl, kr);
        else
          return evalConstVect(kl, kr);
      } else {
        if( kr.isConstant() )
          return evalVectConst(kl, kr);
        else
          return evalVectVect(kl, kr);
      }
    } finally {
      kl.dispose();
      kr.dispose();
    }
  }
}

// =============================================================================
// FunctionCall
// =============================================================================

class FunctionCall extends Expr {
  
  public static abstract class FunctionDefinition {
    
    
  }
  
  public final Function _function;
  
  
  private final ArrayList<Expr> _args = new ArrayList();

  public FunctionCall(int pos, String fName) throws ParserException {
    super(pos);
    _function = Function.FUNCTIONS.get(fName);
    if (_function == null)
      throw new ParserException(_pos, "Function "+fName+" not found.");
  }
  
  public void addArgument(Expr arg) {
    _args.add(arg);
  }
  
  public int numArgs() {
    return _args.size();
  }
  
  public Expr arg(int index) {
    return _args.get(index);
  }
  
  @Override public Result eval() throws EvaluationException {
    Result[] args = new Result[_args.size()];
    for (int i = 0; i < args.length; ++i)
      args[i] = _args.get(i).eval();
    try {
      return _function.eval(args);      
    } catch (Exception e) {
      throw new EvaluationException(_pos, e.getMessage());
    }
  }
  
}