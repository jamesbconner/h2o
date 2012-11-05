package water.exec;

import water.*;

// =============================================================================
// MRVectorUnaryOperator
// =============================================================================
/**
 * Handles the MRTask of performing a unary operator on given arraylet and
 * storing the results into the specified key.
 *
 * @author peta
 */
public abstract class MRVectorUnaryOperator extends MRTask {

  private final Key _key;
  private final Key _resultKey;
  private final int _col;
  double _min = Double.POSITIVE_INFINITY;
  double _max = Double.NEGATIVE_INFINITY;
  double _tot = 0;

  /**
   * Creates the binary operator task for the given keys.
   *
   * All keys must be created beforehand and they represent the left and right
   * operands and the result. They are all expected to point to ValueArrays.
   *
   * @param left
   * @param right
   * @param result
   */
  public MRVectorUnaryOperator(Key key, Key result, int col) {
    _key = key;
    _resultKey = result;
    _col = col;
  }

  /**
   * This method actually does the operation on the data itself.
   *
   * @param left Left operand
   * @param right Right operand
   * @return left operator right
   */
  public abstract double operator(double opnd);

  @Override
  public void map(Key key) {
    ValueArray result = (ValueArray) DKV.get(_resultKey);
    long rowOffset = ValueArray.getOffset(key) / result.row_size();
    VAIterator opnd = new VAIterator(_key,_col, rowOffset);
    int chunkRows = (int) (ValueArray.chunk_size() / result.row_size());
    if (rowOffset + chunkRows >= result.num_rows())
      chunkRows = (int) (result.num_rows() - rowOffset);
    int chunkLength = chunkRows * 8;
    byte[] bits = MemoryManager.allocateMemory(chunkLength); // create the byte array
    for (int i = 0; i < chunkLength; i+=8) {
      opnd.next();
      double x = operator(opnd.datad());
      UDP.set8d(bits,i,x);
      if (x < _min)
        _min = x;
      if (x > _max)
        _max = x;
      _tot += x;
    }
    Value val = new Value(key, bits);
    lazy_complete(DKV.put(key, val));
  }

  @Override
  public void reduce(DRemoteTask drt) {
    // unify the min & max guys
    water.exec.MRVectorUnaryOperator other = (water.exec.MRVectorUnaryOperator) drt;
    if( other._min < _min )
      _min = other._min;
    if( other._max > _max )
      _max = other._max;
    _tot += other._tot;
  }
}

// =============================================================================
// UnaryMinus
// =============================================================================
class UnaryMinus extends MRVectorUnaryOperator {

  public UnaryMinus(Key key, Key result, int col) { super(key, result, col); }

  @Override
  public double operator(double opnd) { return -opnd; }
}

// =============================================================================
// ParametrizedMRVectorUnaryOperator
// =============================================================================
/**
 * An Unary operator that holds an argument.
 *
 * @author peta
 */
abstract class ParametrizedMRVectorUnaryOperator extends MRVectorUnaryOperator {

  public final double _param;

  public ParametrizedMRVectorUnaryOperator(Key key, Key result, int col, double param) {
    super(key, result, col);
    _param = param;
  }
}

// =============================================================================
// LeftAdd
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftAdd extends ParametrizedMRVectorUnaryOperator {

  public LeftAdd(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return opnd + _param; }
}

// =============================================================================
// LeftSub
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftSub extends ParametrizedMRVectorUnaryOperator {

  public LeftSub(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return opnd - _param; }
}

// =============================================================================
// LeftMul
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftMul extends ParametrizedMRVectorUnaryOperator {

  public LeftMul(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return opnd * _param; }
}

// =============================================================================
// LeftDiv
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftDiv extends ParametrizedMRVectorUnaryOperator {

  public LeftDiv(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return opnd / _param; }
}

// =============================================================================
// LeftLess
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftLess extends ParametrizedMRVectorUnaryOperator {

  public LeftLess(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return opnd < _param ? 1 : 0; }
}

// =============================================================================
// LeftLessOrEq
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftLessOrEq extends ParametrizedMRVectorUnaryOperator {

  public LeftLessOrEq(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return opnd <= _param ? 1 : 0; }
}

// =============================================================================
// LeftGreater
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftGreater extends ParametrizedMRVectorUnaryOperator {

  public LeftGreater(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return opnd > _param ? 1 : 0; }
}

// =============================================================================
// LeftGreaterOrEq
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftGreaterOrEq extends ParametrizedMRVectorUnaryOperator {

  public LeftGreaterOrEq(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return opnd >= _param ? 1 : 0; }
}

// =============================================================================
// LeftEq
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftEq extends ParametrizedMRVectorUnaryOperator {

  public LeftEq(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return opnd == _param ? 1 : 0; }
}

// =============================================================================
// LeftNeq
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftNeq extends ParametrizedMRVectorUnaryOperator {

  public LeftNeq(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return opnd != _param ? 1 : 0; }
}

// =============================================================================
// LeftAnd
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftAnd extends ParametrizedMRVectorUnaryOperator {

  public LeftAnd(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) {
    if (opnd==0)
      return 0;
    return _param != 0 ? 1 : 0;
  }
}

// =============================================================================
// LeftOr
// =============================================================================
/**
 * Where the arraylet operand is on the LHS of the operator with parameter.
 *
 * @author peta
 */
class LeftOr extends ParametrizedMRVectorUnaryOperator {

  public LeftOr(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) {
    if (opnd!=0)
      return 1;
    return _param != 0 ? 1 : 0;
  }
}


// =============================================================================
// RightSub
// =============================================================================
/**
 * Where the arraylet operand is on the RHS of the operator with parameter.
 *
 * @author peta
 */
class RightSub extends ParametrizedMRVectorUnaryOperator {

  public RightSub(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return _param - opnd; }
}

// =============================================================================
// RightDiv
// =============================================================================
/**
 * Where the arraylet operand is on the RHS of the operator with parameter.
 *
 * @author peta
 */
class RightDiv extends ParametrizedMRVectorUnaryOperator {

  public RightDiv(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return _param / opnd; }
}

// =============================================================================
// RightAnd
// =============================================================================
/**
 * Where the arraylet operand is on the RHS of the operator with parameter.
 *
 * @author peta
 */
class RightAnd extends ParametrizedMRVectorUnaryOperator {

  public RightAnd(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) {
    if (_param==0)
      return 0;
    return opnd != 0 ? 1 : 0;
  }
}

// =============================================================================
// RightOr
// =============================================================================
/**
 * Where the arraylet operand is on the RHS of the operator with parameter.
 *
 * @author peta
 */
class RightOr extends ParametrizedMRVectorUnaryOperator {

  public RightOr(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) {
    if (_param != 0)
      return 1;
    return opnd != 0 ? 1 : 0;
  }
}
