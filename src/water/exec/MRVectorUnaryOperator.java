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
    ValueArray opnd_ = (ValueArray) DKV.get(_key);
    ValueArray result_ = (ValueArray) DKV.get(_resultKey);

    // get the bits to which we will write
    long chunkOffset = ValueArray.getOffset(key);
    long row = chunkOffset / result_.row_size();
    // now if we are last chunk, number of rows is all remaining
    // otherwise it is the chunk_size() / row_size
    long chunkRows = ValueArray.chunk_size() / result_.row_size(); // now rows per chunk
    if( row / chunkRows == result_.chunks() - 1 )
      chunkRows = result_.num_rows() - row;
    byte[] bits = MemoryManager.allocateMemory((int) chunkRows * 8); // create the byte array
    // now calculate the results
    for( int i = 0; i < chunkRows; ++i ) {
      double opnd = opnd_.datad(row + i, _col);
      double result = operator(opnd);
      UDP.set8d(bits, i * 8, result);
      if( result < _min )
        _min = result;
      if( result > _max )
        _max = result;
      _tot += result;
    }
    // we have the bytes now, just store the value
    Value val = new Value(key, bits);
    DKV.put(key, val);
    // and we are done...
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
// RightAdd
// =============================================================================
/**
 * Where the arraylet operand is on the RHS of the operator with parameter.
 *
 * @author peta
 */
class RightAdd extends ParametrizedMRVectorUnaryOperator {

  public RightAdd(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return _param + opnd; }
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
// RightMul
// =============================================================================
/**
 * Where the arraylet operand is on the RHS of the operator with parameter.
 *
 * @author peta
 */
class RightMul extends ParametrizedMRVectorUnaryOperator {
  
  public RightMul(Key key, Key result, int col, double param) { super(key, result, col, param); }

  @Override
  public double operator(double opnd) { return _param * opnd; }
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
