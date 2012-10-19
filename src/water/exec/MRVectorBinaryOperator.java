package water.exec;

import water.*;

// =============================================================================
// MRVectorBinaryOperator
// =============================================================================
/**
 *
 * @author peta
 */
public abstract class MRVectorBinaryOperator extends MRTask {

  private final Key _leftKey;
  private final Key _rightKe;
  private final Key _resultKey;
  private final int _leftCol;
  private final int _rightCol;
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
  public MRVectorBinaryOperator(Key left, Key right, Key result, int leftCol, int rightCol) {
    _leftKey = left;
    _rightKe = right;
    _resultKey = result;
    _leftCol = leftCol;
    _rightCol = rightCol;
  }

  /**
   * This method actually does the operation on the data itself.
   *
   * @param left Left operand
   * @param right Right operand
   * @return left operator right
   */
  public abstract double operator(double left, double right);

  @Override
  public void map(Key key) {
    ValueArray left_ = (ValueArray) DKV.get(_leftKey);
    ValueArray right_ = (ValueArray) DKV.get(_rightKe);
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
    long leftRow = row % left_.num_rows();
    long rightRow = row % right_.num_rows();
    for( int i = 0; i < chunkRows; ++i ) {
      double left = left_.datad(leftRow, _leftCol);
      double right = right_.datad(rightRow, _rightCol);
      double result = operator(left, right);
      UDP.set8d(bits, i * 8, result);
      if( result < _min )
        _min = result;
      if( result > _max )
        _max = result;
      _tot += result;
      leftRow = (leftRow + 1) % left_.num_rows();
      rightRow = (rightRow + 1) % right_.num_rows();
    }
    // we have the bytes now, just store the value
    Value val = new Value(key, bits);
    lazy_complete(DKV.put(key, val));
    // and we are done...
  }

  @Override
  public void reduce(DRemoteTask drt) {
    // unify the min & max guys
    water.exec.MRVectorBinaryOperator other = (water.exec.MRVectorBinaryOperator) drt;
    if( other._min < _min )
      _min = other._min;
    if( other._max > _max )
      _max = other._max;
    _tot += other._tot;
  }
}

// =============================================================================
// AddOperator
// =============================================================================
class AddOperator extends water.exec.MRVectorBinaryOperator {

  public AddOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left + right; }
}

// =============================================================================
// SubOperator
// =============================================================================
class SubOperator extends water.exec.MRVectorBinaryOperator {

  public SubOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left - right; }
}

// =============================================================================
// MulOperator
// =============================================================================
class MulOperator extends water.exec.MRVectorBinaryOperator {

  public MulOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left * right; }
}

// =============================================================================
// DivOperator
// =============================================================================
class DivOperator extends water.exec.MRVectorBinaryOperator {

  public DivOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left / right; }
}
