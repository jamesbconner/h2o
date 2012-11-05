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
  private final Key _rightKey;
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
    _rightKey = right;
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

  
  /* We are creating new one, so I can tell the row number quite easily from the
   * chunk index. 
   */
  @Override public void map(Key key) {
    ValueArray result = (ValueArray) DKV.get(_resultKey);
    long rowOffset = ValueArray.getOffset(key) / result.row_size();
    VAIterator left = new VAIterator(_leftKey,_leftCol, rowOffset);
    VAIterator right = new VAIterator(_rightKey,_rightCol, rowOffset);
    int chunkRows = (int) (ValueArray.chunk_size() / result.row_size());
    if (rowOffset + chunkRows >= result.num_rows())
      chunkRows = (int) (result.num_rows() - rowOffset);
    int chunkLength = chunkRows * 8;
    byte[] bits = MemoryManager.allocateMemory(chunkLength); // create the byte array
    for (int i = 0; i < chunkLength; i+=8) {
      left.next();
      right.next();
      double x = operator(left.datad(), right.datad());
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

// =============================================================================
// ModOperator
// =============================================================================
class ModOperator extends water.exec.MRVectorBinaryOperator {

  public ModOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left % right; }
}

// =============================================================================
// LessOperator
// =============================================================================
class LessOperator extends water.exec.MRVectorBinaryOperator {

  public LessOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left < right ? 1 : 0; }
}

// =============================================================================
// LessOrEqOperator
// =============================================================================
class LessOrEqOperator extends water.exec.MRVectorBinaryOperator {

  public LessOrEqOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left <= right ? 1 : 0; }
}

// =============================================================================
// GreaterOperator
// =============================================================================
class GreaterOperator extends water.exec.MRVectorBinaryOperator {

  public GreaterOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left > right ? 1 : 0; }
}

// =============================================================================
// GreaterOrEqOperator
// =============================================================================
class GreaterOrEqOperator extends water.exec.MRVectorBinaryOperator {

  public GreaterOrEqOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left >= right ? 1 : 0; }
}

// =============================================================================
// EqOperator
// =============================================================================
class EqOperator extends water.exec.MRVectorBinaryOperator {

  public EqOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left == right ? 1 : 0; }
}

// =============================================================================
// NeqOperator
// =============================================================================
class NeqOperator extends water.exec.MRVectorBinaryOperator {

  public NeqOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return left != right ? 1 : 0; }
}

// =============================================================================
// AndOperator
// =============================================================================
class AndOperator extends water.exec.MRVectorBinaryOperator {

  public AndOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return ((left != 0) &&  (right != 0)) ? 1 : 0; }
}

// =============================================================================
// OrOperator
// =============================================================================
class OrOperator extends water.exec.MRVectorBinaryOperator {

  public OrOperator(Key left, Key right, Key result, int leftCol, int rightCol) { super(left, right, result, leftCol, rightCol); }

  @Override
  public double operator(double left, double right) { return ((left != 0) || (right != 0)) ? 1 : 0; }
}
