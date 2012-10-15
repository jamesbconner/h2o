
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
  
  private final Key leftKey_;
  private final Key rightKey_;
  private final Key resultKey_;
  
  private final int leftCol_;
  private final int rightCol_;
  
  double min_ = Double.MAX_VALUE;
  double max_ = - Double.MAX_VALUE;

  /** Creates the binary operator task for the given keys. 
   * 
   * All keys must be created beforehand and they represent the left and right
   * operands and the result. They are all expected to point to ValueArrays.
   * 
   * @param left
   * @param right
   * @param result 
   */
  public MRVectorBinaryOperator(Key left, Key right, Key result, int leftCol, int rightCol) {
    leftKey_ = left;
    rightKey_ = right;
    resultKey_ = result;
    leftCol_ = leftCol;
    rightCol_ = rightCol;
  } 
  
  /** This method actually does the operation on the data itself. 
   * 
   * @param left Left operand
   * @param right Right operand
   * @return left operator right
   */
  public abstract double operator(double left, double right);
  
  @Override public void map(Key key) {
    ValueArray left_ = (ValueArray)DKV.get(leftKey_);
    ValueArray right_ = (ValueArray)DKV.get(rightKey_);
    ValueArray result_ = (ValueArray)DKV.get(resultKey_);
    
    // get the bits to which we will write
    long chunkOffset = ValueArray.getOffset(key);
    long row = chunkOffset / result_.row_size();
    long chunkRows = ValueArray.chunk_size() / result_.row_size(); // now rows per chunk
    chunkRows = Math.min(result_.num_rows() - row, chunkRows); // now how many rows we have
    byte[] bits = new byte[(int)chunkRows*8]; // create the byte array
    // now calculate the results
    long leftRow = row % left_.num_rows();
    long rightRow = row % right_.num_rows();
    for (int i = 0; i < chunkRows; ++i) {
      double left = left_.datad(leftRow,leftCol_);
      double right = right_.datad(rightRow,rightCol_);
      double result = operator(left,right);
      UDP.set8d(bits,i*8,result);
      if (result<min_)
        min_ = result;
      if (result>max_)
        max_ = result;
      leftRow = leftRow+1 % left_.num_rows(); 
      rightRow = rightRow+1 % right_.num_rows(); 
    }
    // we have the bytes now, just store the value
    Value val = new Value(key,bits);
    DKV.put(key,val);
    // and we are done...
  }

  @Override public void reduce(DRemoteTask drt) {
    // unify the min & max guys
    water.exec.MRVectorBinaryOperator other = (water.exec.MRVectorBinaryOperator) drt;
    if (other.min_ < min_)
      min_ = other.min_;
    if (other.max_ > max_)
      max_ = other.max_;
  }
}


// =============================================================================
// AddOperator
// =============================================================================

class AddOperator extends water.exec.MRVectorBinaryOperator {
  
  public AddOperator(Key left, Key right, Key result, int leftCol, int rightCol) {
    super(left,right,result,leftCol,rightCol);
  }

  @Override public double operator(double left, double right) {
    return left + right;
  }
}

// =============================================================================
// SubOperator
// =============================================================================

class SubOperator extends water.exec.MRVectorBinaryOperator {
  
  public SubOperator(Key left, Key right, Key result, int leftCol, int rightCol) {
    super(left,right,result,leftCol,rightCol);
  }

  @Override public double operator(double left, double right) {
    return left - right;
  }
}

// =============================================================================
// MulOperator
// =============================================================================

class MulOperator extends water.exec.MRVectorBinaryOperator {
  
  public MulOperator(Key left, Key right, Key result, int leftCol, int rightCol) {
    super(left,right,result,leftCol,rightCol);
  }

  @Override public double operator(double left, double right) {
    return left * right;
  }
}

// =============================================================================
// DivOperator
// =============================================================================

class DivOperator extends water.exec.MRVectorBinaryOperator {
  
  public DivOperator(Key left, Key right, Key result, int leftCol, int rightCol) {
    super(left,right,result,leftCol,rightCol);
  }

  @Override public double operator(double left, double right) {
    return left * right;
  }
}


