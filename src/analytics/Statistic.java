package analytics;

/** A statistic that is capable of storing itself into the long[] arrays
 * conveniently. These long arrays are used for fast and memory efficient
 * retrieval of the statistic data by the distributed tree builder. 
 * 
 * TODO This is not thread safe yet! Locks should be implemented. 
 *
 * @author peta
 */
public abstract class Statistic {
  
  /** Adds the given row to the statistic measure.   */
  public abstract void addDataPoint(DataAdapter adapter, long[] data, int offset);
  
  /** Returns the default category for the node. Returns -1 if the current
   * statistic is not capable of computing the default category.
   * 
   * @param data
   * @param offset
   * @return 
   */
  public int defaultCategory(long[] data, int offset) {
    return -1; // this statistic cannot determine the defaultCategory for the node
  }
  
  
  /** Returns the size of the data for the statistic. This data will be reserved
   * in the long array and thus should be multiples of 8 bytes.   */
  public abstract int dataSize();
  
  /** Produces the classifier from the statistic. If the statistic has seen only
   * rows of one type, the ConstClassifier should be returned.    */ 
  public abstract Classifier createClassifier(long[] data, int offset);

  
  /** Reads the long value from given data at given offset.    */
  protected final long readLong(long[] data, int offset) {
    assert (offset % 8 == 0);
    return data[(offset) / 8];
  }
  
  /** Writes the long have to given data at given offset.    */
  protected final void writeLong(long value, long[] data, int offset) {
    assert (offset % 8 == 0);
    data[(offset) / 8] = value;
  }
  
  /** Adds the long value to already existing long value.    */
  protected final void addLong(long value, long[] data, int offset) {
    // TODO this should be atomic!!!
    assert (offset % 8 == 0);
    data[(offset) / 8] += value;
  }
  
  /** Reads a double value.    */
  protected final double readDouble(long[] data, int offset) {
    assert (offset % 8 == 0);
    return Double.longBitsToDouble(data[(offset) / 8]);
  }

  /** Writes a double value. */
  protected final void writeDouble(double value, long[] data, int offset) {
    assert (offset % 8 == 0);
    data[(offset) / 8] = Double.doubleToLongBits(value);
  }

  /** Adds double value to already stored value.    */
  protected final void addDouble(double value, long[] data, int offset) {
    // TODO this should be atomic!!!
    assert (offset % 8 == 0);
    data[(offset) / 8] = Double.doubleToLongBits(readDouble(data,offset) + value);
  }
  
  /** Reads integer value.  Integers are always packed two to a long value.    */
  protected final int readInteger(long[] data, int offset) {
    assert (offset % 4 == 0);
    if (offset % 8 == 0) 
      return (int)(readLong(data,offset) >>> 32);
    else 
      return (int)(readLong(data,offset-4) & 0x00000000ffffffffL);
  }
  
  /** Writes integer value. Integers are always packed two to a long value.    */
  protected final void writeInteger(int value, long[] data, int offset) {
    assert (offset % 4 == 0);
    // TODO this should be atomic!!!
    if (offset % 8 == 0)
      writeLong((readLong(data,offset) & 0x00000000ffffffffL) | ((long)value << 32),data,offset);
    else
      writeLong((readLong(data,offset-4) & 0xffffffff00000000L) | (value & 0x00000000ffffffffL),data,offset-4);
  } 
  
  /** Adds integer value to an integer value. Integer values are always packed
   * two to a long.    */
  protected final void addInteger(int value, long[] data, int offset) {
    assert (offset % 4 == 0); // TODO this should be atomic!!!
    writeInteger(readInteger(data,offset)+value,data,offset);
  }
  
}
