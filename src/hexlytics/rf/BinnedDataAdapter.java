/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.rf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author peta
 */
public class BinnedDataAdapter extends DataAdapter {
  
  /** Provides information about the binning of the column */
  class ColumnBinInfo {
    Set<Short> uniqueValues_ = new HashSet();
    HashMap<Short,Integer> translation_ = new HashMap();    
    int numUniqueValues_;
    
    public void addValue(short value) {
      uniqueValues_.add(value);
    }
    
    public void calculateColumnBins(int maxBins) {
      numUniqueValues_ = uniqueValues_.size();
      Object[] vals = uniqueValues_.toArray();
      Arrays.sort(vals);
      int f = numUniqueValues_ / maxBins;
      if (f == 0)
        f = 1;
      int i = 0, j = 0;
      for (Object o: vals) {
        translation_.put((Short)o,i);
        if (++j == f) {
          j = 0;
          ++i;
        }
      }
    }
    
  }  
  

  ColumnBinInfo[] binInfo_;
  
  /** Calculates the binning information for the given dataset. 
   * 
   */
  public void calculateBinning() {
    System.out.println("Calculating binning information for the dataset...");  
    binInfo_ = new ColumnBinInfo[c_.length];
    for (int i = 0; i < binInfo_.length; ++i)
      binInfo_[i] = new ColumnBinInfo();
    for (int r = 0; r < rows(); ++r)
      for (int c = 0; c < c_.length; ++c)
        binInfo_[c].addValue(data_[r * c_.length + c]);
    for (ColumnBinInfo c: binInfo_)
      c.calculateColumnBins(20);
    
    
    
    System.out.println("Done.");
  }
  
  
  public BinnedDataAdapter(String name, Object[] columns, String classNm) {
    super(name,columns,classNm);
  }

  // By default binning is not supported
  @Override public int columnClasses(int colIndex) {
    return binInfo_[colIndex].numUniqueValues_;
  }

  // by default binning is not supported
  @Override public int getColumnClass(int rowIndex, int colIndex) {
    return binInfo_[colIndex].translation_.get(getS(rowIndex,colIndex));
  }
  
  
  
}
