package hexlytics.RFBuilder;

import hexlytics.RandomForest;
import hexlytics.Tree;
import hexlytics.data.Data;
import hexlytics.data.Data.Row;

/**
 *
 * @author peta
 */
/*public class TreeValidator {
  
  final Director glue_;
  RandomForest rf_;
  
  public TreeValidator(Data data, Director glue) {
    glue_ = glue;
    rf_= new RandomForest(data,glue_,Integer.MAX_VALUE);
  }
  
  public double validateTree(Tree tree) { return rf_.validate(tree); }
      
} */


/** Is capable of validating a given tree against given data. 
 * 
 * Similar to the TreeBuilder can run in multiple threads, but does not have to 
 * 
 * @author peta
 */
class TreeValidator {
  private class Score {
    public int[][] badVotes;
    public int size;
    public Score(int rows) {
      badVotes = new int[2][rows]; // row number and vote
    }
    public void reset() {
      size = 0;
    }
    
    public void record(int row, int vote) {
      badVotes[0][size] = row;
      badVotes[0][size] = vote;
      ++size;
    }
    
    public int[] getBadRows() {
      if (size==0)
        return null;
      int[] result = new int[size];
      System.arraycopy(badVotes[0],0,result,0,result.length);
      return result;
    }
    
    public int[] getBadVotes() {
      if (size==0)
        return null;
      int[] result = new int[size];
      System.arraycopy(badVotes[1],0,result,0,result.length);
      return result;
    }
  }
  
  private ThreadLocal<Score> score_ = new ThreadLocal<>();

  private final Data data_;
  private final Director glue_;
  
  public TreeValidator(Data data, Director glue) {
    data_ = data;
    glue_ = glue;
  }
   
  public double validate(Tree tree) {
    if (score_.get() == null)
      score_.set(new Score(data_.rows()));
    Score score = score_.get();
    score.reset();
    for (Row r: data_) {
      int result = tree.classify(r);
      if (result!=r.classOf())
        score.record(data_.originalIndex(r.index),result);
    }
    int[] badRows = score.getBadRows();
    int[] badVotes = score.getBadVotes();
    glue_.onTreeValidated(tree,badRows,badVotes);
    return (double)score.size / data_.rows();
  }
  
  
  
}
