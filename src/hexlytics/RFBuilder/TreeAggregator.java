package hexlytics.RFBuilder;

import hexlytics.Tree;

import java.util.ArrayList;

/** Aggregates results from the validators and builders. The aggregate results can then
 * be reported to the user.
 *  @author peta
 */
class TreeAggregator {

  public void onReport(String s) { System.out.println(s); }
  
  /** We are done... print the error rate...*/
  public void terminate() { } 
  
  
  /**A chunk holds the data returned by one validator node. */
  private static class Chunk {
    int[][] rows;
    
    public Chunk(int numRows, int numClasses) { rows = new int[numRows][numClasses];  }
    
    public final void storeVote(int row, int vote) {
      rows[row][vote] += 1;
    }
    
    public boolean isGoodRow(int row, int numTrees) {
      if (numTrees == 0)  return true;
      int bestBadVotes = 0, badVotes = 0;
      for (int i : rows[row]) {
        if (bestBadVotes < i)
          bestBadVotes = i;
        badVotes += i;
      }
      return (numTrees-badVotes) > bestBadVotes;
    }
  }
  
  private int totalRows_ = 0;
  private final int numClasses_;
  private final Chunk[] chunks_;
  private final ArrayList<Tree> trees_ = new ArrayList(); // no need to be a set, since there is no real EQ method on trees
  private final Director glue_;
  
  
  public TreeAggregator(int numChunks, int numClasses, Director glue) {
    numClasses_ = numClasses;
    chunks_ = new Chunk[numChunks];
    glue_ = glue;
  }
  
  public void aggregate(int chunkIndex, Tree tree, int rows, int[] badRows, int[] badVotes) {
    // add the tree if we haven't seen it before
    trees_.add(tree);
    // nothing to do if the tree is correct
    if (badRows==null)
      return;
    // create the chunk if we haven't done so
    if (chunks_[chunkIndex] == null) {
      chunks_[chunkIndex] = new Chunk(rows,numClasses_);
      totalRows_ += rows;
    }
    Chunk chunk = chunks_[chunkIndex];
    // add all bad rows to the chunk information
    for (int i = 0; i< badRows.length; ++i) {
      chunk.storeVote(badRows[i], badVotes[i]);
    }
  }
  
  public double getError() {
    int bad = 0;
    for (Chunk c : chunks_) {
      if (c==null) 
        continue;
      for (int i = 0; i< c.rows[0].length; ++i)
        if (!c.isGoodRow(i, trees_.size())) {
          ++bad;
          System.out.println("Row "+i+" is bad");
        }
    }
    if (totalRows_ == 0)
      return -1;
    return (double)bad / totalRows_; 
  }  
}