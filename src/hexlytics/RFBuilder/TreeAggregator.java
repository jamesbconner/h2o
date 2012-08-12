
package hexlytics.RFBuilder;

import hexlytics.Tree;

import java.util.ArrayList;
import java.util.HashSet;

/** Aggregates the results from different validators as well as all the
 * trees so that they can be returned as a random forest. 
 * 
 * Is very simple and crude, single threaded at the moment so that we do not
 * have to deal with the concurrency. Also there is a way to keep a running
 * error rate without recomputing, but again, we'd have to look for concurrency
 * a lot in that case. 
 * 
 * For now, it should be enough. 
 *
 * @author peta
 */
/*public class TreeAggregator {

  // All rows in the data set, and number of error predictions for each of the
  // rows
  final int[] rowErrors; 
  
  final ArrayList<Tree> trees_ = new ArrayList();  // All trees 
  final Director glue_;  // The glue object to signal update
  
  public TreeAggregator(Director glue, int totalRows) {
    rowErrors = new int[totalRows];
    glue_ = glue;
  }
  
  public void aggregateTree(Tree tree, int[] errorRows) {
    trees_.add(tree);
    for (int i: errorRows)
      ++rowErrors[i];
    glue_.onErrorChange(-1);
  }

  public double error() {
    int maxBad = trees_.size() / 2;
    if (trees_.size() % 2 == 0)
      maxBad -= 1;
    int errors = 0;
    for (int i: rowErrors) {
      if (i > maxBad)
        ++errors;
    }
    return (double)errors/rowErrors.length;
  }
 
} */

class TreeAggregator {
  private static class Chunk {
    int[][] rows;
    
    public Chunk(int numRows, int numClasses) {
      rows = new int[numRows][numClasses];
    }
    
    public final void storeVote(int row, int vote) {
      rows[row][vote] += 1;
    }
    
    public boolean isGoodRow(int row, int numTrees) {
      if (numTrees == 0)
        return true;
      int bestBadVotes = 0;
      int badVotes = 0;
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
  
  private final HashSet<Tree> trees_ = new HashSet();
  
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
    boolean changed = false;
    // create the chunk if we haven't done so
    if (chunks_[chunkIndex] == null) {
      chunks_[chunkIndex] = new Chunk(rows,numClasses_);
      totalRows_ += rows;
      changed = true; // error rate always changes when we add new rows
    }
    Chunk chunk = chunks_[chunkIndex];
    // add all bad rows to the chunk information
    for (int i = 0; i< badRows.length; ++i) {
      chunk.storeVote(badRows[i], badVotes[i]);
    }
    glue_.onAggregatorChange();
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



