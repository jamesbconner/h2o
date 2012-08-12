/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

import hexlytics.RandomForest;
import hexlytics.Tree;
import hexlytics.data.Data;

import java.util.ArrayList;

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
public class Aggregator {

  // All rows in the data set, and number of error predictions for each of the
  // rows
  final int[] rowErrors; 
  
  final ArrayList<Tree> trees_ = new ArrayList();  // All trees 
  final AggregatorGlue glue_;  // The glue object to signal update
  
  public Aggregator(Data data, AggregatorGlue glue) {
    rowErrors = new int[data.rows()];
    glue_ = glue;
  }
  
  /** Aggregates the information gained from the given tree. */
  public void aggregateTree(Tree tree, int[] errorRows) {
    trees_.add(tree);
    for (int i: errorRows)
      ++rowErrors[i];
    glue_.onChange();
  }

  /** Computes the forest we have at the moment. */
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
  
  /** Returns the forest created by all the trees aggregated so far. */
  public RandomForest createForest() {
    RandomForest result = new RandomForest(null,0);
    result.addAll(trees_);
    return result;
  }  
}
