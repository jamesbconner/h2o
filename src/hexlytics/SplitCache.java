/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics;

import hexlytics.Statistic.Column;
import hexlytics.Statistic.Split;

/**
 *
 * @author peta
 */
public class SplitCache {

  static class Node {
    
    double[] splitValue_;
    double[] fitness_;
    
    int classOf_;
    
    // Each node has a rather lot of children. For each winning column, there
    // are number of classes (2 for the binary tree) child nodes. 
    Node[] children_;
    
    public final int columns() {
      return fitness_.length;
    }
    
    public final Node getChild(int decissionColumn, int childIndex) {
      return children_[childIndex * columns() + decissionColumn];
    }
    
    public final Node getOrCreateChild(int decissionColumn, int childIndex) {
      if (children_[childIndex * columns() + decissionColumn] == null)
        children_[childIndex * columns() + decissionColumn] = new Node(columns(), children_.length / columns());
      return getChild(decissionColumn, childIndex);
    }
  
    public final Split getSplit(int column) {
      if (fitness_[column] == 0)
        return null;
      return new Split(column, splitValue_[column],fitness_[column]);
    }
    
    public final void storeSplit(Split split) {
      assert(fitness_[split.column] == 0);
      fitness_[split.column] = split.fitness;
      splitValue_[split.column] = split.value;
    }
    
    public Node(int columns, int classesOf) {
      splitValue_ = new double[columns];
      fitness_ = new double[columns];
      children_ = new Node[columns * classesOf];
      classOf_ = -1;
    }
    
    public Node(int classOf) {
      splitValue_ = null;
      fitness_ = null;
      children_ = null;
      classOf_ = classOf;
    }
    
    public void setClassOf(int classOf) {
      splitValue_ = null;
      fitness_ = null;
      children_ = null;
      classOf_ = classOf;
    }
    
  }
  
  private final Node current_;
  public final int depth;
  
  public SplitCache(int columns, int classesOf) {
    current_ = new Node(columns, classesOf);
    depth = 1;
  }
  
  private SplitCache(SplitCache from, Node current) {
    depth = from.depth + 1;
    current_ = current;
  }
  
  public final int classOf() {
    if (current_ == null) 
      return -1;
    return current_.classOf_;
  }
  
  public final void setClassOf(int classOf) {
    if (current_ == null)
      return;
    current_.setClassOf(classOf);
  }

  public final Split getSplit(int column) {
    if (current_ == null) 
      return null;
    return current_.getSplit(column);
  }
  
  public void storeSplit(Split split) {
    if (current_ == null)
      return;
    current_.storeSplit(split);
  }
  
  public final SplitCache child(int decissionColumn, int childIndex) {
    if (depth<1)
      return new SplitCache(this, current_.getOrCreateChild(decissionColumn,childIndex));
    else 
      return new SplitCache(this, null);
  }

  public final boolean areColumnsKnown(Column[] columns) {
    if (current_ == null)
      return false;
    for (Column c: columns)
      if (current_.getSplit(c.column)==null)
        return false;
    return true;
  }
  
}  
