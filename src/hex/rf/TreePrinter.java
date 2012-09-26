package hex.rf;

import hex.rf.Tree.ExclusionNode;
import hex.rf.Tree.LeafNode;
import hex.rf.Tree.SplitNode;

import java.io.IOException;



public abstract class TreePrinter {
  protected final String[] _columnNames;

  public TreePrinter(String[] columns) {
    _columnNames = columns;
  }

  public abstract void printForest(RandomForest rf) throws IOException;
  public abstract void printTree(Tree t) throws IOException;
  abstract void printNode(LeafNode t) throws IOException;
  abstract void printNode(SplitNode t) throws IOException;
  abstract void printNode(ExclusionNode t) throws IOException;
}
