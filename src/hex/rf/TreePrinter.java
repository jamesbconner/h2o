package hex.rf;

import hex.rf.Tree.*;

import java.io.IOException;


public abstract class TreePrinter {
  protected final String[] _columnNames;

  public TreePrinter(String[] columns) {
    _columnNames = columns;
  }

  public abstract void printForest(RandomForest rf) throws IOException;
  public abstract void printTree(Tree t) throws IOException;
  abstract void printNode(LeafNode t) throws IOException;
  abstract void printNode(Node t) throws IOException;
  abstract void printNode(SplitNode t) throws IOException;
  abstract void printNode(ExclusionNode t) throws IOException;
}
