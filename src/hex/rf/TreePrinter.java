package hex.rf;

import hex.rf.Tree.ExclusionNode;
import hex.rf.Tree.LeafNode;
import hex.rf.Tree.SplitNode;

import java.io.IOException;

public abstract class TreePrinter {
  protected final String[] _columnNames;
  protected final String[] _classNames;

  public TreePrinter(String[] columns, String[] classNames) {
    _columnNames = columns;
    _classNames = classNames;
  }

  public abstract void printTree(Tree t) throws IOException;
  abstract void printNode(LeafNode t) throws IOException;
  abstract void printNode(SplitNode t) throws IOException;
  abstract void printNode(ExclusionNode t) throws IOException;
}
