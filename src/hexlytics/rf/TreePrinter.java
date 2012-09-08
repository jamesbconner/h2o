package hexlytics.rf;

import hexlytics.rf.Tree.LeafNode;
import hexlytics.rf.Tree.Node;
import hexlytics.rf.Tree.SplitNode;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;


public abstract class TreePrinter {
  protected final Appendable _dest;
  protected final String[] _columnNames;

  public TreePrinter(OutputStream dest, String[] columns) {
    this(new OutputStreamWriter(dest), columns);
  }

  public TreePrinter(Appendable dest, String[] columns) {
    _dest = dest;
    _columnNames = columns;
  }

  public abstract void printForest(RandomForest rf) throws IOException;
  public abstract void printTree(Tree t) throws IOException;
  abstract void printNode(LeafNode t) throws IOException;
  abstract void printNode(Node t) throws IOException;
  abstract void printNode(SplitNode t) throws IOException;
}
