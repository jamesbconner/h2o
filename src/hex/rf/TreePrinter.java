package hex.rf;

<<<<<<< HEAD:src/hex/rf/TreePrinter.java
import hex.rf.Tree.*;
=======
import hexlytics.rf.Tree.ExclusionNode;
import hexlytics.rf.Tree.LeafNode;
import hexlytics.rf.Tree.SplitNode;
>>>>>>> Delete Unused class Node.:src/hexlytics/rf/TreePrinter.java

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
