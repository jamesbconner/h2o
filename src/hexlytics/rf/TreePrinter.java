package hexlytics.rf;

import hexlytics.rf.Tree.GiniNode;
import hexlytics.rf.Tree.LeafNode;
import hexlytics.rf.Tree.Node;
import java.io.IOException;
import java.text.MessageFormat;


public class TreePrinter {
  private final Appendable _dest;

  public TreePrinter(Appendable dest) {
    _dest = dest;
  }

  public void printForest(RandomForest rf) throws IOException {
    _dest.append("digraph {\n");
    for (Tree t : rf._trees) {
      t._tree.print(this);
    }
    _dest.append("}");
  }

  public void printTree(Tree t) throws IOException {
    _dest.append("digraph {\n");
    t._tree.print(this);
    _dest.append("}");
  }

//  void printNode(GiniNode t) throws IOException {
//    int obj = System.identityHashCode(t);
//
//    _dest.append(String.format("%d [label=\"%s\\n%s\"];\n",
//        obj, "Gini Node",
//        MessageFormat.format("data[{0}] <= {1}", t.column, t.split)));
//
//    t.l_.print(this);
//    t.r_.print(this);
//
//    int lhs = System.identityHashCode(t.l_);
//    int rhs = System.identityHashCode(t.r_);
//    _dest.append(String.format("%d -> %d;\n", obj, lhs));
//    _dest.append(String.format("%d -> %d;\n", obj, rhs));
//  }
//
  void printNode(LeafNode t) throws IOException {
    int obj = System.identityHashCode(t);
    _dest.append(String.format("%d [label=\"%s\\n%s\"];\n",
        obj, "Leaf Node",
        MessageFormat.format("Class {0}", t.class_)));
  }

  void printNode(Node t) throws IOException {
    int obj = System.identityHashCode(t);

    _dest.append(String.format("%d [label=\"%s\\n%s\"];\n",
        obj, "Node",
        MessageFormat.format("data[{0}] <= {1}", t.column_, t.value_)));

    t.l_.print(this);
    t.r_.print(this);

    int lhs = System.identityHashCode(t.l_);
    int rhs = System.identityHashCode(t.r_);
    _dest.append(String.format("%d -> %d;\n", obj, lhs));
    _dest.append(String.format("%d -> %d;\n", obj, rhs));
  }

  void printNode(GiniNode t) throws IOException {
    int obj = System.identityHashCode(t);

    _dest.append(String.format("%d [label=\"%s\\n%s\"];\n",
        obj, "Node",
        MessageFormat.format("data[{0}] <= {1} (gini)", t.column, t.split)));

    t.l_.print(this);
    t.r_.print(this);

    int lhs = System.identityHashCode(t.l_);
    int rhs = System.identityHashCode(t.r_);
    _dest.append(String.format("%d -> %d;\n", obj, lhs));
    _dest.append(String.format("%d -> %d;\n", obj, rhs));
  }
}
