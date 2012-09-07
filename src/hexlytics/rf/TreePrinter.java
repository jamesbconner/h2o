package hexlytics.rf;

import hexlytics.rf.Tree.LeafNode;
import hexlytics.rf.Tree.Node;
import hexlytics.rf.Tree.SplitNode;

import java.io.*;
import java.text.MessageFormat;


public class TreePrinter {
  private final Appendable _dest;
  private final String[] _columnNames;

  public TreePrinter(OutputStream dest, String[] columns) {
    this(new OutputStreamWriter(dest), columns);
  }

  public TreePrinter(Appendable dest, String[] columns) {
    _dest = dest;
    _columnNames = columns;
  }

  public void printForest(RandomForest rf) throws IOException {
    _dest.append("digraph {\n");
    for (Tree t : rf._trees) {
      t._tree.print(this);
    }
    _dest.append("}");
    if( _dest instanceof Flushable ) ((Flushable) _dest).flush();
  }

  public void printTree(Tree t) throws IOException {
    _dest.append("digraph {\n");
    t._tree.print(this);
    _dest.append("}");
    if( _dest instanceof Flushable ) ((Flushable) _dest).flush();
  }

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
        MessageFormat.format("data[{0}] <= {1}",
            _columnNames[t._column], t._value)));

    t._l.print(this);
    t._r.print(this);

    int lhs = System.identityHashCode(t._l);
    int rhs = System.identityHashCode(t._r);
    _dest.append(String.format("%d -> %d;\n", obj, lhs));
    _dest.append(String.format("%d -> %d;\n", obj, rhs));
  }

  void printNode(SplitNode t) throws IOException {
    int obj = System.identityHashCode(t);

    _dest.append(String.format("%d [label=\"%s\\n%s\"];\n",
        obj, "Node",
        MessageFormat.format("data[{0}] <= {1} (gini)",
            _columnNames[t._column], t._split)));

    t._l.print(this);
    t._r.print(this);

    int lhs = System.identityHashCode(t._l);
    int rhs = System.identityHashCode(t._r);
    _dest.append(String.format("%d -> %d;\n", obj, lhs));
    _dest.append(String.format("%d -> %d;\n", obj, rhs));
  }
}
