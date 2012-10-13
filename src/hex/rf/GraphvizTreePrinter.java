package hex.rf;

import hex.rf.Tree.ExclusionNode;
import hex.rf.Tree.LeafNode;
import hex.rf.Tree.SplitNode;

import java.io.*;
import java.text.MessageFormat;


public class GraphvizTreePrinter extends TreePrinter {
  private final Appendable _dest;

  public GraphvizTreePrinter(OutputStream dest, String[] columns, String[]classNames) {
    this(new OutputStreamWriter(dest), columns, classNames);
  }

  public GraphvizTreePrinter(Appendable dest, String[] columns, String[]classNames) {
    super(columns,classNames);
    _dest = dest;
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


  @Override
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

  @Override
  void printNode(ExclusionNode t) throws IOException {
    int obj = System.identityHashCode(t);

    _dest.append(String.format("%d [label=\"%s\\n%s\"];\n",
        obj, "Node",
        MessageFormat.format("data[{0}] == {1} (gini)",
            _columnNames[t._column], t._split)));

    t._l.print(this);
    t._r.print(this);

    int lhs = System.identityHashCode(t._l);
    int rhs = System.identityHashCode(t._r);
    _dest.append(String.format("%d -> %d;\n", obj, lhs));
    _dest.append(String.format("%d -> %d;\n", obj, rhs));
  }



  // Walk and print a serialized tree - we do not get a proper tree structure,
  // instead the deserializer walks us on the fly.
  public void walk_serialized_tree( byte[] tbits ) {
    try {
      _dest.append("digraph {\n");
      new Tree.TreeVisitor<IOException>(tbits) {
        Tree.TreeVisitor leaf(int tclass ) throws IOException {
          String x = (tclass < _classNames.length) 
            ? String.format("%d [label=\"%s\"];\n"      , _ts._off-2, _classNames[tclass])
            : String.format("%d [label=\"Class %d\"];\n", _ts._off-2, tclass);
          _dest.append(x);
          return this;
        }
        Tree.TreeVisitor pre (int col, float fcmp, int off0, int offl, int offr ) throws IOException {
          byte b = _ts._buf[off0];
          _dest.append(String.format("%d [label=\"%s %s %f\"];\n",
                                     off0, _columnNames[col], ((b=='E')?"==":"<="), fcmp));
          _dest.append(String.format("%d -> %d;\n", off0, offl));
          _dest.append(String.format("%d -> %d;\n", off0, offr));
          return this;
        }
      }.visit();
      _dest.append("}");
      if( _dest instanceof Flushable ) ((Flushable) _dest).flush();
    } catch( IOException e ) { throw new Error(e); }
  }
}
