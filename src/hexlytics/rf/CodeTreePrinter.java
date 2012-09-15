package hexlytics.rf;

import hexlytics.rf.Tree.ExclusionNode;
import hexlytics.rf.Tree.LeafNode;
import hexlytics.rf.Tree.Node;
import hexlytics.rf.Tree.SplitNode;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import water.util.IndentingAppender;

public class CodeTreePrinter extends TreePrinter {
  private final IndentingAppender _dest;

  public CodeTreePrinter(OutputStream dest, String[] columns) {
    this(new OutputStreamWriter(dest), columns);
  }

  public CodeTreePrinter(Appendable dest, String[] columns) {
    super(columns);
    _dest = new IndentingAppender(dest);
  }

  public void printForest(RandomForest rf) throws IOException {
    _dest.append("int classify(Row row) {\n");
    _dest.incrementIndent();
    for (Tree t : rf._trees) {
      t._tree.print(this);
    }
    _dest.decrementIndent().append("}").flush();
  }

  public void printTree(Tree t) throws IOException {
    _dest.append("int classify(Row row) {\n");
    _dest.incrementIndent();
    t._tree.print(this);
    _dest.decrementIndent().append("}").flush();
  }

  void printNode(LeafNode t) throws IOException {
    _dest.append("return ").append(Integer.toString(t.class_)).append('\n');
  }

  void printNode(Node t) throws IOException {
    // return (row.getS(_column)<=_value ? _l : _r).classify(row);
    _dest.append("if (row.getS(").append(_columnNames[t._column]).append(") <= ");
    _dest.append(Double.toString(t._value)).append(")\n");
    _dest.incrementIndent();
    t._l.print(this);
    _dest.decrementIndent().append("else\n").incrementIndent();
    t._r.print(this);
    _dest.decrementIndent();
  }

  void printNode(SplitNode t) throws IOException {
    // return r.getColumnClass(_column) <= _split ? _l.classify(r) : _r.classify(r);
    _dest.append("if (row.getColumnClass(").append(_columnNames[t._column]).append(") <= ");
    _dest.append(Integer.toString(t._split)).append(")\n");
    _dest.incrementIndent();
    t._l.print(this);
    _dest.decrementIndent().append("else\n").incrementIndent();
    t._r.print(this);
    _dest.decrementIndent();
  }

  void printNode(ExclusionNode t) throws IOException {
    // return r.getColumnClass(_column) <= _split ? _l.classify(r) : _r.classify(r);
    _dest.append("if (row.getColumnClass(").append(_columnNames[t._column]).append(") == ");
    _dest.append(Integer.toString(t._split)).append(")\n");
    _dest.incrementIndent();
    t._l.print(this);
    _dest.decrementIndent().append("else\n").incrementIndent();
    t._r.print(this);
    _dest.decrementIndent();
  }


  public void walk_serialized_tree( byte[] tbits ) {
    try {
      _dest.append("int classify(float fs[]) {\n");
      _dest.incrementIndent();
      new Tree.TreeVisitor<IOException>(tbits) {
        Tree.TreeVisitor leaf(int tclass ) throws IOException {
          _dest.append(String.format("return %d;\n",tclass));
          return this;
        }
        Tree.TreeVisitor pre (int col, float fcmp, int off0, int offl, int offr ) throws IOException { 
          byte b = _ts._buf[off0];
          _dest.append(String.format("if( fs[%s] %s %f ) \n",_columnNames[col],((b=='E')?"==":"<="), fcmp)).incrementIndent();
          return this;
        }
        Tree.TreeVisitor mid (int col, float fcmp ) throws IOException {
          _dest.decrementIndent().append("else\n").incrementIndent();
          return this;
        }
        Tree.TreeVisitor post(int col, float fcmp ) throws IOException {
          _dest.decrementIndent();
          return this;
        }
      }.visit();
      _dest.decrementIndent().append("}").flush();
    } catch( IOException e ) { throw new Error(e); }
  }
}
