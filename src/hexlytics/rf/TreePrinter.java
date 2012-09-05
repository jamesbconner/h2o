package hexlytics.rf;

import hexlytics.rf.Tree.GiniNode;
import hexlytics.rf.Tree.LeafNode;
import hexlytics.rf.Tree.Node;

import java.io.IOException;


public class TreePrinter {
  private final Appendable _dest;
  
  public TreePrinter(Appendable dest) {
    _dest = dest;
  }
  
  public void print(Tree t) throws IOException {
    _dest.append("graph {");
    t.tree_.print(this);
    _dest.append("}");
  }
  
  public void printNode(GiniNode t) throws IOException {
    int obj = System.identityHashCode(t);
    
    _dest.append(String.format("%d [label=\"%s\\n%s\"];",
        obj, "Gini Node",
        String.format("data[%d] <= %d", t.column, t.split)));
    
    t.l_.print(this);
    t.r_.print(this);
    
    int lhs = System.identityHashCode(t.l_);
    int rhs = System.identityHashCode(t.r_);
    _dest.append(String.format("%d -> %d", obj, lhs));
    _dest.append(String.format("%d -> %d", obj, rhs));
  }

  public void printNode(LeafNode t) throws IOException {
    int obj = System.identityHashCode(t);
    _dest.append(String.format("%d [label=\"%s\\n%s\"];",
        obj, "Leaf Node",
        String.format("Class %d", t.class_)));
  }
  
  public void printNode(Node t) throws IOException {
    int obj = System.identityHashCode(t);
    
    _dest.append(String.format("%d [label=\"%s\\n%s\"];",
        obj, "Node",
        String.format("data[%d] <= %d", t.column_, t.value_)));
    
    t.l_.print(this);
    t.r_.print(this);
    
    int lhs = System.identityHashCode(t.l_);
    int rhs = System.identityHashCode(t.r_);
    _dest.append(String.format("%d -> %d", obj, lhs));
    _dest.append(String.format("%d -> %d", obj, rhs));
  }
}
