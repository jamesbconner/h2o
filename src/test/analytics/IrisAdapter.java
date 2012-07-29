package test.analytics;

import analytics.AverageStatistic;
import analytics.DataAdapter;
import analytics.RF;
import analytics.RFBuilder;


public class IrisAdapter extends DataAdapter {
 
  String[] names = new String[] { "ID", "Sepal.Length", "Sepal.Width",
      "Petal.Length", "Petal.Width", "Species" };

  class F {  int id; double sl, sw, pl, pw;   String class_;
    F(int id_, double sl_, double sw_, double pl_, double pw_, String class_) {
      id = id_;  sl = sl_;   sw = sw_;  pl = pl_;  pw = pw_; this.class_ = class_;
    }
  }

  F[] data = new F[] { new F(1, 5.1, 3.5, 1.4, 0.2, "setosa"),
      new F(2, 4.9, 3.0, 1.4, 0.2, "setosa"),
      new F(3, 4.7, 3.2, 1.3, 0.2, "setosa"),
      new F(4, 4.6, 3.1, 1.5, 0.2, "setosa"),
      new F(5, 5.0, 3.6, 1.4, 0.2, "setosa"),
      new F(6, 5.4, 3.9, 1.7, 0.4, "setosa"),
      new F(7, 4.6, 3.4, 1.4, 0.3, "setosa"),
      new F(8, 5.0, 3.4, 1.5, 0.2, "setosa"),
      new F(9, 4.4, 2.9, 1.4, 0.2, "setosa"),
      new F(10, 4.9, 3.1, 1.5, 0.1, "setosa"),
      new F(11, 5.4, 3.7, 1.5, 0.2, "setosa"),
      new F(12, 4.8, 3.4, 1.6, 0.2, "setosa"),
      new F(13, 4.8, 3.0, 1.4, 0.1, "setosa"),
      new F(14, 4.3, 3.0, 1.1, 0.1, "setosa"),
      new F(15, 5.8, 4.0, 1.2, 0.2, "setosa"),
      new F(16, 5.7, 4.4, 1.5, 0.4, "setosa"),
      new F(17, 5.4, 3.9, 1.3, 0.4, "setosa"),
      new F(18, 5.1, 3.5, 1.4, 0.3, "setosa"),
      new F(19, 5.7, 3.8, 1.7, 0.3, "setosa"),
      new F(20, 5.1, 3.8, 1.5, 0.3, "setosa"),
      new F(21, 5.4, 3.4, 1.7, 0.2, "setosa"),
      new F(22, 5.1, 3.7, 1.5, 0.4, "setosa"),
      new F(23, 4.6, 3.6, 1.0, 0.2, "setosa"),
      new F(24, 5.1, 3.3, 1.7, 0.5, "setosa"),
      new F(25, 4.8, 3.4, 1.9, 0.2, "setosa"),
      new F(26, 5.0, 3.0, 1.6, 0.2, "setosa"),
      new F(27, 5.0, 3.4, 1.6, 0.4, "setosa"),
      new F(28, 5.2, 3.5, 1.5, 0.2, "setosa"),
      new F(29, 5.2, 3.4, 1.4, 0.2, "setosa"),
      new F(30, 4.7, 3.2, 1.6, 0.2, "setosa"),
      new F(31, 4.8, 3.1, 1.6, 0.2, "setosa"),
      new F(32, 5.4, 3.4, 1.5, 0.4, "setosa"),
      new F(33, 5.2, 4.1, 1.5, 0.1, "setosa"),
      new F(34, 5.5, 4.2, 1.4, 0.2, "setosa"),
      new F(35, 4.9, 3.1, 1.5, 0.2, "setosa"),
      new F(36, 5.0, 3.2, 1.2, 0.2, "setosa"),
      new F(37, 5.5, 3.5, 1.3, 0.2, "setosa"),
      new F(38, 4.9, 3.6, 1.4, 0.1, "setosa"),
      new F(39, 4.4, 3.0, 1.3, 0.2, "setosa"),
      new F(40, 5.1, 3.4, 1.5, 0.2, "setosa"),
      new F(41, 5.0, 3.5, 1.3, 0.3, "setosa"),
      new F(42, 4.5, 2.3, 1.3, 0.3, "setosa"),
      new F(43, 4.4, 3.2, 1.3, 0.2, "setosa"),
      new F(44, 5.0, 3.5, 1.6, 0.6, "setosa"),
      new F(45, 5.1, 3.8, 1.9, 0.4, "setosa"),
      new F(46, 4.8, 3.0, 1.4, 0.3, "setosa"),
      new F(47, 5.1, 3.8, 1.6, 0.2, "setosa"),
      new F(48, 4.6, 3.2, 1.4, 0.2, "setosa"),
      new F(49, 5.3, 3.7, 1.5, 0.2, "setosa"),
      new F(50, 5.0, 3.3, 1.4, 0.2, "setosa"),
      new F(51, 7.0, 3.2, 4.7, 1.4, "versicolor"),
      new F(52, 6.4, 3.2, 4.5, 1.5, "versicolor"),
      new F(53, 6.9, 3.1, 4.9, 1.5, "versicolor"),
      new F(54, 5.5, 2.3, 4.0, 1.3, "versicolor"),
      new F(55, 6.5, 2.8, 4.6, 1.5, "versicolor"),
      new F(56, 5.7, 2.8, 4.5, 1.3, "versicolor"),
      new F(57, 6.3, 3.3, 4.7, 1.6, "versicolor"),
      new F(58, 4.9, 2.4, 3.3, 1.0, "versicolor"),
      new F(59, 6.6, 2.9, 4.6, 1.3, "versicolor"),
      new F(60, 5.2, 2.7, 3.9, 1.4, "versicolor"),
      new F(61, 5.0, 2.0, 3.5, 1.0, "versicolor"),
      new F(62, 5.9, 3.0, 4.2, 1.5, "versicolor"),
      new F(63, 6.0, 2.2, 4.0, 1.0, "versicolor"),
      new F(64, 6.1, 2.9, 4.7, 1.4, "versicolor"),
      new F(65, 5.6, 2.9, 3.6, 1.3, "versicolor"),
      new F(66, 6.7, 3.1, 4.4, 1.4, "versicolor"),
      new F(67, 5.6, 3.0, 4.5, 1.5, "versicolor"),
      new F(68, 5.8, 2.7, 4.1, 1.0, "versicolor"),
      new F(69, 6.2, 2.2, 4.5, 1.5, "versicolor"),
      new F(70, 5.6, 2.5, 3.9, 1.1, "versicolor"),
      new F(71, 5.9, 3.2, 4.8, 1.8, "versicolor"),
      new F(72, 6.1, 2.8, 4.0, 1.3, "versicolor"),
      new F(73, 6.3, 2.5, 4.9, 1.5, "versicolor"),
      new F(74, 6.1, 2.8, 4.7, 1.2, "versicolor"),
      new F(75, 6.4, 2.9, 4.3, 1.3, "versicolor"),
      new F(76, 6.6, 3.0, 4.4, 1.4, "versicolor"),
      new F(77, 6.8, 2.8, 4.8, 1.4, "versicolor"),
      new F(78, 6.7, 3.0, 5.0, 1.7, "versicolor"),
      new F(79, 6.0, 2.9, 4.5, 1.5, "versicolor"),
      new F(80, 5.7, 2.6, 3.5, 1.0, "versicolor"),
      new F(81, 5.5, 2.4, 3.8, 1.1, "versicolor"),
      new F(82, 5.5, 2.4, 3.7, 1.0, "versicolor"),
      new F(83, 5.8, 2.7, 3.9, 1.2, "versicolor"),
      new F(84, 6.0, 2.7, 5.1, 1.6, "versicolor"),
      new F(85, 5.4, 3.0, 4.5, 1.5, "versicolor"),
      new F(86, 6.0, 3.4, 4.5, 1.6, "versicolor"),
      new F(87, 6.7, 3.1, 4.7, 1.5, "versicolor"),
      new F(88, 6.3, 2.3, 4.4, 1.3, "versicolor"),
      new F(89, 5.6, 3.0, 4.1, 1.3, "versicolor"),
      new F(90, 5.5, 2.5, 4.0, 1.3, "versicolor"),
      new F(91, 5.5, 2.6, 4.4, 1.2, "versicolor"),
      new F(92, 6.1, 3.0, 4.6, 1.4, "versicolor"),
      new F(93, 5.8, 2.6, 4.0, 1.2, "versicolor"),
      new F(94, 5.0, 2.3, 3.3, 1.0, "versicolor"),
      new F(95, 5.6, 2.7, 4.2, 1.3, "versicolor"),
      new F(96, 5.7, 3.0, 4.2, 1.2, "versicolor"),
      new F(97, 5.7, 2.9, 4.2, 1.3, "versicolor"),
      new F(98, 6.2, 2.9, 4.3, 1.3, "versicolor"),
      new F(99, 5.1, 2.5, 3.0, 1.1, "versicolor"),
      new F(100, 5.7, 2.8, 4.1, 1.3, "versicolor"),
      new F(101, 6.3, 3.3, 6.0, 2.5, "virginica"),
      new F(102, 5.8, 2.7, 5.1, 1.9, "virginica"),
      new F(103, 7.1, 3.0, 5.9, 2.1, "virginica"),
      new F(104, 6.3, 2.9, 5.6, 1.8, "virginica"),
      new F(105, 6.5, 3.0, 5.8, 2.2, "virginica"),
      new F(106, 7.6, 3.0, 6.6, 2.1, "virginica"),
      new F(107, 4.9, 2.5, 4.5, 1.7, "virginica"),
      new F(108, 7.3, 2.9, 6.3, 1.8, "virginica"),
      new F(109, 6.7, 2.5, 5.8, 1.8, "virginica"),
      new F(110, 7.2, 3.6, 6.1, 2.5, "virginica"),
      new F(111, 6.5, 3.2, 5.1, 2.0, "virginica"),
      new F(112, 6.4, 2.7, 5.3, 1.9, "virginica"),
      new F(113, 6.8, 3.0, 5.5, 2.1, "virginica"),
      new F(114, 5.7, 2.5, 5.0, 2.0, "virginica"),
      new F(115, 5.8, 2.8, 5.1, 2.4, "virginica"),
      new F(116, 6.4, 3.2, 5.3, 2.3, "virginica"),
      new F(117, 6.5, 3.0, 5.5, 1.8, "virginica"),
      new F(118, 7.7, 3.8, 6.7, 2.2, "virginica"),
      new F(119, 7.7, 2.6, 6.9, 2.3, "virginica"),
      new F(120, 6.0, 2.2, 5.0, 1.5, "virginica"),
      new F(121, 6.9, 3.2, 5.7, 2.3, "virginica"),
      new F(122, 5.6, 2.8, 4.9, 2.0, "virginica"),
      new F(123, 7.7, 2.8, 6.7, 2.0, "virginica"),
      new F(124, 6.3, 2.7, 4.9, 1.8, "virginica"),
      new F(125, 6.7, 3.3, 5.7, 2.1, "virginica"),
      new F(126, 7.2, 3.2, 6.0, 1.8, "virginica"),
      new F(127, 6.2, 2.8, 4.8, 1.8, "virginica"),
      new F(128, 6.1, 3.0, 4.9, 1.8, "virginica"),
      new F(129, 6.4, 2.8, 5.6, 2.1, "virginica"),
      new F(130, 7.2, 3.0, 5.8, 1.6, "virginica"),
      new F(131, 7.4, 2.8, 6.1, 1.9, "virginica"),
      new F(132, 7.9, 3.8, 6.4, 2.0, "virginica"),
      new F(133, 6.4, 2.8, 5.6, 2.2, "virginica"),
      new F(134, 6.3, 2.8, 5.1, 1.5, "virginica"),
      new F(135, 6.1, 2.6, 5.6, 1.4, "virginica"),
      new F(136, 7.7, 3.0, 6.1, 2.3, "virginica"),
      new F(137, 6.3, 3.4, 5.6, 2.4, "virginica"),
      new F(138, 6.4, 3.1, 5.5, 1.8, "virginica"),
      new F(139, 6.0, 3.0, 4.8, 1.8, "virginica"),
      new F(140, 6.9, 3.1, 5.4, 2.1, "virginica"),
      new F(141, 6.7, 3.1, 5.6, 2.4, "virginica"),
      new F(142, 6.9, 3.1, 5.1, 2.3, "virginica"),
      new F(143, 5.8, 2.7, 5.1, 1.9, "virginica"),
      new F(144, 6.8, 3.2, 5.9, 2.3, "virginica"),
      new F(145, 6.7, 3.3, 5.7, 2.5, "virginica"),
      new F(146, 6.7, 3.0, 5.2, 2.3, "virginica"),
      new F(147, 6.3, 2.5, 5.0, 1.9, "virginica"),
      new F(148, 6.5, 3.0, 5.2, 2.0, "virginica"),
      new F(149, 6.2, 3.4, 5.4, 2.3, "virginica"),
      new F(150, 5.9, 3.0, 5.1, 1.8, "virginica") };

  
  public int numColumns() { return 5;  }
  public boolean isInt(int index) {  return index==0;  }
  public int toInt(int index) {  if(index==0) return data[cur].id; else throw new Error(); }

  public double toDouble(int index) {
    if (index >0 && index <=4) {
      if (index==1) return data[cur].pl;
      if (index==2) return data[cur].pw;
      if (index==3) return data[cur].sl;
      if (index==4) return data[cur].sw;
    } else if (index==0) return (double)data[cur].id;
    throw new Error("Accessing column "+index);
  }

  public int numRows() { return data.length;  }
  public int numClasses() { return 3; }
  public int dataClass() {
    if(data[cur].class_.equals("setosa")) return 0;
    if(data[cur].class_.equals("versicolor")) return 1;
    if(data[cur].class_.equals("virginica")) return 2;
    throw new Error();
  }

  public static void main(String []_) {   
    DataAdapter data=new IrisAdapter();
    RF rf = RF.compute(100,new IrisBuilder(67436482,data));
    System.out.println("Built.");
  }
}

class IrisBuilder extends RFBuilder {
  protected IrisBuilder(long seed, DataAdapter data) { super(seed,data);  }
  @Override  protected void createStatistic(ProtoNode node, int[] columns) {
        node.addStatistic(new AverageStatistic(columns,3));
  }
  @Override protected int numberOfFeatures(ProtoNode node, ProtoTree tree) { return 3; }  
}
