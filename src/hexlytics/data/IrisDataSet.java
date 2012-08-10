/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.data;


/** Iris data set. For speed uses the trick that we return the full array
 * including the category, but the numColumns does not say the category can be
 * used. 
 * 
 * This can of course be completely changed without breaking anything else.
 *
 * @author peta
 */
class IrisDataSet extends DataSet {
  
  public static final int SL = 0;
  public static final int SW = 1;
  public static final int PL = 2;
  public static final int PW = 3;
  public static final int CAT = 4;
  public static final int ID = 5;
  
  @Override public long numRows() {
    return data_.length;
  }

  @Override public double[] getRow(int index) {
    return data_[index];
  }

  @Override public int getRowCategory(int index) {
    return (int)data_[index][CAT];
  }

  @Override public String columnName(int colIndex) {
    switch (colIndex) {
      case 0:
        return "SL";
      case 1:
        return "SW";
      case 2:
        return "PL";
      case 3:
        return "PW";
      default:
        return "Out of bounds or not a valid index";
    }
  }

  @Override public int numColumns() {
    return 4;
  }

  private double[] createRow(int id, double sl, double sw, double pl, double pw, String cat) {
    double c;
    // Can we rely on Java 7 and use switch over strings? I would love that:)
    if (cat.equals("setosa"))
      c = 0;
    else if (cat.equals("versicolor"))
      c = 1;
    else if (cat.equals("virginica"))
      c = 2;
    else 
      throw new Error("Invalid Iris data category "+cat);
    return new double[] { sl, sw, pl, pw, c, id };
  }
  
  double[][] data_;
  
  public IrisDataSet() {
    createData();
  }
  
  public IrisDataSet(long seed) {
    super(seed);
    createData();
  }
    
  private void createData() {  
    data_ = new double[][] { 
      createRow(1, 5.1, 3.5, 1.4, 0.2, "setosa"),
      createRow(2, 4.9, 3.0, 1.4, 0.2, "setosa"),
      createRow(3, 4.7, 3.2, 1.3, 0.2, "setosa"),
      createRow(4, 4.6, 3.1, 1.5, 0.2, "setosa"),
      createRow(5, 5.0, 3.6, 1.4, 0.2, "setosa"),
      createRow(6, 5.4, 3.9, 1.7, 0.4, "setosa"),
      createRow(7, 4.6, 3.4, 1.4, 0.3, "setosa"),
      createRow(8, 5.0, 3.4, 1.5, 0.2, "setosa"),
      createRow(9, 4.4, 2.9, 1.4, 0.2, "setosa"),
      createRow(10, 4.9, 3.1, 1.5, 0.1, "setosa"),
      createRow(11, 5.4, 3.7, 1.5, 0.2, "setosa"),
      createRow(12, 4.8, 3.4, 1.6, 0.2, "setosa"),
      createRow(13, 4.8, 3.0, 1.4, 0.1, "setosa"),
      createRow(14, 4.3, 3.0, 1.1, 0.1, "setosa"),
      createRow(15, 5.8, 4.0, 1.2, 0.2, "setosa"),
      createRow(16, 5.7, 4.4, 1.5, 0.4, "setosa"),
      createRow(17, 5.4, 3.9, 1.3, 0.4, "setosa"),
      createRow(18, 5.1, 3.5, 1.4, 0.3, "setosa"),
      createRow(19, 5.7, 3.8, 1.7, 0.3, "setosa"),
      createRow(20, 5.1, 3.8, 1.5, 0.3, "setosa"),
      createRow(21, 5.4, 3.4, 1.7, 0.2, "setosa"),
      createRow(22, 5.1, 3.7, 1.5, 0.4, "setosa"),
      createRow(23, 4.6, 3.6, 1.0, 0.2, "setosa"),
      createRow(24, 5.1, 3.3, 1.7, 0.5, "setosa"),
      createRow(25, 4.8, 3.4, 1.9, 0.2, "setosa"),
      createRow(26, 5.0, 3.0, 1.6, 0.2, "setosa"),
      createRow(27, 5.0, 3.4, 1.6, 0.4, "setosa"),
      createRow(28, 5.2, 3.5, 1.5, 0.2, "setosa"),
      createRow(29, 5.2, 3.4, 1.4, 0.2, "setosa"),
      createRow(30, 4.7, 3.2, 1.6, 0.2, "setosa"),
      createRow(31, 4.8, 3.1, 1.6, 0.2, "setosa"),
      createRow(32, 5.4, 3.4, 1.5, 0.4, "setosa"),
      createRow(33, 5.2, 4.1, 1.5, 0.1, "setosa"),
      createRow(34, 5.5, 4.2, 1.4, 0.2, "setosa"),
      createRow(35, 4.9, 3.1, 1.5, 0.2, "setosa"),
      createRow(36, 5.0, 3.2, 1.2, 0.2, "setosa"),
      createRow(37, 5.5, 3.5, 1.3, 0.2, "setosa"),
      createRow(38, 4.9, 3.6, 1.4, 0.1, "setosa"),
      createRow(39, 4.4, 3.0, 1.3, 0.2, "setosa"),
      createRow(40, 5.1, 3.4, 1.5, 0.2, "setosa"),
      createRow(41, 5.0, 3.5, 1.3, 0.3, "setosa"),
      createRow(42, 4.5, 2.3, 1.3, 0.3, "setosa"),
      createRow(43, 4.4, 3.2, 1.3, 0.2, "setosa"),
      createRow(44, 5.0, 3.5, 1.6, 0.6, "setosa"),
      createRow(45, 5.1, 3.8, 1.9, 0.4, "setosa"),
      createRow(46, 4.8, 3.0, 1.4, 0.3, "setosa"),
      createRow(47, 5.1, 3.8, 1.6, 0.2, "setosa"),
      createRow(48, 4.6, 3.2, 1.4, 0.2, "setosa"),
      createRow(49, 5.3, 3.7, 1.5, 0.2, "setosa"),
      createRow(50, 5.0, 3.3, 1.4, 0.2, "setosa"),
      createRow(51, 7.0, 3.2, 4.7, 1.4, "versicolor"),
      createRow(52, 6.4, 3.2, 4.5, 1.5, "versicolor"),
      createRow(53, 6.9, 3.1, 4.9, 1.5, "versicolor"),
      createRow(54, 5.5, 2.3, 4.0, 1.3, "versicolor"),
      createRow(55, 6.5, 2.8, 4.6, 1.5, "versicolor"),
      createRow(56, 5.7, 2.8, 4.5, 1.3, "versicolor"),
      createRow(57, 6.3, 3.3, 4.7, 1.6, "versicolor"),
      createRow(58, 4.9, 2.4, 3.3, 1.0, "versicolor"),
      createRow(59, 6.6, 2.9, 4.6, 1.3, "versicolor"),
      createRow(60, 5.2, 2.7, 3.9, 1.4, "versicolor"),
      createRow(61, 5.0, 2.0, 3.5, 1.0, "versicolor"),
      createRow(62, 5.9, 3.0, 4.2, 1.5, "versicolor"),
      createRow(63, 6.0, 2.2, 4.0, 1.0, "versicolor"),
      createRow(64, 6.1, 2.9, 4.7, 1.4, "versicolor"),
      createRow(65, 5.6, 2.9, 3.6, 1.3, "versicolor"),
      createRow(66, 6.7, 3.1, 4.4, 1.4, "versicolor"),
      createRow(67, 5.6, 3.0, 4.5, 1.5, "versicolor"),
      createRow(68, 5.8, 2.7, 4.1, 1.0, "versicolor"),
      createRow(69, 6.2, 2.2, 4.5, 1.5, "versicolor"),
      createRow(70, 5.6, 2.5, 3.9, 1.1, "versicolor"),
      createRow(71, 5.9, 3.2, 4.8, 1.8, "versicolor"),
      createRow(72, 6.1, 2.8, 4.0, 1.3, "versicolor"),
      createRow(73, 6.3, 2.5, 4.9, 1.5, "versicolor"),
      createRow(74, 6.1, 2.8, 4.7, 1.2, "versicolor"),
      createRow(75, 6.4, 2.9, 4.3, 1.3, "versicolor"),
      createRow(76, 6.6, 3.0, 4.4, 1.4, "versicolor"),
      createRow(77, 6.8, 2.8, 4.8, 1.4, "versicolor"),
      createRow(78, 6.7, 3.0, 5.0, 1.7, "versicolor"),
      createRow(79, 6.0, 2.9, 4.5, 1.5, "versicolor"),
      createRow(80, 5.7, 2.6, 3.5, 1.0, "versicolor"),
      createRow(81, 5.5, 2.4, 3.8, 1.1, "versicolor"),
      createRow(82, 5.5, 2.4, 3.7, 1.0, "versicolor"),
      createRow(83, 5.8, 2.7, 3.9, 1.2, "versicolor"),
      createRow(84, 6.0, 2.7, 5.1, 1.6, "versicolor"),
      createRow(85, 5.4, 3.0, 4.5, 1.5, "versicolor"),
      createRow(86, 6.0, 3.4, 4.5, 1.6, "versicolor"),
      createRow(87, 6.7, 3.1, 4.7, 1.5, "versicolor"),
      createRow(88, 6.3, 2.3, 4.4, 1.3, "versicolor"),
      createRow(89, 5.6, 3.0, 4.1, 1.3, "versicolor"),
      createRow(90, 5.5, 2.5, 4.0, 1.3, "versicolor"),
      createRow(91, 5.5, 2.6, 4.4, 1.2, "versicolor"),
      createRow(92, 6.1, 3.0, 4.6, 1.4, "versicolor"),
      createRow(93, 5.8, 2.6, 4.0, 1.2, "versicolor"),
      createRow(94, 5.0, 2.3, 3.3, 1.0, "versicolor"),
      createRow(95, 5.6, 2.7, 4.2, 1.3, "versicolor"),
      createRow(96, 5.7, 3.0, 4.2, 1.2, "versicolor"),
      createRow(97, 5.7, 2.9, 4.2, 1.3, "versicolor"),
      createRow(98, 6.2, 2.9, 4.3, 1.3, "versicolor"),
      createRow(99, 5.1, 2.5, 3.0, 1.1, "versicolor"),
      createRow(100, 5.7, 2.8, 4.1, 1.3, "versicolor"),
      createRow(101, 6.3, 3.3, 6.0, 2.5, "virginica"),
      createRow(102, 5.8, 2.7, 5.1, 1.9, "virginica"),
      createRow(103, 7.1, 3.0, 5.9, 2.1, "virginica"),
      createRow(104, 6.3, 2.9, 5.6, 1.8, "virginica"),
      createRow(105, 6.5, 3.0, 5.8, 2.2, "virginica"),
      createRow(106, 7.6, 3.0, 6.6, 2.1, "virginica"),
      createRow(107, 4.9, 2.5, 4.5, 1.7, "virginica"),
      createRow(108, 7.3, 2.9, 6.3, 1.8, "virginica"),
      createRow(109, 6.7, 2.5, 5.8, 1.8, "virginica"),
      createRow(110, 7.2, 3.6, 6.1, 2.5, "virginica"),
      createRow(111, 6.5, 3.2, 5.1, 2.0, "virginica"),
      createRow(112, 6.4, 2.7, 5.3, 1.9, "virginica"),
      createRow(113, 6.8, 3.0, 5.5, 2.1, "virginica"),
      createRow(114, 5.7, 2.5, 5.0, 2.0, "virginica"),
      createRow(115, 5.8, 2.8, 5.1, 2.4, "virginica"),
      createRow(116, 6.4, 3.2, 5.3, 2.3, "virginica"),
      createRow(117, 6.5, 3.0, 5.5, 1.8, "virginica"),
      createRow(118, 7.7, 3.8, 6.7, 2.2, "virginica"),
      createRow(119, 7.7, 2.6, 6.9, 2.3, "virginica"),
      createRow(120, 6.0, 2.2, 5.0, 1.5, "virginica"),
      createRow(121, 6.9, 3.2, 5.7, 2.3, "virginica"),
      createRow(122, 5.6, 2.8, 4.9, 2.0, "virginica"),
      createRow(123, 7.7, 2.8, 6.7, 2.0, "virginica"),
      createRow(124, 6.3, 2.7, 4.9, 1.8, "virginica"),
      createRow(125, 6.7, 3.3, 5.7, 2.1, "virginica"),
      createRow(126, 7.2, 3.2, 6.0, 1.8, "virginica"),
      createRow(127, 6.2, 2.8, 4.8, 1.8, "virginica"),
      createRow(128, 6.1, 3.0, 4.9, 1.8, "virginica"),
      createRow(129, 6.4, 2.8, 5.6, 2.1, "virginica"),
      createRow(130, 7.2, 3.0, 5.8, 1.6, "virginica"),
      createRow(131, 7.4, 2.8, 6.1, 1.9, "virginica"),
      createRow(132, 7.9, 3.8, 6.4, 2.0, "virginica"),
      createRow(133, 6.4, 2.8, 5.6, 2.2, "virginica"),
      createRow(134, 6.3, 2.8, 5.1, 1.5, "virginica"),
      createRow(135, 6.1, 2.6, 5.6, 1.4, "virginica"),
      createRow(136, 7.7, 3.0, 6.1, 2.3, "virginica"),
      createRow(137, 6.3, 3.4, 5.6, 2.4, "virginica"),
      createRow(138, 6.4, 3.1, 5.5, 1.8, "virginica"),
      createRow(139, 6.0, 3.0, 4.8, 1.8, "virginica"),
      createRow(140, 6.9, 3.1, 5.4, 2.1, "virginica"),
      createRow(141, 6.7, 3.1, 5.6, 2.4, "virginica"),
      createRow(142, 6.9, 3.1, 5.1, 2.3, "virginica"),
      createRow(143, 5.8, 2.7, 5.1, 1.9, "virginica"),
      createRow(144, 6.8, 3.2, 5.9, 2.3, "virginica"),
      createRow(145, 6.7, 3.3, 5.7, 2.5, "virginica"),
      createRow(146, 6.7, 3.0, 5.2, 2.3, "virginica"),
      createRow(147, 6.3, 2.5, 5.0, 1.9, "virginica"),
      createRow(148, 6.5, 3.0, 5.2, 2.0, "virginica"),
      createRow(149, 6.2, 3.4, 5.4, 2.3, "virginica"),
      createRow(150, 5.9, 3.0, 5.1, 1.8, "virginica")
    };
  }
  
}
