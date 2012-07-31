package test.analytics;

import water.csv.CSVParser.CSVParserSetup;

public class PokerAvg extends DAvg<int[]>{
  
  /**
   * 
   */
  private static final long serialVersionUID = 2113818373535413958L;
  
  static CSVParserSetup _setup;
  static {
    _setup = new CSVParserSetup();
    _setup._parseColumnNames = false;
  }
  
  public PokerAvg() throws NoSuchFieldException, SecurityException{
    super(new int[11], null, _setup);
  }
    
    
}
