package test.analytics;
import analytics.AverageStatistic;
import analytics.DataAdapter;
import analytics.Statistic;

public class PokerChunkedAdapter extends DataAdapter {

   DataChunk chunk = new DataChunk();
   int maxRow, curRow;
   
   PokerChunkedAdapter() { for(int i=0;i<11;i++) chunk.addIntCol(); }
   
   void addRow(int[] data) {
     for(int i=0;i<11;i++) chunk.setI(data[i], i, maxRow);
     maxRow++;
   }

   public int numRows() { return maxRow; }
   public int numColumns() { return 10; }
   public boolean isInt(int index) { return true; }
   public int toInt(int index) { return chunk.getI(curRow, index);  }
   public double toDouble(int index) { throw new Error(); }
   public int numClasses() { return 10; }
   public int dataClass() { return chunk.getI(curRow, 10); } 
   public int numFeatures() { return 6; }
   public Statistic createStatistic() { return new AverageStatistic(this); }

}
