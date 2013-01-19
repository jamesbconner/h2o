/**
 *
 */
package hex.rf;

import java.util.*;

import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;

import water.*;
import water.Timer;

/**
 *   Stratified-based loader.
 *
 */
public class NormalDABuilder extends DABuilder {

  public NormalDABuilder(final DRF drf) {
    super(drf);
  }

  @Override
  protected final DataAdapter inhaleData(final Key arykey, final Key [] keys) {
    final ValueArray ary = ValueArray.value(DKV.get(arykey));
    final int rowsize = ary._rowsize;
    _drf._numrows = DKV.get(keys[0])._max/rowsize; // Rows-per-chunk

    // Start the timer.
    Timer t_bin = new Timer();
    // Create the data adapter.
    final DataAdapter dapt = new DataAdapter( ary,
                                              _drf._classcol,
                                              _drf._ignores,
                                              getRowCount(keys, rowsize),
                                              getChunkId(keys),
                                              _drf._seed,
                                              _drf._binLimit,
                                              _drf._classWt);

    // Check that we have proper number of valid columns vs. features selected, if not cap.
    checkAndLimitFeatureUsedPerSplit(dapt);

    // Now load the DataAdapter with all the rows on this node.
    final int ncolumns = ary._cols.length;
    int bins = 0;
    for (int i = 0; i < ncolumns; i++) if (dapt.binColumn(i)) bins++;
    int[] colIds = new int[bins];
    for (int i = 0, j = 0; i < ncolumns; i++)
      if (dapt.binColumn(i)) colIds[j++]=i;
    if (bins > 0) {
      binData(dapt, keys, ary, colIds);
    }
    // Binning is done.
    Utils.pln("[RF] Binning done in " + t_bin);

    // Inhale data.
    Timer t_inhale = new Timer();
    ArrayList<RecursiveAction> dataInhaleJobs = new ArrayList<RecursiveAction>();
    int start_row = 0;
    for( final Key k : keys ) {    // now read the values
      final int S = start_row;
      if (!k.home()) continue;
      final int rows = DKV.get(k)._max/rowsize;
      dataInhaleJobs.add(new RecursiveAction() {
        @Override
        protected void compute() {
          AutoBuffer bits = ary.getChunk(k);
          ROWS: for(int j = 0; j < rows; ++j) {
            for(int c = 0; c < ncolumns; ++c) { // Bail out of broken rows in not-ignored columns
              if( !dapt.ignore(c) && ary.isNA(bits,j,c)) {
                dapt.setBad(S+j);
                continue ROWS;
              }
            }
            for( int c = 0; c < ncolumns; ++c) {
              if( dapt.ignore(c) ) {
                dapt.addValue((short)0,S+j,c);
              } else if( dapt.binColumn(c) ) {
                dapt.addValue((float)ary.datad(bits,j,c), S+j, c);
              } else {
                long v = ary.data(bits,j,c);
                v -= ary._cols[c]._min;
                dapt.addValue((short)v, S+j, c);
              }
            }
          }
        }
      });

      start_row += rows;
    }
    ForkJoinTask.invokeAll(dataInhaleJobs);
    Utils.pln("[RF] Inhale done in " + t_inhale);

    return dapt;
  }

  /** Bin specified columns of given value array and inhale them into given data adapter. */
  private static void binData(final DataAdapter dapt, final Key [] keys, final ValueArray ary, final int [] colIds){
    final int rowsize= ary._rowsize;

    ArrayList<RecursiveAction> jobs = new ArrayList<RecursiveAction>();
    int start_row = 0;
    for(final Key k:keys) {
      if( !k.home() ) continue;
      final int rows = DKV.get(k)._max/rowsize;
      final int S = start_row;
      jobs.add(new RecursiveAction() {
        @Override
        protected void compute() {
          AutoBuffer bits = ary.getChunk(k);
          ROWS: for(int j = 0; j < rows; ++j) {
            for(int col : colIds)
              if( ary.isNA(bits,j,col) ) continue ROWS;
            for(int col : colIds)
              dapt.addValueRaw((float)ary.datad(bits,j,col), j + S, col);
          }
        }
      });
      start_row += rows;
    }
    ForkJoinTask.invokeAll(jobs);

    // Now do binning.
    jobs.clear();
    for(final int col : colIds) {
      jobs.add(new RecursiveAction() {
        @Override protected void compute() {
          dapt.computeBins(col);
        }
      });
    }
    ForkJoinTask.invokeAll(jobs);
  }
}
