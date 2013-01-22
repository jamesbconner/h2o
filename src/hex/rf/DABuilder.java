package hex.rf;

import java.util.ArrayList;

import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import water.*;

class DABuilder {

  protected final DRF _drf;

  static DABuilder create(final DRF drf) {
    return drf._useStratifySampling ? new StratifiedDABuilder(drf) : new DABuilder(drf);
  }

  @SuppressWarnings("unused") private DABuilder() { this(null); };

  DABuilder(final DRF drf) { _drf = drf;  }

  final DataAdapter build(final Key arykey, final Key [] keys) { return inhaleData(arykey, keys); }

  /** Check that we have proper number of valid columns vs. features selected, if not cap*/
  final void checkAndLimitFeatureUsedPerSplit(final DataAdapter dapt) {
    int validCols = -1; // for classIdx column
    for (int i = 0; i < dapt.columns(); ++i) if (!dapt.ignore(i)) ++validCols;
    if (validCols < _drf._numSplitFeatures) {
      Utils.pln("Limiting features from " + _drf._numSplitFeatures +
          " to " + validCols + " because there are no more valid columns in the dataset");
      _drf._numSplitFeatures= validCols;
    }
  }

  /** Return the number of rows on this node. */
  final int getRowCount(final Key[] keys, final int rowsize) {
    int num_rows = 0;    // One pass over all chunks to compute max rows
    for( Key key : keys ) if( key.home() ) num_rows += DKV.get(key)._max/rowsize;
    return num_rows;
  }

  /** Return chunk index of the first chunk on this node. Used to identify the trees built here.*/
  final long getChunkId(final Key[] keys) {
    for( Key key : keys ) if( key.home() ) return ValueArray.getChunkIndex(key);
    throw new Error("No key on this node");
  }

  /** Build data adapter for given array */
  protected  DataAdapter inhaleData(final Key arykey, final Key [] keys) {
    final ValueArray ary = ValueArray.value(DKV.get(arykey));
    final int rowsize = ary._rowsize;
    _drf._numrows = DKV.get(keys[0])._max/rowsize; // Rows-per-chunk
    Timer t_inhale = new Timer();
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
    ArrayList<RecursiveAction> dataInhaleJobs = new ArrayList<RecursiveAction>();
    int start_row = 0;
    for( final Key k : keys ) {    // now read the values
      final int S = start_row;
      if (!k.home()) continue;     // skip no local keys (we only inhale local data)
      final int rows = DKV.get(k)._max/rowsize;
      dataInhaleJobs.add(new RecursiveAction() {
        @Override protected void compute() {
          AutoBuffer bits = ary.getChunk(k);
          for(int j = 0; j < rows; ++j) {
            int rowNum = S + j; // row number in the subset of the data on the node
            boolean rowIsValid = false;
            for( int c = 0; c < ncolumns; ++c) {
              if( dapt.ignore(c) ) continue;
              else if( !dapt.isValid(ary,bits,j,c)) dapt.addBad(rowNum, c);
              else {
                dapt.add((float)ary.datad(bits,j,c), rowNum, c);
                if (c!=_drf._classcol) // if the row contains at least one correct value except class column consider it as correct
                  rowIsValid |= true;
              }
            }
            // The whole row is invalid in the following cases: all values are NaN or there is no class specified (NaN in class column)
            if (!rowIsValid) dapt.markIgnoredRow(j);
          }
        }});
      start_row += rows;
    }
    ForkJoinTask.invokeAll(dataInhaleJobs);
    dapt.shrink();
    Utils.pln("[RF] Inhale done in " + t_inhale);
    return dapt;
  }
}
