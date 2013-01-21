package hex.rf;

import water.*;

abstract public class DABuilder {
  protected final DRF _drf;

  public static DABuilder create(final DRF drf) {
    return drf._useStratifySampling ? new StratifiedDABuilder(drf) : new NormalDABuilder(drf);
  }

  @SuppressWarnings("unused") private DABuilder() { this(null); };

  DABuilder(final DRF drf) { _drf = drf;  }

  final DataAdapter build(final Key arykey, final Key [] keys) {
    return inhaleData(arykey, keys);
  }

  protected abstract DataAdapter inhaleData(final Key arykey, final Key [] keys);

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
}
