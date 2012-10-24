package hex;

import water.*;

public class NOPTask extends MRTask {

  public int _res;
  @Override
  public void map(Key key) {
    byte [] mem = DKV.get(key).get();
    for(byte b:mem){
      _res ^= b;
    }
  }

  @Override
  public void reduce(DRemoteTask drt) {
    NOPTask other = (NOPTask)drt;
    _res ^= other._res;
  }
}
