/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water;

/** Simple persistence backend that does not persist the values in any way.
 * 
 * This backend is used for internal values that should not be persisted. It
 * literally does nothing.
 *
 * @author peta
 */
public class PersistNone extends Persistence {

  @Override public Value load(Key k, Value sentinel) {
    assert (false); // no sentinels in PersistNone
    return sentinel;
  }
  
  @Override public byte[] get(Key k, Value v, int len) {
    assert (false);
    return null;
  }

  @Override public boolean store(Key k, Value v) {
    return true;
  }

  @Override public boolean delete(Key k, Value v) {
    return true;
  }

  @Override public long size(Key k, Value v) {
    return 0;
  }

  @Override public String name() {
    return "internal";
  }
  
  @Override public Type type() {
    return Type.INTERNAL;
  }
  
  PersistNone() {
    super(Type.INTERNAL);
  }
  
  static final PersistNone _instance;
  
  public static PersistNone instance() {
    return _instance;
  }
  
  static {
    _instance = new PersistNone();
  }

}