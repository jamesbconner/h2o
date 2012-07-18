/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water;

/** Sentinel singleton value.
 * 
 * This value is changed for the real value upon the first access. 

* @author peta
 */
public class ValueSentinel extends Value {

  public static final int MAX_MASK = 0xf1000000;
        
  protected ValueSentinel(short userData, VectorClock vc, long vcl, Key k, int status, Persistence p, byte type) {
    super(((0xff & userData) << 8) | (0xff & type) | MAX_MASK,0,vc,vcl,k,status);
    setPersistenceBackend(p);
  }
  
  protected ValueSentinel(int max, int len, VectorClock vc, long vcl, Key k, int status) {
    super(max,len,vc,vcl,k,status);
    assert (len == 0);
    assert (max < 0);
  }
  
  public static Value create(short userData, Persistence p, byte type) {
    return new ValueSentinel(userData, VectorClock.NOW, VectorClock.weak_jvmboot_time(),null,PERSISTED,p,type);
  }
  
  @Override public byte type() {
    return (byte)(_max & 0xff);
  }
  
  public short userData() {
    return (short)((0xffff00 & _max) >> 8);
  }
}
