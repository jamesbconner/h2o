/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water;

/** Deleted value indicator.
 * 
 * Used in place for deleted values. 
 *
 * @author peta
 */
public class ValueDeleted extends ValueSentinel {
  
  public static final int MAX_MASK = 0xf2000000;
  
  private ValueDeleted(short userData, Key k, int status, Persistence p, byte type) {
    super(((0xff & userData) << 8) | (0xff & type) | MAX_MASK,0,k,status);
    setPersistenceBackend(p);
  }

  protected ValueDeleted(int max, int len, Key k, int status) {
    super(max,len,k,status);
  }
  
  public static Value create(short userData, Key k, Persistence p, byte type) {
    return new ValueDeleted(userData,k,NOT_STARTED,p,type);
  }
  
}
