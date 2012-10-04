package water;

import java.util.Arrays;

/**
 * Atomic Append of a Key
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class AppendKey extends Atomic {
  private byte[] _keybits;

  public AppendKey() { }                // nullary constructor for serialization
  public AppendKey( byte[] bits ) { _keybits = bits; } // Constructor for tests
  public AppendKey( Key keyToAppend ) { // Append a key
    _keybits = new byte[keyToAppend.wire_len()];
    keyToAppend.write(_keybits,0);
  }

  // Just append the bits
  @Override public byte[] atomic( byte[] bits1 ) {
    if( bits1 == null ) bits1 = new byte[4]; // Include the key count of zero
    byte[] bits2 = Arrays.copyOf(bits1,bits1.length+_keybits.length);
    System.arraycopy(_keybits,0,bits2,bits1.length,_keybits.length);
    UDP.set4(bits2,0,UDP.get4(bits1,0)+1); // Increment key count
    return bits2;
  }
  // Do not return the bits, so on success zap the array.
  @Override public void onSuccess() { _keybits = null; }
}
