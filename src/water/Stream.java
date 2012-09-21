package water;
import java.util.Arrays;

/**
 * A Dumb-Ass byte[]-backed Stream, because Java Got It Wrong.
 * Sorta like a ByteArray[Out,In]putStream (both, at once)
 * but includes read/write 1/2/4/8-byte sized thingys like a
 * Data[Out,In]putStream - but does not throw exceptions...
 * AND uses native X86 byte-order.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class Stream {
  public byte[] _buf;
  public int _off;
  public Stream()                    { _buf = new byte[4];   }
  public Stream(int len)             { _buf = new byte[len]; }
  public Stream( byte[] b )          { _buf = b;             }
  public Stream( byte[] b, int off ) { _buf = b; _off = off; }

  public  byte[] grow ( int l ) { return (_off+l < _buf.length) ? _buf : grow2(_off+l);  }
  private byte[] grow2( int l ) {
    int l2 = _buf.length;
    while( l2 < l ) l2<<=1;
    return (_buf = Arrays.copyOf(_buf,l2));
  }

  public Stream set1 ( int   a) {        grow(1)[_off++] = (byte)a ; return this; }
  public Stream set2 ( int   a) { _off += UDP.set2 (grow(2),_off,a); return this; }
  public Stream set3 ( int   a) { _off += UDP.set3 (grow(3),_off,a); return this; }
  public Stream set4 ( int   a) { _off += UDP.set4 (grow(4),_off,a); return this; }
  public Stream set4f( float a) { _off += UDP.set4f(grow(4),_off,a); return this; }
  public Stream set8 ( long  a) { _off += UDP.set8 (grow(8),_off,a); return this; }
  public Stream set8d(double a) { _off += UDP.set8d(grow(8),_off,a); return this; }

  @SuppressWarnings("deprecation") public Stream setLen2Str(String s) {
      assert s.length() < 65535;
      grow(2+s.length());
      set2(s.length());
      s.getBytes(0, s.length(), _buf, _off);
      _off += s.length();
      return this;
  }

  public Stream setLen2Bytes(byte[] b) {
      assert b.length < 65535;
      grow(2+b.length);
      set2(b.length);
      System.arraycopy(b, 0, _buf, _off, b.length);
      _off += b.length;
      return this;
  }

  public Stream setLen4Bytes(byte[] b) {
      grow(4+b.length);
      set4(b.length);
      System.arraycopy(b, 0, _buf, _off, b.length);
      _off += b.length;
      return this;
  }

  public void setBytes(byte[] b, int len) {
    grow(len);
    System.arraycopy(b, 0, _buf, _off, len);
    _off += len;
  }

  public byte[] trim() { return Arrays.copyOf(_buf,_off); }

  public byte   get1 () { return           _buf[ _off++]    ; }
  public int    get2 () { return UDP.get2 (_buf,(_off+=2)-2); }
  public int    get3 () { return UDP.get3 (_buf,(_off+=3)-3); }
  public int    get4 () { return UDP.get4 (_buf,(_off+=4)-4); }
  public float  get4f() { return UDP.get4f(_buf,(_off+=4)-4); }
  public long   get8 () { return UDP.get8 (_buf,(_off+=8)-8); }
  public double get8d() { return UDP.get8d(_buf,(_off+=8)-8); }

  public String getLen2Str()   {
    int l = get2(), o = _off; _off += l;
    return new String        (_buf, o, l);
  }
  public byte[] getLen2Bytes() { int l = get2(); return Arrays.copyOfRange(_buf, _off, _off += l); }
  public byte[] getLen4Bytes() { int l = get4(); return Arrays.copyOfRange(_buf, _off, _off += l); }
}
