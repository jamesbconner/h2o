package water.serialization;

import java.io.*;

import water.Stream;

public abstract class RTSerGenHelpers {
  public static int lenIntArray(int[] ints) {
    return 1 + (ints != null ? 4 + 4*ints.length : 0);
  }

  public static void writeIntArray(Stream s, int[] ints) {
    if(ints == null) s.set1(0);
    else {
      s.set1(1);
      s.set4(ints.length);
      for( int i : ints ) s.set4(i);
    }
  }

  public static int[] readIntArray(Stream s) {
    if(s.get1() == 0) return null;
    int[] ints = new int[s.get4()];
    for(int i = 0; i < ints.length; ++i) ints[i] = s.get4();
    return ints;
  }

  public static void writeIntArray(DataOutputStream s, int[] ints) throws IOException {
    if(ints == null) s.writeByte(0);
    else {
      s.writeByte(1);
      s.writeInt(ints.length);
      for( int i : ints ) s.writeInt(i);
    }
  }

  public static int[] readIntArray(DataInputStream s) throws IOException {
    if(s.readByte() == 0) return null;
    int[] ints = new int[s.readInt()];
    for(int i = 0; i < ints.length; ++i) ints[i] = s.readInt();
    return ints;
  }

}
