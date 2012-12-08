package water;

import java.io.*;


public class SerializationUtils {

  public static int wire_len(Key [] keys){
    int res = 4;
    if(keys != null){
      for(Key k:keys){
        ++res;
        if(k != null){
          res += k.wire_len();
        }
      }
    }
    return res;
  }

  public static void write(DataOutputStream dos, Key [] keys) throws IOException{
    if(keys == null)dos.writeInt(-1);
    else {
      dos.writeInt(keys.length);
      for(Key k:keys){
        dos.writeBoolean(k != null);
        if(k != null){
          k.write(dos);
        }
      }
    }
  }

  public static void write(Stream s, Key [] keys) {
    if(keys == null)s.set4(-1);
    else {
      s.set4(keys.length);
      for(Key k:keys){
        s.setz(k != null);
        if(k != null){
          k.write(s);
        }
      }
    }
  }

  public static Key [] readKeys(DataInputStream dis) throws IOException{
    int len = dis.readInt();
    if(len == -1)return null;
    Key [] res = new Key[len];
    for(int i = 0; i < len; ++i){
      if(dis.readBoolean())res[i] = Key.read(dis);
    }
    return res;
  }

  public static Key [] readKeys(Stream s) {
    int len = s.get4();
    if(len == -1)return null;
    Key [] res = new Key[len];
    for(int i = 0; i < len; ++i){
      if(s.getz())res[i] = Key.read(s);
    }
    return res;
  }

  public static int wire_len(Key [][] keys){
    int res = 4;
    if(keys != null)
      for(Key [] k:keys)
        res += wire_len(k);
    return res;
  }

  public static void write(Key [][] keys, DataOutputStream dos) throws IOException{
    if(keys == null)dos.writeInt(-1);
    else {
      dos.writeInt(keys.length);
      for(Key [] k:keys)
        write(dos,k);
    }
  }

  public static void write(Key [][] keys, Stream s) {
    if(keys == null)s.set4(-1);
    else {
      s.set4(keys.length);
      for(Key [] k:keys)
        write(s,k);
    }
  }

  public static Key [][] read2DKeyArray(DataInputStream dis) throws IOException{
    int len = dis.readInt();
    if(len == -1)return null;
    Key [][] res = new Key[len][];
    for(int i = 0; i < len; ++i)
      res[i] = readKeys(dis);
    return res;
  }

  public static Key [][] read2DKeyArray(Stream s) {
    int len = s.get4();
    if(len == -1)return null;
    Key [][] res = new Key[len][];
    for(int i = 0; i < len; ++i)
      res[i] = readKeys(s);
    return res;
  }

}
