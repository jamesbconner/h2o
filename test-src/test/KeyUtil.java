package test;
import static org.junit.Assert.*;
import com.google.common.io.Closeables;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.junit.Ignore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import water.*;
import water.parser.ParseDataset;

public class KeyUtil {
  private static int _initial_keycnt = 0;

  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] { });
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 10000) {
      if (H2O.CLOUD.size() > 2) break;
      try { Thread.sleep(100); } catch( InterruptedException ie ) {}
    }
    assertEquals("Cloud size of 3", 3, H2O.CLOUD.size());
    _initial_keycnt = H2O.store_size();
  }

  @AfterClass public static void checkLeakedKeys() {
    DKV.write_barrier();
    int leaked_keys = H2O.store_size() - _initial_keycnt;
    if( leaked_keys != 0 ) 
      for( Key k : H2O.keySet() )
        System.err.println("Leaked key: "+k);
    assertEquals("No keys leaked", 0, leaked_keys);
  }

  public static File find_test_file( String fname ) {
    // When run from eclipse, the working directory is different.
    // Try pointing at another likely place
    File file = new File(fname);
    if( !file.exists() ) file = new File("build/"+fname);
    if( !file.exists() ) file = new File("../"+fname);
    if( !file.exists() ) file = new File("../build/"+fname);
    return file;
  }

  public static Key load_test_file( String fname ) {
    return load_test_file(find_test_file(fname));
  }

  public static Key load_test_file(File file, String keyname){
    Key key = null;
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
      key = ValueArray.readPut(keyname, fis);
    } catch( IOException e ) {
      Closeables.closeQuietly(fis);
    }
    if( key == null ) fail("failed load to "+file.getName());
    return key;
  }

  public static Key load_test_file( File file ) {
    return load_test_file(file, file.getPath());
  }

  public static ValueArray parse_test_key(Key fileKey, Key parsedKey) {
    System.out.println("PARSE: " + fileKey + ", " + parsedKey);
    ParseDataset.parse(parsedKey, DKV.get(fileKey));
    return ValueArray.value(DKV.get(parsedKey));
  }
  public static ValueArray parse_test_key(Key fileKey) {
    return parse_test_key(fileKey, Key.make());
  }

  public static String replaceExtension(String fname, String newExt){
    int i = fname.lastIndexOf('.');
    if(i == -1) return fname + "." + newExt;
    return fname.substring(0,i) + "." + newExt;
  }


  public static String getHexKeyFromFile(File f){
    return replaceExtension(f.getName(),"hex");
  }

  public static String getHexKeyFromRawKey(String str){
    if(str.startsWith("hdfs://"))str = str.substring(7);
    return replaceExtension(str,"hex");
  }

}
