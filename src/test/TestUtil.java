package test;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.junit.Ignore;

import water.DKV;
import water.Key;
import water.ValueArray;
import water.parser.ParseDataset;

@Ignore
public class TestUtil {
  public static File find_test_file( String fname ) {
    // When run from eclipse, the working directory is different.
    // Try pointing at another likely place
    File file = new File(fname);
    if( !file.exists() ) file = new File("build/"+fname);
    if( !file.exists() ) file = new File("../"+fname);
    return file;
  }

  public static Key load_test_file( String fname ) { 
    return load_test_file(find_test_file(fname)); 
  }

  public static Key load_test_file( File file ) {
    Key key = null;
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
      key = ValueArray.read_put_file(file.getPath(), fis, (byte)0);
    } catch( IOException e ) {
      try { if( fis != null ) fis.close(); } catch( IOException e2 ) { }
    }
    if( key == null ) fail("failed load to "+file.getName());
    return key;
  }
  
  public static ValueArray parse_test_key(Key fileKey) {
    Key parsedKey = Key.make();
    ParseDataset.parse(parsedKey, DKV.get(fileKey));
    return (ValueArray) DKV.get(parsedKey); 
  }
}
