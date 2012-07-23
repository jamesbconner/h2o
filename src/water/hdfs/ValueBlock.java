package water.hdfs;



public class ValueBlock {

}
/** Value for the HDFS block. 
 * 
 * In fact the HDFS block value is the same as ICE value
 *
 * @author peta
 */
/*public class ValueBlock extends ValueLocallyPersisted {
  
  private static final NonBlockingHashMapLong<String> _blockPaths = new NonBlockingHashMapLong();
  
  public static final byte VALUE_TYPE = 'B';

  static final ValueBlock ON_DISK = new ValueBlock(-2,0,VectorClock.NOW,VectorClock.weak_jvmboot_time(),null,PERSISTED);
  
  public static final String DEFAULT_ROOT = "/home/hduser/hadoop/tmp/dfs/data/current";
  
  static final File _datanodeRoot;
  
  private static final String BLOCK_PREFIX = "blk_";

  // persistence ---------------------------------------------------------------
  
  @Override protected boolean getString_impl( int len, StringBuilder sb ) {
    sb.append("[hdfs blk_"+KeyManager.getBlockFromKey(_key)+"]");
    sb.append(is_local_persist() ? "." : "!");
    return false;
  }
  
  @Override protected Value make_deleted(Key key, VectorClock vc, long weak) {
    return new ValueBlock(-1,0,vc,weak,key,0);
  }
  
  @Override protected byte type() { 
    return VALUE_TYPE;
  }
  
  // Return an Ice Value for this Key
  @Override protected Value load(Key key) {
    assert this == ON_DISK;
    VectorClock vc = VectorClock.NOW;
    return new ValueBlock(size_impl(key),0,vc.weak_vc(),vc.weak_jvmboot_time(),key,PERSISTED);
  }
  
  @Override protected File getFileForKey(Key k, byte type) {
    assert (type==VALUE_TYPE);
    String prefix = _blockPaths.get(KeyManager.getBlockFromKey(k));
    if (prefix==null)
      throw new Error("NOT_IMPLEMENTED");
    return new File(_datanodeRoot,prefix+File.separator+BLOCK_PREFIX+KeyManager.getBlockFromKey(k));
  }
  
  // initialization ------------------------------------------------------------
  
  static {
    String s = H2O.OPT_ARGS.hdfs_datanode == null ? DEFAULT_ROOT : H2O.OPT_ARGS.hdfs_datanode;
    File root = new File(s);
    if (root.exists()) {
      System.out.println("[hdfs] dead datanode found at "+root.getAbsolutePath());
      _datanodeRoot = root;
    } else {
      _datanodeRoot = null;
      System.out.println("[hdfs] no datanode found in "+root.getAbsolutePath());
    }
  }

  public static void initialize() {
    if (_datanodeRoot==null) {
      System.out.println("[hdfs] not running on datanode, skipping datanode initialization...");
      return;
    }
    System.out.println("[hdfs] loading datanode blocks...");
    loadBlocksInternally(_datanodeRoot,"");
    System.out.println("       DONE");
  }
  
  // implementation ------------------------------------------------------------  

  // cp from hadoop sources - Block.java
  private static boolean isHdfsBlock(File f) {
    String name = f.getName();
    if ( name.startsWith( BLOCK_PREFIX ) && 
        name.indexOf( '.' ) < 0 ) {
      return true;
    } else {
      return false;
    }
  }
  
  // Returns the block id from the given file.
  private static long getBlockIdFromFile(File f) {
    return Long.parseLong(f.getName().substring(4));
  }

  // Loads all hdfs blocks on the given path as internal block files. 
  private static void loadBlocksInternally(File path,String prefix) {
    assert (path.isDirectory());
    for (File f: path.listFiles()) {
      if (f.isDirectory()) {
        loadBlocksInternally(f,prefix+File.separator+f.getName());
      } else if (isHdfsBlock(f)) {
        long bid = getBlockIdFromFile(f);
        Key k = KeyManager.createInternalBlockKey(bid);
        H2O.putIfAbsent_raw(k, ON_DISK);
        k.is_local_persist(ON_DISK,null); // Register knowledge of this key on disk
        _blockPaths.put(bid,prefix);
      }
    }
  }
  
  
  
  
  
  
  
  
  public ValueBlock( int max, int len, VectorClock vc, long vcl, Key key, int mode ) {
    super(max,len,vc,vcl, key, mode);
  }
  // Make from a Key and a byte array
  public ValueBlock( Key key, int len ) { this(len, len,null,0,key,NOT_STARTED); }
  public ValueBlock( Key key, String s ) {
    this(key,s.getBytes().length); 
    byte [] sbytes = s.getBytes();
    byte [] mem = mem();
    for(int i = 0; i < mem.length; ++i){
      mem[i] = sbytes[i];
    }
  }

} */
