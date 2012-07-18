/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package water.hdfs;



public class ValueINode {
  
}

/**
 *
 * @author peta
 */
/*public class ValueINode extends ValueLocallyPersisted {
  
  // locally persisted value implementation ------------------------------------
  static final ValueINode ON_LOCAL_DISK = new ValueINode(-2,0,VectorClock.NOW,VectorClock.weak_jvmboot_time(),null,PERSISTED);

  private ValueINode( int max, int len, VectorClock vc, long vcl, Key key, int mode ) {
    super(max,len,vc,vcl, key, mode);
  }
  // Make from a Key and a byte array
  public ValueINode( Key key, int len ) { this(len ,len, null,0,key,NOT_STARTED); }
  public ValueINode( Key key, String s ) { 
    this(key,s.getBytes().length);     
    byte [] sbytes = s.getBytes();
    byte [] mem = mem();
    for(int i = 0; i < mem.length; ++i)
      mem[i] = sbytes[i];
  }

  // Make from the wire: assume not locally persisted yet
  static ValueINode make_wire(int max, int len, VectorClock vc, long vcl, Key key ) {
    return new ValueINode(max,len,vc,vcl,key,NOT_STARTED);
  }

  @Override protected byte type() { return 'N'; }

  // Return an Ice Value for this Key
  @Override protected Value load(Key key) {
    assert this == ON_LOCAL_DISK;
    VectorClock vc = VectorClock.NOW;
    return new ValueINode(size_impl(key),0,vc.weak_vc(),vc.weak_jvmboot_time(),key,PERSISTED);
  }

  @Override protected Value make_deleted(Key key, VectorClock vc, long weak) {
    return new ValueINode(-1,0,vc,weak,key,0);
  }

  // Gets local file corresponding to given key
  @Override protected File getFileForKey(Key k, byte type) {
    return null;
  }
  
  @Override protected boolean getString_impl( int len, StringBuilder sb ) {
    sb.append("[hdfs inode] ");
    sb.append(path());
    if (isDirectory()) {
      sb.append(" (DIR)");
    } else {
      sb.append(" (");
      sb.append(fileSize());
      sb.append(", blocks: ");
      sb.append(numBlocks());
      sb.append(")");
    }
    return true;
  }
  
  
  // INode record implementation in the memory ---------------------------------
  
  public static enum Elements {
    numBlocks(8),
    hdfsNumBlocks(8, Elements.numBlocks),
    replication(2, Elements.hdfsNumBlocks),
    modified(8, Elements.replication),
    accessed(8, Elements.modified),
    blockSize(8, Elements.accessed),
    nsQuota(8, Elements.blockSize),
    dsQuota(8, Elements.nsQuota),
    permissions(2, Elements.dsQuota),
    
    endOfFixedSizeData(0, Elements.permissions) // contains the end of fixed size data
    ;
    final int size;
    final int offset;
    Elements(int size,Elements last) {
      this.size = size;
      this.offset = last.offset + last.size;
    }
    Elements(int size) {
      this.size = size;
      this.offset = 0;
      
    }
  };
  
  long numBlocks() {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.numBlocks.size==8);
    return UDP.get8(mem(),Elements.numBlocks.offset);
  }

  long hdfsNumBlocks() {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.hdfsNumBlocks.size==8);
    return UDP.get8(mem(),Elements.hdfsNumBlocks.offset);
  }

  short replication() {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.replication.size==2);
    return (short)UDP.get2(mem(),Elements.replication.offset);
  }

  long modified() {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.modified.size==8);
    return UDP.get8(mem(),Elements.modified.offset);
  }

  long accessed() {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.accessed.size==8);
    return UDP.get8(mem(),Elements.accessed.offset);
  }

  long blockSize() {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.blockSize.size==8);
    return UDP.get8(mem(),Elements.blockSize.offset);
  }
  
  long nsQuota() {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.nsQuota.size==8);
    return UDP.get8(mem(),Elements.nsQuota.offset);
  }

  long dsQuota() {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.dsQuota.size==8);
    return UDP.get8(mem(),Elements.dsQuota.offset);
  }

  short permissions() {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.permissions.size==2);
    return (short)UDP.get2(mem(),Elements.permissions.offset);
  }
  
  private void setNumBlocks(long value) {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.numBlocks.size==8);
    UDP.set8(mem(),Elements.numBlocks.offset,value);
  } 

  private void setHdfsNumBlocks(long value) {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.hdfsNumBlocks.size==8);
    UDP.set8(mem(),Elements.hdfsNumBlocks.offset,value);
  } 
  
  void setReplication(short value) {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.replication.size==2);
    UDP.set2(mem(),Elements.replication.offset,value);
  }

  void setModified(long value) {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.modified.size==8);
    UDP.set8(mem(),Elements.modified.offset,value);
  }

  void setAccessed(long value) {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.accessed.size==8);
    UDP.set8(mem(),Elements.accessed.offset,value);
  }

  void setBlockSize(long value) {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.blockSize.size==8);
    UDP.set8(mem(),Elements.blockSize.offset,value);
  }
  
  void setNsQuota(long value) {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.nsQuota.size==8);
    UDP.set8(mem(),Elements.nsQuota.offset,value);
  }

  void setDsQuota(long value) {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.dsQuota.size==8);
    UDP.set8(mem(),Elements.dsQuota.offset,value);
  }

  void setPermissions(short value) {
    assert(mem().length>=Elements.endOfFixedSizeData.offset);
    assert(Elements.permissions.size==2);
    UDP.set2(mem(),Elements.permissions.offset,value);
  }
  
  private int blockOffset(int index) {
    return  Elements.endOfFixedSizeData.offset+index*BlockInfo.SIZE;
  }
  
  BlockInfo blockInfo(int index) {
    assert (index<=numBlocks());
    return new BlockInfo(mem(),blockOffset(index));
  }
  
  void setBlockInfo(int index, BlockInfo bi) {
    assert (index<=numBlocks());
    bi.store(mem(),blockOffset(index));
  }
  
  private int pathOffset() {
    return (int)(Elements.endOfFixedSizeData.offset + numBlocks()*BlockInfo.SIZE);
  }
  
  String path() {
    int offset = pathOffset();
    int size = UDP.get4(mem(),offset);
    offset +=4;
    return new String(mem(), offset, size);
  }
  
  // it shoudl never be used to change the path as it assumes the value was
  // created with this path in mind
  private void setPath(String value) {
    int offset = pathOffset();
    assert (mem().length>=(offset+4+value.length()));
    try {
      UDP.set4(mem(),offset,value.length());
    } catch (Exception e) {
      UDP.set4(mem(),offset,value.length());
    }
    System.arraycopy(value.getBytes(),0,mem(),offset+4,value.length());
  }
  
  private int groupOffset() {
    int offset = pathOffset();
    return offset + UDP.get4(mem(),offset)+4;
  }

  String group() {
    int offset = groupOffset();
    int size = UDP.get4(mem(),offset);
    offset +=4;
    return new String(mem(), offset, size);
  }
  
  // it shoudl never be used to change the group as it assumes the value was
  // created with this group in mind
  void setGroup(String value) {
    int offset = groupOffset();
    assert (mem().length>=offset+4+value.length());
    UDP.set4(mem(),offset,value.length());
    System.arraycopy(value.getBytes(),0,mem(),offset+4,value.length());
  } 

  private int userOffset() {
    int offset = groupOffset();
    return offset + UDP.get4(mem(),offset)+4;
  }

  String user() {
    int offset = userOffset();
    int size = UDP.get4(mem(),offset);
    offset +=4;
    return new String(mem(), offset, size);
  }
  
  // it shoudl never be used to change the user as it assumes the value was
  // created with this user in mind
  void setUser(String value) {
    int offset = userOffset();
    assert (mem().length==offset+4+value.length());
    UDP.set4(mem(),offset,value.length());
    System.arraycopy(value.getBytes(),0,mem(),offset+4,value.length());
  } 
  
  public boolean isDirectory() {
    return hdfsNumBlocks()==-1;
  }
  
  public long fileSize() {
    long result = 0;
    for (int i = 0; i<numBlocks();++i) {
      result += BlockInfo.readLength(mem(),blockOffset(i));
    }
    return result;
  }
  
  public static ValueINode make(DataInputStream is) throws IOException {
    String path;
    short replication;
    long modified;
    long accessed;
    long blockSize;
    BlockInfo[] blocks;
    long nsQuota = 0;
    long dsQuota = 0;
    String user;
    String group;
    short permissions;

    path = Utils.readShortString(is);
    replication = is.readShort();
    modified = is.readLong();
    // load accessed time
    if (_hdfs_imgVersion <= -17)  
      accessed = is.readLong();
    else
      accessed = 0;
    // load block size
    if (_hdfs_imgVersion <= -8)  
      blockSize = is.readLong();
    else 
      blockSize = 0;
    // load the blocks
    int numBlocks = is.readInt();
    // WTF the >= 0 ??? 
    if ((numBlocks>0) || ((_hdfs_imgVersion < -9) && numBlocks ==0)) {
      //
      // file
      //
      blocks = new BlockInfo[numBlocks];
      for (int i = 0; i<numBlocks; ++i) 
        blocks[i] = new BlockInfo(is);
      // update the blockSize if not set by the protocol
      if (-8 <=_hdfs_imgVersion) {
        assert (blockSize == 0);
        if (blocks == null) {
          blockSize = DEFAULT_BLOCK_SIZE;
        } else {
          if (blocks.length>0) {
            blockSize = blocks[0].length;
          } else {
            blockSize = Math.max(blocks[0].length,DEFAULT_BLOCK_SIZE);
          }
        }
      }
    } else {
      // 
      // directory
      //
      blocks = null;
      // Depending on the version loads the ns quota
      if (_hdfs_imgVersion <= -16)  
        nsQuota = is.readLong();
      else
        nsQuota = -1;
      // Depending on the version loads the ds quota
      if (_hdfs_imgVersion <= -18)  
        dsQuota = is.readLong();
      else
        dsQuota = -1;
    }
    // Depending on the version, loads the permissions of the inode
    if (_hdfs_imgVersion <= -11) {
      user = Utils.readVString(is);
      group = Utils.readVString(is);
      permissions = is.readShort();
    } else {
      user = null;
      group = null;
      permissions = DEFAULT_PERMISSIONS;
    }
    //
    // Loaded - create the value
    //
    int totalSize = 12 + path.length() + user.length() + group.length() + Elements.endOfFixedSizeData.offset;
    if (blocks!=null)
      totalSize += blocks.length * BlockInfo.SIZE;
    
    // only namenode can create the inode from data input stream
    Key key = KeyManager.createINodeKey(path, H2O.SELF);
    ValueINode value = new ValueINode(key,totalSize);
    value.init_weak_vector_clock(VectorClock.NOW);
    // set the value's memory array
    value.setNumBlocks(blocks==null ? 0 : blocks.length);
    value.setHdfsNumBlocks(numBlocks);
    value.setReplication(replication);
    value.setModified(modified);
    value.setAccessed(accessed);
    value.setBlockSize(blockSize);
    value.setNsQuota(nsQuota);
    value.setDsQuota(dsQuota);
    value.setPermissions(permissions);
    if (blocks!=null) 
      for (int i = 0; i<blocks.length; ++i)
        value.setBlockInfo(i,blocks[i]);
    value.setPath(path);
    value.setGroup(group);
    value.setUser(user);
    // we have all, return the value
    return value;
  }
    
  
  static H2ONode _namenode = null;
  
  public static final byte VALUE_TYPE = 'N';

  static final ValueINode ON_DISK = null; // new ValueHdfsINode(-2,null,VectorClock.NOW,VectorClock.weak_jvmboot_time(),null,PERSISTED);
  
  public static final String DEFAULT_ROOT = "/home/hduser/hadoop/tmp/dfs/name/current";
  
  static final File _namenodeFsImage;

  // hdfs fsimage state information --------------------------------------------
  
  static int _hdfs_imgVersion;
  static int _hdfs_namespaceID;
  static long _hdfs_genStamp;
  
  static SecretManager _manager;
  
  // hdfs configuration variables ----------------------------------------------
  
  public static long DEFAULT_BLOCK_SIZE = 0;
  public static short DEFAULT_PERMISSIONS = 0;
  
  // initialization ------------------------------------------------------------
  
  static {
    String s = H2O.OPT_ARGS.hdfs_datanode == null ? DEFAULT_ROOT : H2O.OPT_ARGS.hdfs_datanode;
    File root = new File(s+File.separator+"fsimage");
    if (root.exists()) {
      System.out.println("[hdfs] dead namenode found at "+root.getAbsolutePath());
      _namenodeFsImage = root;
    } else {
      _namenodeFsImage = null;
      System.out.println("[hdfs] no namenode found in "+root.getAbsolutePath());
    }
  }
  
  public static void initialize() {
    if (_namenodeFsImage==null) // don't do anything if we are not the namenode
      return;
    // set the namenode to itself so that it gets broadcasted to the cloud
    // load the filesystem image and create the INode keys
    _namenode = H2O.SELF;
    System.out.println("[hdfs] reading fsimage...");
    if (loadFsImage()) {
      System.out.println("       DONE");
      water.H2O.SELF.set_node_type(H2ONode.HDFS_NAMENODE);
      System.out.println("[hdfs] heartbeat updated for the namenode information");
    } else {
      _namenode = null; // revert
    }
  }
  
  
  private static boolean loadFsImage() {
    DataInputStream is = null;
    try {
      is = new DataInputStream(new FileInputStream(_namenodeFsImage));
      _hdfs_imgVersion = is.readInt();
      System.out.println("[hdfs] fsimage version "+_hdfs_imgVersion);
      _hdfs_namespaceID = is.readInt();
      System.out.println("[hdfs] fsimage version "+_hdfs_namespaceID);
      long numFiles = loadNumberOfFiles(is);
      System.out.println("[hdfs] number of files "+numFiles);
      loadGenerationStamp(is);
      for (long i = 0; i<numFiles; ++i) {
        ValueINode value = ValueINode.make(is);
        H2O.putIfAbsent_raw(value._key, value);
        assert (value._key != null);
      }
      loadDatanodeInformation(is);
      loadFilesUnderConstruction(is);
      loadSecretManagerState(is);
      try {
        is.readByte();
        Log.die("[hdfs] not all fsimage is understood. Maybe incompatible hdfs version.");
      } catch (IOException e) {
        // pass
      }
      
      
      return true;
    } catch (FileNotFoundException e) {
      System.out.println("[hdfs] unable to read the hdfs fsimage.");
    } catch (IOException e) {
      Log.die("[hdfs] corrupted fsimage file or incompatible hdfs version.");
    } finally {
      if (is!=null)
        try { is.close(); } catch (IOException e) { }
    }
    return false;
  }

  // fsimage load helpers ------------------------------------------------------
  
  private static long loadNumberOfFiles(DataInputStream in) throws IOException {
      if (_hdfs_imgVersion <= -16) {
        return in.readLong();
      } else {
        return in.readInt();
      }
  }
  
  private static void loadGenerationStamp(DataInputStream in) throws IOException {
    if (_hdfs_imgVersion <= -12)
      _hdfs_genStamp = in.readLong();
    else
      _hdfs_genStamp = 0;
  }

  private static void loadDatanodeInformation(DataInputStream in) throws IOException {
    if ((_hdfs_imgVersion > -3) || (_hdfs_imgVersion <=-12))
      return;
    System.out.println("[hdfs] Datanode information is present, but is discarded...");
    int size = in.readInt();
    while (size>0) {
      Utils.readShortString(in); // name
      Utils.readShortString(in); // storage id
      in.readShort(); // infoPort
      in.readLong(); // capacity
      in.readLong(); // remaining
      in.readLong(); // lastUpdate
      in.readInt(); // xceiverCount
    }
    // PETA We might want to keep this information somewhere and put it back at
    // the end although it seems it is not necessary
  }
  
  private static void loadFilesUnderConstruction(DataInputStream in) throws IOException {
    if (_hdfs_imgVersion > -13)
      return;
    if (in.readInt()!=0) {
      System.out.println("[hdfs] Unable to process filesystem image with files under construction.");
      Log.die("[hdfs] Make sure that HDFS is left in clean state before running H2O.");
    }
  }
  
  private static void loadSecretManagerState(DataInputStream in) throws IOException {
    if (_hdfs_imgVersion > -19) {
      _manager = null;
      return;
    }
    System.out.println("[hdfs] Loading secret manager...");
    _manager = new SecretManager(in);
  }
  
} */
