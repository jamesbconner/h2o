package water.web;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;

import water.*;

public class ImportFolder extends H2OPage {


  public static class FolderIntegrityChecker extends DRemoteTask {

    String[] _files;
    long[] _sizes;
    short[] _ok;
    int _nodes;
    int _stringSizes;

    ArrayList<File> _filesInProgress;

    @Override public void compute() {
      _ok = new short[_files.length];
      for (int i = 0; i < _files.length; ++i) {
        File f = new File(_files[i]);
        if (f.exists() && (f.length()==_sizes[i]))
          _ok[i] = 1;
      }
      ++_nodes;
      tryComplete();
    }

    @Override public void reduce(DRemoteTask drt) {
      FolderIntegrityChecker other = (FolderIntegrityChecker) drt;
      if (_ok == null)
        _ok = other._ok;
      else
        for (int i = 0; i < _ok.length; ++i)
          _ok[i] += other._ok[i];
      _nodes += other._nodes;
    }

    private void addFolder(File folder) {
      if (folder.isDirectory()) {
        for (File f: folder.listFiles()) {
          if (f.isDirectory())
            addFolder(f);
          else
            _filesInProgress.add(f);
        }
      } else {
        _filesInProgress.add(folder);
      }
    }


    public FolderIntegrityChecker(File root) {
      _filesInProgress = new ArrayList();
      addFolder(root);
      _files = new String[_filesInProgress.size()];
      _sizes = new long[_filesInProgress.size()];
      _stringSizes = 0;
      for (int i = 0; i < _files.length; ++i) {
        File f = _filesInProgress.get(i);
        _files[i] = f.getAbsolutePath();
        _sizes[i] = f.length();
        _stringSizes += _files[i].length();
      }
      _filesInProgress = null;
      _nodes = 0;
    }

    private Key importFile(int i) {
      if (_ok[i] == _nodes) {
        File f = new File(_files[i]);
        Key k = PersistNFS.decodeFile(f);
        long size = f.length();
        Value val = (size < 2*ValueArray.chunk_size())
          ? new Value((int)size,0,k,Value.NFS)
          : new ValueArray(k,size,Value.NFS);
        val.setdsk();
        UKV.put(k,val);
        return k;
      } else {
        return null;
      }
    }

    public String importFilesHTML() {
      StringBuilder sb = new StringBuilder();
      int correct = 0;
      for (int i = 0; i < _files.length; ++i) {
        Key k = importFile(i);
        if (k == null) {
          sb.append(error("File <strong>"+_files[i]+"</strong> does not have the same size on all nodes."));
        } else {
          RString html = new RString("File <strong>%File</strong> imported as" +
          		" key <a href='/Inspect?Key=%$Key'>%Key</a>");
          html.replace("File", _files[i]);
          html.replace("Key", k);
          sb.append(success(html.toString()));
          ++correct;
        }
      }
      return "Out of "+_files.length+" a total of "+correct+" was successfully imported to the cloud."+sb.toString();
    }

    public FolderIntegrityChecker() {

    }

    @Override public int wire_len() {
      return super.wire_len() + 4 + 4 + 4 +_stringSizes + _files.length*(2+8+2);
    }

    @Override public void write(DataOutputStream os) throws IOException {
      super.write(os);
      os.writeInt(_files.length);
      os.writeInt(_stringSizes);
      os.writeInt(_nodes);
      for (int i = 0; i < _files.length; ++i) {
        os.writeShort(_files[i].length());
        os.write(_files[i].getBytes());
        os.writeLong(_sizes[i]);
        os.writeShort(_ok == null ? 0 : _ok[i]);
      }
    }

    @Override public void write(Stream s) {
      super.write(s);
      s.set4(_files.length);
      s.set4(_stringSizes);
      s.set4(_nodes);
      for (int i = 0; i < _files.length; ++i) {
        s.set2(_files[i].length());
        s.setBytes(_files[i].getBytes(),_files[i].length());
        s.set8(_sizes[i]);
        s.set2(_ok == null ? 0 : _ok[i]);
      }
    }

    @Override public void read(DataInputStream is) throws IOException {
      super.read(is);
      int fLength = is.readInt();
      _stringSizes = is.readInt();
      _nodes = is.readInt();
      _files = new String[fLength];
      _sizes = new long[fLength];
      _ok = new short[fLength];
      byte[] buffer = new byte[1000];
      for (int i = 0; i < fLength; ++i) {
        int ss = is.readShort();
        if (ss>buffer.length)
          buffer = new byte[ss];
        is.read(buffer, 0, ss);
        _files[i] = new String(buffer, 0, ss);
        _sizes[i] = is.readLong();
        _ok[i] = is.readShort();
      }
    }

    @Override public void read(Stream s) {
      super.read(s);
      int fLength = s.get4();
      _stringSizes = s.get4();
      _nodes = s.get4();
      _files = new String[fLength];
      _sizes = new long[fLength];
      _ok = new short[fLength];
      byte[] buffer = new byte[1000];
      for (int i = 0; i < fLength; ++i) {
        int ss = s.get2();
        if (ss>buffer.length)
          buffer = new byte[ss];
        s.getBytes(buffer,ss);
        _files[i] = new String(buffer, 0, ss);
        _sizes[i] = s.get8();
        _ok[i] = (short) s.get2();
      }
    }
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) {
    try {
      String folder = args.getProperty("Folder");
      int rf = getAsNumber(args, "RF", Key.DEFAULT_DESIRED_REPLICA_FACTOR);
      if ((rf<0) || (rf>127))
        throw new Exception("Replication factor must be from 0 to 127.");

      File root = new File(folder);
      if (!root.exists())
        throw new Exception("Unable to import folder "+folder+". Folder not found.");
      folder = root.getCanonicalPath();
      FolderIntegrityChecker checker = new FolderIntegrityChecker(new File(folder));
      checker.invokeOnAllNodes();
      return checker.importFilesHTML();
    } catch (Exception e) {
      return error(e.toString());
    }
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Folder" };
  }
}
