package hexlytics.rf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import water.Arguments;
import water.util.KeyUtil;

/**
 * Testing RF. Launches RF (in a new vm) for files specified in args and records
 * results.
 */
public class RFRunner {
  static final long MAX_RUNNING_TIME = 30 * 60000; // max runtime is 30 mins
  static final int ERROR_IDX = 2;
  static final Pattern[] RESULT = new Pattern[] {
      Pattern.compile("Number of trees:[ ]*([0-9]+)"),
      Pattern.compile("No of variables tried at each split:[ ]*([0-9]+)"),
      Pattern.compile("Estimate of error rate:[ ]*([0-9]+)"),
      Pattern.compile("Avg tree depth \\(min, max\\):[ ]*([0-9]+).*"),
      Pattern.compile("Avg tree leaves \\(min, max\\):[ ]*([0-9]+).*"),
      Pattern.compile("Validated on \\(rows\\):[ ]*([0-9]+).*"),
      Pattern.compile("Random forest finished in:[ ]*(.*)"), };

  static final Pattern EXCEPTION = Pattern
      .compile("Exception in thread \"(.*\") (.*)");
  static final Pattern ERROR = Pattern.compile("java.lang.(.*?Error.*)");
  static final Pattern MEM_CRICITAL = Pattern
      .compile("[h20] MEMORY LEVEL CRITICAL, stopping allocations");

  static final String[] RESULTS = new String[] { "ntrees", "nvars", "err",
      "avg depth", "avg leaves", "N rows", "RunTime" };

  /**
   * Represents spawned process with H2O running RF. Hooks stdout and stderr and
   * looks for exceptions (failed run) and results.
   */
  static class RFProcess extends Thread {
    Process _process;
    BufferedReader _rd;
    volatile String _line;
    String[] results = new String[RESULTS.length];
    String exception;
    PrintStream _stdout = System.out;

    /* Creates RFPRocess and spawns new process. */
    public RFProcess(String jarPath, String vmArgs, String h2oArgs,
        String rfArgs) throws Exception {
      List<String> c = new ArrayList<String>();
      c.add("java");
      for( String s : vmArgs.split(" ") )
        if( !s.isEmpty() )
          c.add(s.startsWith("-") ? s : "-" + s);
      if( vmArgs.contains("jar") )
        throw new Error("should not contain -jar!");
      c.add("-jar");
      c.add(jarPath);
      c.add("-mainClass");
      c.add("hexlytics.rf.RandomForest");
      for( String s : rfArgs.split(" ") )
        if( !s.isEmpty() )
          c.add(s);
      if( h2oArgs != null && !h2oArgs.isEmpty() ) {
        c.add("-h2oArgs");
        c.add(h2oArgs);
      }
      String s = "";
      for( String str : c )
        s += str + " ";
      System.out.println("Command = '" + s + "'");
      ProcessBuilder bldr = new ProcessBuilder(c);
      bldr.redirectErrorStream(true);
      _process = bldr.start();
      _rd = new BufferedReader(new InputStreamReader(_process.getInputStream()));
    }

    /** Kill the spawned process. And kill the thread. */
    void cleanup() {
      _process.destroy();
      try {
        _process.waitFor();
      } catch( InterruptedException e ) {
    }
    }

    /**
     * Read stdout and stderr of the spawned process. Read by lines and print
     * them to our stdout. Look for exceptions (fail) and result (error rate and
     * running time).
     */
    @Override
    public void run() {
      try {
        int state = 0;
        int memCriticals = 0;
        while( (_line = _rd.readLine()) != null ) {
          _stdout.println(_line);
          Matcher m = RESULT[state].matcher(_line);
          if( m.find() ) {
            results[state] = m.group(1);
            if( ++state == RESULT.length ) {
              System.out.println("Error: " + results[ERROR_IDX] + "%");
              break;
            }
          }
          m = EXCEPTION.matcher(_line);
          if( m.find() ) { // exception has been thrown -> fail!
            exception = "thread=" + m.group(1) + ", exception = " + m.group(2);
            break;
          }
          m = ERROR.matcher(_line);
          if( m.find() ) { // exception has been thrown -> fail!
            exception = m.group(1);
            break;
        }
          if( MEM_CRICITAL.matcher(_line).find() ) {
            if( ++memCriticals == 10 ) { // unlikely to recover, but can waste
                                         // lot fo time, kill it
              exception = "LOW MEMORY";
              break;
    }
          } else
            memCriticals = 0;
  }
      } catch( Exception e ) {
        throw new Error(e);
      }
    }
  }

  static final InetAddress myAddr;

  static {
    try {
      myAddr = InetAddress.getLocalHost();
    } catch( UnknownHostException e ) {
      throw new Error(e);
  }
  }
  
  /** look for input files. If the path ends with '*' all files will be used. */
  public static Collection<File> parseDatasetArg(String str) {
    ArrayList<File> files = new ArrayList<File>();
    StringTokenizer tk = new StringTokenizer(str, ",");
    while( tk.hasMoreTokens() ) {
      String path = tk.nextToken();
      if( path.endsWith("*") ) {
        path = path.substring(0, path.length() - 1);
        File f = KeyUtil.find_test_file(path);
        if( !f.isDirectory() )
          throw new Error("invalid path '" + path + "*'");
        for( File x : f.listFiles() ) {
          if( x.isFile()
              && (x.getName().endsWith(".csv") || x.getName().endsWith(".data")) )
            files.add(x);
          else if( x.isDirectory() )
            files.addAll(parseDatasetArg(x.getAbsolutePath() + "*"));
        }
      } else
        files.add(KeyUtil.find_test_file(path));
    }
    return files;
  }

  public static class OptArgs extends Arguments.Opt {
    public String h2ojar = "build/h2o.jar"; // path to the h2o.jar
    public String files = "smalldata/poker/poker-hand-testing.data"; // dataset
    public String fileConcats = "1,4,8,12,16,24,32,64,128";
    public String rawKeys;
    public String parsedKeys;
    public String validation;
    public String h2oArgs = ""; // args for the spawned h2o
    public String jvmArgs = " -Xmx4g"; // args for the spawned jvm
    public String rfArgs = ""; // args for RF
    public String resultDB = "./results.csv"; // path to the file with the
                                              // results
    public String nodes = myAddr.getHostAddress(); // list of nodes, currently
                                                   // ignored
    public int nseeds = 2;
    public String testCfgFile;
  }

  public static final OptArgs ARGS = new OptArgs();

  static PrintStream stdout = System.out;
  
  static void runAllDataSizes(File f, int[] multiples, RFArgs rfa)
      throws Exception {
    String fname = f.getAbsolutePath();
    String extension = "";
    int idx = fname.lastIndexOf('.');
    if( idx != -1 ) {
      extension = fname.substring(idx);
      fname = fname.substring(0, idx);
    }
    File f2 = new File(fname + "_" + extension);
    f2.createNewFile();
    f2.deleteOnExit();
    StringBuilder bldr = new StringBuilder();
    Reader r = new FileReader(f);
    char[] buf = new char[1024 * 1024];
    int n = 0;
    while( (n = r.read(buf)) > 0 )
      bldr.append(buf, 0, n);
    r.close();
    String content = bldr.toString();
    bldr = null;
    int prevMultiple = 0;
    for( int m : multiples ) {
      FileWriter fw = new FileWriter(f2, true);
      for( int i = prevMultiple; i != m; ++i )
      fw.write(content);
      fw.close();      
      prevMultiple = m;
      File frenamed = new File(fname + "_" + m + extension);
      f2.renameTo(frenamed);
      f2 = frenamed;
      rfa.file = frenamed.getAbsolutePath();
      runTest(ARGS.h2ojar, ARGS.jvmArgs, ARGS.h2oArgs, rfa.toString(),
          ARGS.resultDB, stdout);
    }      
  }
  
  static boolean runTest(String h2oJar, String jvmArgs, String h2oArgs,
      String rfArgs, String resultDB, PrintStream stdout) throws Exception {
    RFProcess p = new RFProcess(h2oJar, jvmArgs, h2oArgs, rfArgs);
    p._stdout = stdout;
    p.start();
    p.join(MAX_RUNNING_TIME);
    p.cleanup();
    p.join(); // in case we timed out...
    
      File resultFile = new File(resultDB);
      boolean writeColumnNames = !resultFile.exists();
      FileWriter fw = new FileWriter(resultFile, true);
      if( writeColumnNames ) {
        fw.write("Timestamp, JVM args, H2O args, RF args");
      for( String s : RESULTS )
        fw.write(", " + s);
        fw.write("\n");
      }
    fw.write(new SimpleDateFormat("yyyy.MM.dd HH:mm").format(new Date(System
        .currentTimeMillis())) + ",");
      fw.write(jvmArgs + "," + h2oArgs + "," + rfArgs);
    if( p.exception != null ) {
      System.err.println("Error: " + p.exception);
      fw.write("," + p.exception);
    } else
      for( String s : p.results )
        fw.write("," + s);
      fw.write("\n");
      fw.close();

    return true;
    }

  private static int seed() {
    return (int) (System.currentTimeMillis() & 0xFFFF);
  }

  final static String[] stat_types = new String[] { "gini", "entropy" };
  final static int[] ntrees = new int[] { 1, 50, 100 };
  
  static void runTests() throws Exception {
    PrintStream out = new PrintStream(new File("RFRunner.stdout.txt"));
    stdout = out;
    try {
    RFArgs rfa = new RFArgs();
      String[] keyInputs = (ARGS.parsedKeys != null) ? ARGS.parsedKeys
          .split(",") : null;
      String[] fileInputs = (ARGS.files != null) ? ARGS.files.split(",") : null;
      String[] rawKeyInputs = (ARGS.rawKeys != null) ? ARGS.rawKeys.split(",")
          : null;
      for( String statType : stat_types ) {
        for( int n : ntrees ) {
          rfa.seed = seed();
          rfa.singlethreaded = true;
          rfa.statType = statType;
          rfa.ntrees = n;
          if( keyInputs != null )
            for( String s : keyInputs ) {
              rfa.parsedKey = s;
              runTest(ARGS.h2ojar, ARGS.jvmArgs, ARGS.h2oArgs, rfa.toString(),
                  ARGS.resultDB, out);
              Thread.sleep(1000);
          }
          rfa.parsedKey = null;
          if( rawKeyInputs != null )
            for( String s : rawKeyInputs ) {
              rfa.rawKey = s;
              runTest(ARGS.h2ojar, ARGS.jvmArgs, ARGS.h2oArgs, rfa.toString(),
                  ARGS.resultDB, out);
              Thread.sleep(1000);
            }
          rfa.rawKey = null;
          if( fileInputs != null )
            for( String s : fileInputs ) {
              rfa.file = s;
              String[] strMultiples = ARGS.fileConcats.split(",");
              int[] concatCounts = new int[strMultiples.length];
              int idx = 0;
              for( String x : strMultiples )
                concatCounts[idx++] = Integer.valueOf(x);
              runAllDataSizes(new File(s), concatCounts, rfa);
              Thread.sleep(1000);
        }
      }
    }
    } finally {
    out.close();
  }
  }

  public static class RFArgs extends Arguments.Opt {
    String file;
    public String parsedKey;
    String rawKey;
    String validationFile;
    int ntrees = 10;
    int depth = -1;
    double cutRate = 0;
    String statType = "entropy";
    int seed = 42;
    boolean singlethreaded;    

    @Override
    public String toString() {
      StringBuilder bldr = new StringBuilder();
      if( file != null )
        bldr.append(" -file=" + file);
      if( parsedKey != null )
        bldr.append(" -parsedKey=" + parsedKey);
      if( rawKey != null )
        bldr.append(" -rawKey=" + rawKey);
      if( validationFile != null )
        bldr.append(" -validationFile=" + validationFile);
      bldr.append(" -ntrees=" + ntrees);
      bldr.append(" -depth=" + depth);
      bldr.append(" -cutRate=" + cutRate);
      bldr.append(" -statType=" + statType);
      bldr.append(" -seed=" + seed);
      if( singlethreaded )
        bldr.append(" -singleThreaded");
      return bldr.toString();
  }
  }
  
  public static void main(String[] args) throws Exception {
    // parse args
    Arguments arguments = new Arguments(args);
    arguments.extract(ARGS);
    File flatfile = null;
    if( !ARGS.nodes.equals("all") ) { // create a flatfile so that we do not get
                                      // interference from other nodes
      flatfile = new File("flatfile" + Math.round(100000 * Math.random()));
      flatfile.deleteOnExit();
      FileWriter fw = new FileWriter(flatfile);
      StringTokenizer tk = new StringTokenizer(ARGS.nodes, ",");
      while( tk.hasMoreTokens() )
        fw.write(tk.nextToken() + "\n");
      fw.close();
      if( !ARGS.h2oArgs.contains("-flatfile") )
        ARGS.h2oArgs += " -flatfile " + flatfile.getAbsolutePath();
    }
    runTests();
  }
}
