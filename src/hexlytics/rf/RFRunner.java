package hexlytics.rf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import water.Arguments;
import water.util.KeyUtil;

/**
 * Launch RF in a new vm and records  results.
 */
public class RFRunner {

  static final long MAX_RUNNING_TIME = 20 * 60000; // max runtime is 20 mins
  static final int ERROR_IDX = 2;

  static final Pattern[] RESULT = new Pattern[] {
      Pattern.compile("Number of trees:[ ]*([0-9]+)"),
      Pattern.compile("No of variables tried at each split:[ ]*([0-9]+)"),
      Pattern.compile("Estimate of error rate:[ ]*([0-9]+)"),
      Pattern.compile("Avg tree depth \\(min, max\\):[ ]*([0-9]+).*"),
      Pattern.compile("Avg tree leaves \\(min, max\\):[ ]*([0-9]+).*"),
      Pattern.compile("Validated on \\(rows\\):[ ]*([0-9]+).*"),
      Pattern.compile("Random forest finished in:[ ]*(.*)"), };
  static final Pattern EXCEPTION = Pattern.compile("Exception in thread \"(.*\") (.*)");
  static final Pattern ERROR = Pattern.compile("java.lang.(.*?Error.*)");
  static final Pattern MEM_CRICITAL = Pattern.compile("[h20] MEMORY LEVEL CRITICAL, stopping allocations");
  static final String[] RESULTS = new String[] { "ntrees", "nvars", "err", "avg.depth", "avg.leaves", "rows", "time" };

  static final String JAVA         = "java";
  static final String JAR          = "-jar build/h2o.jar";
  static final String MAIN         = "-mainClass hexlytics.rf.RandomForest";
  static final String[] stat_types = new String[] { "gini", "entropy" };
  static final int[] ntrees        = new int[] { 1, 50, 100, 300 };
  static final int[] sizeMultiples = new int[]{ 1, 4, 12, 24, 32, 64};
  static PrintStream stdout        = System.out;


  static class RFArgs extends Arguments.Opt {
    String file;                // data
    String parsedKey;           //
    String rawKey;              //
    String validationFile;      // validation data
    int ntrees = 10;            // number of trees
    int depth = -1;             // max depth of trees
    double cutRate = 0;         // min purity of a node
    String statType = "entropy";// split type
    int seed = 42;              // seed
    boolean singlethreaded;     // multi threaded
  }

  static class OptArgs extends Arguments.Opt {
    String files = "smalldata/poker/poker-hand-testing.data"; // dataset
    int     maxConcat = 16;                                    // how big should we go?
    String rawKeys;
    String parsedKeys;
    String h2oArgs = "";                                      // args for the spawned h2o
    String jvmArgs = " -Xmx4g";                               // args for the spawned jvm
    String resultDB = "/tmp/results.csv";                     // output file
  }

  /**
   * Represents spawned process with H2O running RF. Hooks stdout and stderr and
   * looks for exceptions (failed run) and results.
   */
  static class RFProcess extends Thread {
    Process _process;
    BufferedReader _rd;
    String[] results = new String[RESULTS.length];
    String exception;
    PrintStream _stdout = System.out;

    /* Creates RFPRocess and spawns new process. */
    RFProcess(String cmd) throws Exception {
      System.out.println("'"+JAVA+" "+cmd+"'");
      List<String> c = new ArrayList<String>();
      c.add(JAVA);  for(String s : cmd.split(" "))  { s = s.trim(); if (s.length()>0) c.add(s); }
      ProcessBuilder bldr = new ProcessBuilder(c);
      bldr.redirectErrorStream(true);
      _process = bldr.start();
      _rd = new BufferedReader(new InputStreamReader(_process.getInputStream()));
    }

    /** Kill the spawned process. And kill the thread. */
    void cleanup() { _process.destroy(); try { _process.waitFor(); } catch( InterruptedException e ) { } }

    /** Read stdout and stderr of the spawned process. Read by lines and print
     * them to our stdout. Look for exceptions (fail) and result (error rate and
     * running time). */
    @Override public void run() {
      try {
        int state = 0, memCriticals = 0;
        String _line;
        while( (_line = _rd.readLine()) != null ) {
          _stdout.println(_line);
          Matcher m = RESULT[state].matcher(_line);
          if( m.find() ) {
            results[state] = m.group(1);
            if( ++state == RESULT.length ) {
              System.out.println("Error: " + results[ERROR_IDX] + "%");   break;
            }
          }
          m = EXCEPTION.matcher(_line);
          if( m.find() ) { // exception has been thrown -> fail!
            exception = "thread=" + m.group(1) + ", exception = " + m.group(2);   break;
          }
          m = ERROR.matcher(_line);
          if( m.find() ) { // exception has been thrown -> fail!
            exception = m.group(1);  break;
          }
          if( MEM_CRICITAL.matcher(_line).find() ) {
            if( ++memCriticals == 10 ) { // unlikely to recover, but can waste lot of time, kill it
              exception = "LOW MEMORY";  break;
            }
          } else  memCriticals = 0;
        }
      } catch( Exception e ) { throw new Error(e); }
    }
  }

  /** look for input files. If the path ends with '*' all files will be used. */
  static Collection<File> parseDatasetArg(String str) {
    ArrayList<File> files = new ArrayList<File>();
    StringTokenizer tk = new StringTokenizer(str, ",");
    while( tk.hasMoreTokens() ) {
      String path = tk.nextToken();
      if( path.endsWith("*") ) {
        path = path.substring(0, path.length() - 1);
        File f = KeyUtil.find_test_file(path);
        if( !f.isDirectory() ) throw new Error("invalid path '" + path + "*'");
        for( File x : f.listFiles() ) {
          if( x.isFile() && (x.getName().endsWith(".csv") || x.getName().endsWith(".data")) )
            files.add(x);
          else if( x.isDirectory() )
            files.addAll(parseDatasetArg(x.getAbsolutePath() + "*"));
        }
      } else files.add(KeyUtil.find_test_file(path));
    }
    return files;
  }

  static boolean runTest(String cmd, String resultDB, PrintStream stdout) throws Exception {
    RFProcess p = new RFProcess(cmd);
    p._stdout = stdout;
    p.start(); p.join(MAX_RUNNING_TIME); p.cleanup();
    p.join(); // in case we timed out...
    File resultFile = new File(resultDB);
    boolean writeColumnNames = !resultFile.exists();
    FileWriter fw = new FileWriter(resultFile, true);
    if( writeColumnNames ) {
      fw.write("Timestamp, Command");
      for( String s : RESULTS )
        fw.write(", " + s);
        fw.write("\n");
    }
    fw.write(new SimpleDateFormat("yyyy.MM.dd HH:mm").format(new Date(System.currentTimeMillis())) + ","+cmd+",");
        if( p.exception != null ) {
      System.err.println("Error: " + p.exception);  fw.write("," + p.exception);
    } else for( String s : p.results )  fw.write("," + s);
    fw.write("\n");
    fw.close();
    return true;
  }

  private static int seed() { return (int) (System.currentTimeMillis() & 0xFFFF); }
  private static int[] size(int[] multiples, int max){
    int off =0;
    while(multiples[off]<max) off++;
    return Arrays.copyOf(multiples, off+1);
  }

  static String[] makeFiles(String[] files, int[] multiples) {
    LinkedList<String> ls = new LinkedList();
    for(String f:files)
      try { ls.addAll( Arrays.asList(makeFile(f,multiples)) ); } catch (IOException e) { throw new Error(e); }
    String[] res = new String[ls.size()];
    ls.toArray(res);
    return res;
  }
  static String[] makeFile(String fn,int[] multiples) throws IOException {
    File f = new File(fn);
    String name = f.getName();
    StringBuilder bldr = new StringBuilder();
    Reader r = new FileReader(f);
    char[] buf = new char[1024 * 1024];
    int n = 0;
    String[] names = new String[multiples.length];
    while( (n = r.read(buf)) > 0 ) bldr.append(buf, 0, n);
    r.close();
    String content = bldr.toString();
    bldr = null;
    int off=0;
    for( int m : multiples ) {
      File f2 = new File(names[off++] = "/tmp/"+ name + "."+ m);
      FileWriter fw = new FileWriter(f2, true);
      for( int i = 0; i <= m; ++i )  fw.write(content);
      fw.close();
     // f2.deleteOnExit();
    }
    return names;
  }


  static void runTests(String javaCmd, PrintStream out, OptArgs args) throws Exception {
    stdout = out;
    int[] szMultiples = size(sizeMultiples, args.maxConcat);
    int[] szTrees = ntrees;
    String[] stats  = stat_types;
    boolean[] threading = new boolean[]{true,false};
    int[] seeds = new int[]{ 3, 42, 135};
    String[] files = makeFiles(args.files.split(","),szMultiples);

    int experiments = files.length * szTrees.length*stats.length*threading.length*seeds.length;
    String[] commands = new String[experiments];
    int i = 0;
    for(String f : files)
      for (boolean thread : threading)
        for (int sz :szTrees)
          for(String stat : stats)
            for(int seed : seeds) {
              RFArgs rfa = new RFArgs();
              rfa.seed = seed; rfa.statType = stat; rfa.file = f;
              rfa.ntrees = sz;  rfa.singlethreaded = thread;
              commands[i++] = javaCmd + " " + rfa;
            }

    for( String cmd : commands)
       runTest(cmd, args.resultDB, out);
  }


  public static void main(String[] args) throws Exception {
    final OptArgs ARGS        = new OptArgs();
    new Arguments(args).extract(ARGS);
    PrintStream out = new PrintStream(new File("/tmp/RFRunner.stdout.txt"));
    String javaCmd =   ARGS.jvmArgs + " " + JAR + " " + MAIN;
    try { runTests(javaCmd, out, ARGS); } finally { out.close(); }
  }
}


/* File flatfile = new File("flatfile" + Math.round(100000 * Math.random()));
flatfile.deleteOnExit();
FileWriter fw = new FileWriter(flatfile);
StringTokenizer tk = new StringTokenizer(ARGS.nodes, ",");
while( tk.hasMoreTokens() ) fw.write(tk.nextToken() + "\n");
fw.close();
if( !ARGS.h2oArgs.contains("-flatfile") ) ARGS.h2oArgs+= " -flatfile "+flatfile.getAbsolutePath();
*/