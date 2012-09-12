package hexlytics.rf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
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

import org.omg.CORBA._PolicyStub;

import test.TestUtil;
import water.Arguments;

/**
 * Testing RF. Launches RF (in a new vm) for files specified in args and records results.
 */
public class RFRunner {
	static final long MAX_RUNNING_TIME = 30*60000; // max runtime is 30 mins
	
//	   Number of trees: 10
//	   >>No of variables tried at each split: 3
//	   >>             Estimate of error rate: 29%  (0.29168053276973704)
//	   >>                   Confusion matrix:
//	   >>                   0         1         2         3         4         5         6         7         8         9 err/class
//	   >>         0    216262     40901       121        17         8        25         0         0         0         0      0.16
//	   >>         1     71025    142208      3188       563       296         4         7         1         0         0      0.35
//	   >>         2      1895     18908      3250       340        30         0        37         0         0         0      0.87
//	   >>         3       616      7786       504      1966        12         0        40         6         0         0      0.82
//	   >>         4       158      1574        60        15       171         0         0         0         0         0      0.91
//	   >>         5       852        83         1         0         0        74         0         0         0         0      0.93
//	   >>         6         1       368       177       127         0         0        33         3         0         0      0.95
//	   >>         7         0        35        17        65         1         0         4         1         0         0      0.99
//	   >>         8         1         2         0         0         2         1         0         0         0         0       1.0
//	   >>         9         0         1         0         0         0         0         0         0         0         0       1.0
//	   >>
//	   >>          Avg tree depth (min, max): 37.4 (35.0 ... 40.0)
//	   >>         Avg tree leaves (min, max): 209779.3 (203180.0 ... 220731.0)
//	   >>                Validated on (rows): 513843
//	
	static final Pattern [] RESULT = new Pattern[] {
		 Pattern.compile("Number of trees:[ ]*([0-9]+)"),
		 Pattern.compile("No of variables tried at each split:[ ]*([0-9]+)"),
		 Pattern.compile("Estimate of error rate:[ ]*([0-9]+)"),
		 Pattern.compile("Avg tree depth \\(min, max\\):[ ]*([0-9]+).*"),
		 Pattern.compile("Avg tree leaves \\(min, max\\):[ ]*([0-9]+).*"),
		 Pattern.compile("Validated on \\(rows\\):[ ]*([0-9]+).*"),		 		 
	};
	static final Pattern EXCEPTION = Pattern.compile("Exception in thread \"(.*\") (.*)");
	static final String [] TREE_RESULTS = 
	    new String[] {"ntrees","nvars","err","avg depth", "avg leaves", "N rows"};
	
	/**
	 * Represents spawned process with H2O running RF.
	 * Hooks stdout and stderr and looks for exceptions (failed run) and results.
	 */
	static class RFProcess extends Thread {
		Process _process;
		BufferedReader _rd;
		volatile String _line;
		String [] results = new String[TREE_RESULTS.length];			
		String exception;

		/* Creates RFPRocess and spawns new process. */
		public RFProcess(String h2oJarPath, String vmArgs, String h2oArgs, String rfArgs) throws Exception {
			List<String> cmd = new ArrayList<String>();
			cmd.add("java");
			for (String s : vmArgs.split(" "))
				if (!s.isEmpty()) cmd.add( s.startsWith("-")? s : "-"+s);
			if (vmArgs.contains("jar")) throw new Error("should not contain -jar!");
            cmd.add("-jar"); cmd.add(h2oJarPath);
			cmd.add("-mainClass");	cmd.add("hexlytics.rf.RandomForest");			
			for (String s : rfArgs.split(" "))
				if (!s.isEmpty()) cmd.add(s);
			if (h2oArgs != null && !h2oArgs.isEmpty()) {
				cmd.add("-h2oArgs"); cmd.add(h2oArgs);
			}
			if (!rfArgs.isEmpty())	cmd.add(rfArgs);
			
			ProcessBuilder bldr = new ProcessBuilder(cmd);
			bldr.redirectErrorStream(true);
			_process = bldr.start();
			_rd = new BufferedReader(new InputStreamReader(_process.getInputStream()));
		}

		/** Kill the spawned process. And kill the thread. */
		void cleanup() {  
		  _process.destroy();
		  try { _process.waitFor(); } catch( InterruptedException e ) {}      
		}

		/**
		 *  Read stdout and stderr of the spawned process. Read by lines and print them to our stdout.
		 *  Look for exceptions (fail) and result (error rate and running time).
		 */
		@Override public void run() {
		  try {
		    int state = 0;
		    while ((_line = _rd.readLine()) != null) {
		      System.out.println(">>"+ _line);
		      Matcher m = RESULT[state].matcher(_line);
		      if (m.find()) {
		        results[state] = m.group(1);
		        if(++state == RESULT.length) break;							
		      }					
		      m = EXCEPTION.matcher(_line);
		      if (m.find()) { // exception has been thrown -> fail!
		        exception = "thread=" + m.group(1)+", exception = " + m.group(2);
		        break;
		      }
		    }				
		  } catch (Exception e) { throw new Error(e); }			
		}
	}

	static final InetAddress myAddr;

	static {
	  try { myAddr = InetAddress.getLocalHost();} catch (UnknownHostException e){throw new Error(e);}
	}

	/**
	 *  look up input file. If the path ends with * all files in the given subtree will be used. 
	 */
	public static Collection<File> parseDatasetArg(String str) {
	  ArrayList<File> files = new ArrayList<File>();
	  StringTokenizer tk = new StringTokenizer(str, ",");
	  while (tk.hasMoreTokens()) {
	    String path = tk.nextToken();
	    if (path.endsWith("*")) {
	      path = path.substring(0, path.length() - 1);
	      File f = TestUtil.find_test_file(path);
	      if (!f.isDirectory()) throw new Error("invalid path '" + path + "*'");
	      for (File x : f.listFiles()) {
	        if (x.isFile() && (x.getName().endsWith(".csv") || x.getName().endsWith(".data")))
	          files.add(x);
	        else if(x.isDirectory()) files.addAll(parseDatasetArg(x.getAbsolutePath() + "*"));
	      }
	    } else	files.add(TestUtil.find_test_file(path));
	  }
	  return files;
	}

	public static class OptArgs extends Arguments.Opt {
	  public String h2ojar = "build/h2o.jar"; // path to the h2o.jar
	  public String dasets = "smalldata/*";    // dataset to process
	  public String h2oArgs = ""; // args for the spawned h2o
	  public String jvmArgs = ""; // args for the spawned jvm
	  public String rfArgs = ""; // args for RF
	  public String resultDB = "./results.csv"; // path to the file with the results
	  public String nodes = myAddr.getHostAddress(); // list of nodes, currently ignored
	}
	public static final OptArgs ARGS = new OptArgs();

	public static void main(String[] args) throws Exception {		
	  Arguments arguments = new Arguments(args);
	  arguments.extract(ARGS);
	  File flatfile = null;
	  if (!ARGS.nodes.equals("all")) {
	    flatfile = new File("flatfile" + Math.round(100000 * Math.random()));
	    flatfile.deleteOnExit();
	    FileWriter fw = new FileWriter(flatfile);
	    StringTokenizer tk = new StringTokenizer(ARGS.nodes, ",");
	    while (tk.hasMoreTokens()) fw.write(tk.nextToken() + "\n");
	    fw.close();
	    if (!ARGS.h2oArgs.contains("-flatfile")) ARGS.h2oArgs +=" -flatfile "+flatfile.getAbsolutePath();
	  }
	  Collection<File> inputData = parseDatasetArg(ARGS.dasets);
	  for (File f : inputData) {
	    System.out.println("==== File: " + f.getPath());
	    RFProcess p = new RFProcess(ARGS.h2ojar, ARGS.jvmArgs, ARGS.h2oArgs , ARGS.rfArgs);
	    p.start();
	    p.join(MAX_RUNNING_TIME);
	    p.cleanup();
	    p.join(); // in case we timed out...
	    if( p.exception != null)  System.err.println("Error: "+p.exception);
	    else {
	      File resultFile = new File(ARGS.resultDB);
	      boolean writeColumnNames = !resultFile.exists();
	      FileWriter fw = new FileWriter(resultFile, true);
	      if(writeColumnNames){
	        fw.write("Timestamp, JVM args, H2O args, RF args, dataset");
	        for(String s:TREE_RESULTS)fw.write(", " + s);
	        fw.write("\n");
	      }								
	      fw.write(new SimpleDateFormat("yyyy.MM.dd HH:mm").format(new Date(System.currentTimeMillis())) + ",");
	      fw.write(ARGS.jvmArgs + "," + ARGS.h2oArgs + ","+ ARGS.rfArgs + "," + f.getPath());
	      for(String s:p.results) fw.write("," + s);
	      fw.write("\n");
	      fw.close();
	    }
	  }		
	}
}
