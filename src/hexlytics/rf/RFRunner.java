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
 * Class for testing RF. Launches RF (in a new vm) for files specified in args and records result (error rate and runtime). 
 * 
 *   
 * @author tomasnykodym
 *
 */
public class RFRunner {
	static final long MAX_RUNNING_TIME = 30*60000; // max runtime is 30 mins
	
	static final Pattern [] RES_MSG_PATTERN = new Pattern[] {
		 Pattern.compile("----- Random Forest finished -----"), 
		 Pattern.compile("Tree time to build:[ ]*(.*)"),
		 Pattern.compile("Tree depth:[ ]*(.*)"),
		 Pattern.compile("Tree leaves:[ ]*(.*)"),
		 Pattern.compile("Tree error rate:[ ]*(.*)"),
		 Pattern.compile("Data rows:[ ]*([^\n]*)(.*)"),
		 Pattern.compile("Overall error:[ ]*(.*)"),		 		 
	};
	
	static final Pattern EXCEPTION_MSG_PATTERN = Pattern
			.compile("Exception in thread \"(.*\") (.*)");

	static final String [] TREE_RESULTS = new String[] {"time2Build","depth","leaves","error rate", "data rows", "overallError"};
	
	/**
	 * Represents spawned process with H2O running RF.
	 * Hooks stdout and stderr and looks for exceptions (failed run) and results.
	 *  
	 * @author tomasnykodym
	 *
	 */
	static class RFProcess extends Thread {
		Process _process;
		BufferedReader _rd;
		volatile String _line;
		long _lastUpdate = 0;
		String [] _rfResults = new String[TREE_RESULTS.length];			
		boolean _exception;
		String _exceptionStr = null;
		volatile boolean _running = false;

		/*
		 * Creates RFPRocess and spawns new process with running RandomFores.main with given args.
		 */
		public RFProcess(String h2oJarPath, String vmArgs, String h2oArgs,
				String rfArgs) throws Exception {
			List<String> cmd = new ArrayList<String>();
			cmd.add("java");
			for (String s : vmArgs.split(" "))
				if (!s.isEmpty())
					cmd.add((s.startsWith("-")?s:"-" + s));
			if (!vmArgs.contains("jar"))
				cmd.add("-jar");
			else throw new Error("passed vm args should not contain -jar!");
			cmd.add(h2oJarPath);
			cmd.add("-mainClass");
			cmd.add("hexlytics.rf.RandomForest");			
			for (String s : rfArgs.split(" "))
				if (!s.isEmpty())
					cmd.add(s);
			if (h2oArgs != null && !h2oArgs.isEmpty()) {
				cmd.add("-h2oArgs");
				cmd.add(h2oArgs);
			}
			if (!rfArgs.isEmpty())
				cmd.add(rfArgs);
			StringBuilder b = new StringBuilder();
			for(String s:cmd)b.append(s + " ");
			ProcessBuilder bldr = new ProcessBuilder(cmd);
			bldr.redirectErrorStream(true);
			_process = bldr.start();
			_rd = new BufferedReader(new InputStreamReader(
					_process.getInputStream()));
			start();
			synchronized(this) { // wait for the thread to really start
				while(!_running)wait();
			}
		}

		/**
		 * Kill the spawned process.
		 * @throws InterruptedException
		 */
		void killProcess() throws InterruptedException {
			if (_running) {
				_process.destroy();
				_process.waitFor();
				_running = false;
				_process = null;
			}
		}

		static final int RESULTMSG_RUNTIME = 1;
		static final int RESULTMSG_ERROR = 2;

		/**
		 *  Read stdout and stderr of the spawned process. Read by lines and print them to our stdout.
		 *  Look for exceptions (fail) and result (error rate and running time).
		 */
		@Override
		public void run() {
			try {
				synchronized (this) {
					_running = true;
					notifyAll();
				}
				int state = 0;
				while ((_line = _rd.readLine()) != null) {
					_lastUpdate = System.currentTimeMillis();
					System.out.println(_line);
					Matcher m = RES_MSG_PATTERN[state].matcher(_line);
					if (m.find()) {
						if(state > 0)_rfResults[state-1] = m.group(1);
						if(++state == RES_MSG_PATTERN.length) break;							
					}					
					m = EXCEPTION_MSG_PATTERN.matcher(_line);
					if (m.find()) { // exception has been thrown -> fail!
						_exception = true;
						_exceptionStr = "thread=" + m.group(1)
								+ ", exception = " + m.group(2);
						break;
					}
					
				}				
			} catch (Exception e) {
				_exception = true;
				e.printStackTrace();
			} finally {
				if(_exception && _exceptionStr != null && !_exceptionStr.isEmpty()){
					System.err.println("Exception encoutered: " + _exceptionStr);
				}
				if(_process != null){
					_process.destroy();
					_process = null;
				}
				synchronized (this) {
					if(_running = true){
						_running = false;
						notifyAll();
					}
				}
			}
		}
	}

	static final InetAddress myAddr;

	static {
		try {
			myAddr = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			throw new Error(e);
		}
	}

	public static class OptArgs extends Arguments.Opt {
		public String h2ojar = "build/h2o.jar"; // path to the h2o.jar
		// dataset to process, if path ends with *, 
		// all files in the given subtree with .data or csv extension will be processed
		public String dasets = "smalldata/*"; 
		public String h2oArgs = ""; // args for the spawned h2o
		public String jvmArgs = ""; // args for the spawned jvm
		public String rfArgs = ""; // args for RF
		public String resultDB = "./results.csv"; // path to the file with the results
		public String nodes = myAddr.getHostAddress(); // list of nodes, currently ignored
	}

	/**
	 *  look up input file. If the path ends with * all files in the given subtree will be used. 
	 * @param str path 
	 * @return
	 */
	public static Collection<File> parseDatasetArg(String str) {
		ArrayList<File> files = new ArrayList<File>();
		StringTokenizer tk = new StringTokenizer(str, ",");
		while (tk.hasMoreTokens()) {
			String path = tk.nextToken();
			if (path.endsWith("*")) {
				path = path.substring(0, path.length() - 1);
				File f = TestUtil.find_test_file(path);
				if (!f.isDirectory())
					throw new Error("invalid path '" + path + "*'");
				for (File x : f.listFiles()) {
					if (x.isFile() && (x.getName().endsWith(".csv") || x.getName().endsWith(".data")))
						files.add(x);
					else if(x.isDirectory())files.addAll(parseDatasetArg(x.getAbsolutePath() + "*"));
				}
			} else
				files.add(TestUtil.find_test_file(path));
		}
		return files;
	}

	public static final OptArgs OPT_ARGS = new OptArgs();

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {		
		Arguments arguments = new Arguments(args);
		arguments.extract(OPT_ARGS);
		File flatfile = null;
		if (!OPT_ARGS.nodes.equals("all")) {
			flatfile = new File("flatfile"
					+ Math.round(100000 * Math.random()));
			flatfile.deleteOnExit();
			FileWriter fw = new FileWriter(flatfile);
			StringTokenizer tk = new StringTokenizer(OPT_ARGS.nodes, ",");
			while (tk.hasMoreTokens()) {
				fw.write(tk.nextToken() + "\n");
			}
			fw.close();
			if (!OPT_ARGS.h2oArgs.contains("-flatfile"))
				OPT_ARGS.h2oArgs += " -flatfile "
						+ flatfile.getAbsolutePath();
		}
		Collection<File> inputData = parseDatasetArg(OPT_ARGS.dasets);
		for (File f : inputData) {
			System.out.println("Procesing " + f.getPath());
			RFProcess p = new RFProcess(OPT_ARGS.h2ojar, OPT_ARGS.jvmArgs,
					OPT_ARGS.h2oArgs , OPT_ARGS.rfArgs);
			synchronized (p) {
				if (p._running) {
					p.wait(MAX_RUNNING_TIME);
				}
			}
			p.killProcess();
			if (!p._exception) { // write result to a file
				File resultFile = new File(OPT_ARGS.resultDB);
				boolean writeColumnNames = !resultFile.exists();
				FileWriter fw = new FileWriter(resultFile, true);
				if(writeColumnNames){
					fw.write("Timestamp, JVM args, H2O args, RF args, dataset");
					for(String s:TREE_RESULTS)fw.write(", " + s);
					fw.write("\n");
				}								
				fw.write(new SimpleDateFormat("yyyy.MM.dd HH:mm")
						.format(new Date(System.currentTimeMillis())) + ",");
				fw.write(OPT_ARGS.jvmArgs + "," + OPT_ARGS.h2oArgs + ","
						+ OPT_ARGS.rfArgs + "," + f.getPath());
				for(String s:p._rfResults) fw.write("," + s);
				fw.write("\n");
				fw.close();
			}
		}		
	}
}
