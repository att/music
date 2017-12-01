package com.att.research.mdbc.tests;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Run all the tests against all the configurations specified in /tests.json.
 *
 * @author Robert Eby
 */
public class MAIN {
	public static final String CONFIG = "/tests.json";

	/**
	 * This class runs all the tests against all the configurations specified in /tests.json.
	 * It assumes that a copy of Cassandra is running locally on port 9042, that a copy of H2
	 * server is is running locally on port 8082, and that a copy of MySQL is running locally
	 * on port 3306.  These can be adjusted by editing the /tests.json file.
	 *
	 * @param args command line arguments
	 * @throws Exception if anything goes wrong
	 */
	public static void main(String[] args) throws Exception {
		new MAIN(args).run();
		System.exit(0);
	}

	private JSONArray configs;
	private List<Test> tests;
	private int total_success, total_failure;

	public MAIN(String[] args) throws Exception {
		configs = null;
		tests = new ArrayList<Test>();
		total_success = total_failure = 0;

		InputStream is = null;
		if (args.length == 0) {
			is = this.getClass().getResourceAsStream(CONFIG);
		} else {
			is = new FileInputStream(args[0]);
		}
		if (is != null) {
			JSONObject jo = new JSONObject(new JSONTokener(is));
			is.close();
			configs = jo.getJSONArray("configs");

			JSONArray ja = jo.getJSONArray("tests");
			for (int i = 0; i < ja.length(); i++) {
				Class<?> cl = Class.forName(ja.getString(i).trim());
				if (cl != null) {
					Constructor<?> con = cl.getConstructor();
					tests.add((Test) con.newInstance());
				}
			}
		} else {
			String conf = (args.length == 0) ? CONFIG : args[0];
			throw new Exception("Cannot find configuration resource: "+conf);
		}
	}
	public void run() {
		Logger logger = Logger.getLogger(this.getClass());
		for (int ix = 0; ix < configs.length(); ix++) {
			JSONObject config = configs.getJSONObject(ix);
			int succ = 0, fail = 0;
			logger.info("*** Testing with configuration: "+config.getString("description"));
			System.out.println("Testing with configuration: "+config.getString("description"));
			for (Test t : tests) {
				String nm = t.getName() + " ............................................................";
				System.out.print("  Test: "+nm.substring(0, 60));
				try {
					List<String> msgs = t.run(config);
					if (msgs == null || msgs.size() == 0) {
						succ++;
						System.out.println(" OK!");
					} else {
						fail++;
						System.out.println(" Fail!");
						System.out.flush();
						for (String m : msgs) {
							System.out.println("  "+m);
						}
						System.out.flush();
					}
				} catch (Exception x) {
					fail++;
					System.out.println(" Fail!");
				}
			}
			System.out.println();
			total_success += succ;
			total_failure += fail;
		}
		String m = "Testing completed: "+total_success+" successful tests, "+total_failure+": failures.";
		logger.info(m);
		System.out.println(m);
	}
}
