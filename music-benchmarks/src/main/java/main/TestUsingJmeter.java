package main;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
 
public class TestUsingJmeter extends AbstractJavaSamplerClient implements Serializable {
    private static final long serialVersionUID = 1L;
    private final static String USER_AGENT = "Mozilla/5.0";
    public static String[] musicNodes;
 
    // set up default arguments for the JMeter GUI
    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        return defaultParameters;
    }
    
    public SampleResult runTest(JavaSamplerContext context) {
    	SampleResult result = new SampleResult();
        try{
        	BmOperation bmo = new BmOperation("music_bm.txt");
        	int threadNum = JMeterContextService.getContext().getThreadNum();
        	result.sampleStart(); // start stopwatch
        	bmo.execute(threadNum);
        	result.sampleEnd(); // stop stopwatch
            result.setSuccessful( true );
            result.setResponseMessage( "Successfully performed action" );
        }
        catch (Exception e) {
        result.sampleEnd(); // stop stopwatch
        result.setSuccessful( false );
        result.setResponseMessage( "Exception: " + e );

        // get stack trace as a String to return as document data
        java.io.StringWriter stringWriter = new java.io.StringWriter();
        e.printStackTrace( new java.io.PrintWriter( stringWriter ) );
        result.setDataType( org.apache.jmeter.samplers.SampleResult.TEXT );
        result.setResponseCode( "500" );
    }

    return result;
    }
    
    public static void main(String[] args){   	
		//read the file and populate parameters (third line is ipList)
		try {
			BufferedReader br = new BufferedReader(new FileReader(args[0]));

			br.readLine();

			br.readLine();

			String ipListLine = br.readLine();
			String[] ipList = ipListLine.split(" ");
			
	    	int numEntries = Integer.parseInt(args[1]);
			MusicHandle musHandle = new MusicHandle(ipList);
			musHandle.initialize(numEntries);
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}