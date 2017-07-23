package main;


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
    
    public static void main(String[] nodeIps){
    	//create key space
		MusicHandle musHandle = new MusicHandle(nodeIps);
		String bmKeyspace = "BenchmarksKeySpace";
		System.out.println("Keyspace "+bmKeyspace+" created...");
		musHandle.createKeyspaceEventual(bmKeyspace);
		
		
		//create table
		String bmTable = "BmEmployees";
		Map<String,String> fields = new HashMap<String,String>();
		fields.put("id", "uuid");
		fields.put("name", "text");
		fields.put("count", "varint");
		fields.put("address", "Map<text,text>");
		fields.put("PRIMARY KEY", "(name)");
		musHandle.createTableEventual(bmKeyspace, bmTable, fields);
		
		//fill rows in the table
		for(int i = 0; i < 5; ++i){
			Map<String,Object> values = new HashMap<String,Object>();
		    values.put("id", UUID.randomUUID());
		    values.put("name", "emp"+i);
		    values.put("count", 4);
		    Map<String, String> address = new HashMap<String, String>();
		    address.put("number", "1");
		    address.put("street", "att way");
		    values.put("address", address);
	   	musHandle.insertIntoTableEventual(bmKeyspace, bmTable,values);
		}

    }
       
    
}