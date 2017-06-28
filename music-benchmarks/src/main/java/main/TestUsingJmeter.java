package main;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
 
public class TestUsingJmeter extends AbstractJavaSamplerClient implements Serializable {
    private static final long serialVersionUID = 1L;
    private final static String USER_AGENT = "Mozilla/5.0";
    public static String[] musicNodes;
 
    // set up default arguments for the JMeter GUI
    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        //defaultParameters.addArgument("URL", "http://www.google.com/");
        //defaultParameters.addArgument("SEARCHFOR", "newspaint");
        return defaultParameters;
    }
    
    
    private String getOperationType(){
    	String line=null;
		try {
			FileReader fileReader = new FileReader(new File("test.txt"));
			 BufferedReader br = new BufferedReader(fileReader);
			 line = br.readLine();
			 // if no more lines the readLine() returns null
			 br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    	 return line; 
    }
    public SampleResult runTest(JavaSamplerContext context) {
    	SampleResult result = new SampleResult();
    	result.sampleStart(); // start stopwatch
        try{
        	
        	System.out.println("running jmeter-a log");
        	System.out.println(getOperationType());
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
		MusicHandle tc = new MusicHandle(nodeIps);
		String bmKeyspace = "BenchmarksKeySpace";
		System.out.println("Keyspace "+bmKeyspace+" created...");
		tc.createKeyspaceEventual(bmKeyspace);
		
		
		//create table
		String bmTable = "BmEmployees";
		Map<String,String> fields = new HashMap<String,String>();
		fields.put("id", "uuid");
		fields.put("name", "text");
		fields.put("count", "varint");
		fields.put("address", "Map<text,text>");
		fields.put("PRIMARY KEY", "(name)");
		tc.createTableEventual(bmKeyspace, bmTable, fields);
		
		//fill a row in the table
		Map<String,Object> values = new HashMap<String,Object>();
	    values.put("id", UUID.randomUUID());
	    values.put("name", "bharath");
	    values.put("count", 4);
	    Map<String, String> address = new HashMap<String, String>();
	    address.put("number", "1");
	    address.put("street", "att way");
	    values.put("address", address);
	   	tc.insertIntoTableEventual(bmKeyspace, bmTable,values);

    }
       
    
}