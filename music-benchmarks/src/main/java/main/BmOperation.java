package main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BmOperation {
	private String operationType;
	private String parameter;
	private String[] ipList;
	private MusicHandle musicHandle;

	public BmOperation(String fileName){
		Properties prop = new Properties();
		try {
			InputStream fstream = new FileInputStream(fileName);
			prop.load(fstream);
			fstream.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		operationType = prop.getProperty("op.type");
		parameter = prop.getProperty("parameter");
		ipList = prop.getProperty("ip.list").split(":");

		musicHandle = new MusicHandle(ipList);		
}

public void initialize(int numEntries){
	musicHandle.initialize(numEntries);
}

public void execute(int threadNum){//thread num is used to exclusively identify the threads/requests
	musicHandle.rowId = "emp"+threadNum;
	if(operationType.equals("cassa_ev_put")){
		long start = System.currentTimeMillis();
		musicHandle.cassaEvPut();
    	System.out.println("BmOperation, execute:"+(System.currentTimeMillis() - start));
	}else
		if(operationType.equals("music_ev_put")){
			musicHandle.musicEvPut();
		}else
			if(operationType.equals("cassa_quorum_put")){
				musicHandle.cassaQuorumPut();
			}else
				if(operationType.equals("music_critical_put")){
					musicHandle.musicCriticalPut();
				}else
					if(operationType.equals("zk_critical_put")){
						musicHandle.zkCriticalPut();
					}else
						if(operationType.equals("music_mix_put")){	//parameter field relevant only for this option
							/*
							 * parameter in the BmOperation interpreted as % eventual puts
							 * and we use the threadNum to determine if the operation should be eventual or 
							 * critical based on simple modulo arithmetic. 
							 */
							int propEvPuts = 100/Integer.parseInt(parameter);
							if(threadNum % propEvPuts == 0)
								musicHandle.musicEvPut();
							else
								musicHandle.musicCriticalPut();
						}
	//this option is causing bugs...mainly in closing out the thread. Not really needed now. 
	/*	if(operationType.equals("cassa_direct_put")){
			cassaHandle.update();			
		}else */
	//		cassaHandle.close();
}

public static void main(String[] args){
	BmOperation opHandle = new BmOperation("/Users/bharathb/AttWork/Music/keys/apache-jmeter-3.0/bin/music_bm.properties");
	opHandle.initialize(10);
	opHandle.execute(5);
	opHandle.execute(4);
	opHandle.execute(3);
	opHandle.execute(2);

}

}
