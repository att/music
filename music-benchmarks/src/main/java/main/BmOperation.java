package main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


public class BmOperation {
	private String operationType;
	private String parameter;
	private String[] ipList;
	private MusicHandle musicHandle;

	public BmOperation(String fileName){
		//read the file and populate parameters
		try {
			BufferedReader br = new BufferedReader(new FileReader(fileName));

			this.operationType = br.readLine();

			this.parameter = br.readLine();

			String ipListLine = br.readLine();
			this.ipList = ipListLine.split(" ");

			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		musicHandle = new MusicHandle(ipList);		
	}

	public void execute(int threadNum){//thread num is used to exclusively identify the threads/requests
		musicHandle.rowId = "emp"+threadNum;
		if(operationType.equals("cassa_ev_put")){
			musicHandle.cassaEvPut();
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
		if(operationType.equals("music_mix_put")){	
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
		BmOperation opHandle = new BmOperation("/Users/bharathb/AttWork/Music/keys/apache-jmeter-3.0/bin/music_bm.txt");
		opHandle.execute(10);
	}

}
