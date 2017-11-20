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
		int repFactor = Integer.parseInt(prop.getProperty("rep.factor"));

		musicHandle = new MusicHandle(ipList,repFactor);		
	}

	public BmOperation(String operationType, String parameter, String[] ipList, int repFactor){
		this.operationType = operationType;
		this.parameter = parameter;
		this.ipList = ipList;
		musicHandle = new MusicHandle(ipList,repFactor);		
	}
	
	public void initialize(int numEntries){
		musicHandle.initialize(numEntries);
	}

	public void execute(int threadNum){//thread num is used to exclusively identify the threads/requests
		musicHandle.rowId = "emp"+threadNum;
		
        switch (operationType) {
        case "music_ev_put":
			musicHandle.musicEvPut();
			break;
        case "music_atomic_put":
			musicHandle.musicAtomicPut();
			break;
        case "music_ev_get":
			musicHandle.musicEvGet();
			break;
        case "music_atomic_get":
			musicHandle.musicAtomicGet();
			break;
        case "zk_normal_put":
        	musicHandle.zkNormalPut();
			break;
        case "zk_atomic_put":
        	musicHandle.zkAtomicPut();
			break;
        case "zk_atomic_get":
        	musicHandle.zkAtomicGet();
			break;
        case "music_mix_put":
			int propEvPuts = 100/Integer.parseInt(parameter);
			if(threadNum % propEvPuts == 0)
				musicHandle.musicEvPut();
			else
				musicHandle.musicAtomicPut();
			break;
        case "zk_mix_put":
			propEvPuts = 100/Integer.parseInt(parameter);
			if(threadNum % propEvPuts == 0)
				musicHandle.zkNormalPut();
			else
				musicHandle.zkAtomicPut();
			break;
        default:
        	System.out.println("No such operation exists");
        }
	}

	public static void main(String[] args){
		String[] ipList = new String[1];
		ipList[0] = "localhost";
		System.out.println(System.currentTimeMillis());
		System.out.println("31536000000");
//		BmOperation opHandle = new BmOperation("music_ev_put", "-1", ipList, 1);
//		BmOperation opHandle = new BmOperation("music_atomic_put", "-1", ipList, 1);
//		BmOperation opHandle = new BmOperation("zk_normal_put", "-1", ipList, 1);
//		BmOperation opHandle = new BmOperation("zk_atomic_put", "-1", ipList, 1);

		int threadNum =1;	
	//	opHandle.initialize(10);
		
//		opHandle.execute(threadNum);
	}

}
