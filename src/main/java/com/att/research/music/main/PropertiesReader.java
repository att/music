package com.att.research.music.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class PropertiesReader {
	Properties prop = new Properties();

	public PropertiesReader(){
		prop = new Properties();
		URL resource = getClass().getResource("/");
		String musicPropertiesFilePath = resource.getPath().replace("WEB-INF/classes/", "WEB-INF/music.properties");
		// Open the file
		try {
			InputStream fstream = new FileInputStream(musicPropertiesFilePath);
			prop.load(fstream);
	        fstream.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getMyId(){
		return prop.get("my.id")+"";	
	}
	
	public String getMyPublicIp(){
		return prop.get("my.public.ip")+"";	
	}
	
	public String[] getAllIds(){
		String colonSeparatedIds = prop.get("all.ids")+"";
		String [] allIds = colonSeparatedIds.split(":");
		return allIds;
	}

	public String[] getAllPublicIps(){
		String colonSeparatedPublicIps = prop.get("all.public.ips")+"";
		String [] allPublicIps = colonSeparatedPublicIps.split(":");
		return allPublicIps;
	}

}
