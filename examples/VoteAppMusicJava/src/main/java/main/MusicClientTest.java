package main;

import com.att.research.music.client.MusicClient;


public class MusicClientTest {
	public static void main(String[] args){
		MusicClient mc = new MusicClient();
		try {
			mc.createKeyspace("aambi");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	

}
