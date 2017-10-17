package com.att.research.music.main;

import java.io.File;
import java.util.Arrays;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Log4JInitServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	
	final static Logger logger = Logger.getLogger(RestMusic.class);


	public void init(ServletConfig config) throws ServletException {
		
		System.out.println("Log4JInitServlet is initializing log4j");
		String log4jLocation = config.getInitParameter("log4j-properties-location");

		ServletContext sc = config.getServletContext();

		if (log4jLocation == null) {
			System.err.println("*** No log4j-properties-location init param, so initializing log4j with BasicConfigurator");
			BasicConfigurator.configure();
		} else {
			String webAppPath = sc.getRealPath("/");
			String log4jProp = log4jLocation;
			File yoMamaYesThisSaysYoMama = new File(log4jProp);
			if (yoMamaYesThisSaysYoMama.exists()) {
				System.out.println("Initializing log4j with: " + log4jProp);
				PropertyConfigurator.configure(log4jProp);
			} else {
				System.out.println("*** " + log4jProp + " file not found, so initializing log4j with BasicConfigurator");
				BasicConfigurator.configure();
			}
		}
		
		PropertiesReader prop = new PropertiesReader();
		logger.info("Starting MUSIC "+ MusicUtil.version +" on node with id "+prop.getMyId()+" and public ip "+prop.getMyPublicIp()+"...");
		logger.info("List of all MUSIC ids:"+ Arrays.toString(prop.getAllIds()));
		logger.info("List of all MUSIC public ips:"+ Arrays.toString(prop.getAllPublicIps()));
/*		try {
			MusicCore.initializeNode();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		super.init(config);
	}
	
}