package com.hwx.hive;



import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;

/*
*Dependencies:-
*commons-configuration-xx.jar (UGI set configurations and metrics)
*commons-logging-xx.jar (*Not mandatory)
*hadoop-auth-xx.jar
*hadoop-common-xx.jar (UGI stuff, hadoop configurations)
*hive-jdbc-1.2xx-standalone.jar
*log4j-xx-api-xx.jar (for log4j)
*log4j-api-xx.jar (for log4j)
*log4j-core.xx.jar (for log4j)
*xercesImpl-xx.jar 

* Read configurations from hiveJDBC.properties file
* log4j2.xml provides few log4j properties
*/

import org.apache.hadoop.security.UserGroupInformation;


public class HiveJDBCKnox {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	 protected static final Logger logger= Logger.getLogger(HiveJDBCKnox.class);
	/**
	 * @param args
	 * @throws SQLException
	 * @throws IOException 
	 * @throws CertificateException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyStoreException 
	 */
	public static void main(String[] args) throws SQLException, IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	     
		//For knox connection to work, need to import appropriate SSL cert to the java truststore
		//For windows:- 
		//C:\Program Files\Java\jdk1.8.0_111\jre\lib\security>keytool +
		//  -importcert -file C:\Users\tiduserknox\Documents\public_cert.cer -keystore cacerts  -alias "forknox"
		
/*		KeyStore keyStore = KeyStore.getInstance("JKS");
		String fileName = System.getProperty("java.home") + 
		   "/lib/security/cacerts";
		FileInputStream stream = new FileInputStream(new File(fileName));
		keyStore.load( stream, "changeit".toCharArray());
		Enumeration<String> enAliases= keyStore.aliases();
		while(enAliases.hasMoreElements())
		{
		System.out.println(enAliases.nextElement());		
		}
		System.out.println(System.getProperty("javax.net.ssl.trustStore"));
		System.setProperty("javax.net.ssl.trustStore",fileName);*/


		org.apache.hadoop.conf.Configuration hdpConfig = new org.apache.hadoop.conf.Configuration();
		hdpConfig.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(hdpConfig);

		// This is the knox connection string, needs ca cert to be imported into the java truststore being used
		Connection con = DriverManager.getConnection("jdbc:hive2://knox.prod.hdp.hortonworks.com/;ssl=true;transportMode=http;httpPath=gateway/default/hive","tiduserknox","tiduserpasswordknox");
		
		Statement stmt = con.createStatement();
		String sql = ("show databases");
		ResultSet res=null;

	    res = stmt.executeQuery(sql);
		if (res!=null)
		{
		ResultSetMetaData metadata = res.getMetaData();
	    int columnCount = metadata.getColumnCount(); 
	    System.out.println(columnCount);
	    for (int i = 1; i <= columnCount; i++) {
	    	String delim=",";
	    	if(i==columnCount)
	    	{
	    		delim="";
	    	}
	    	System.out.println("<<"+metadata.getColumnName(i)+">>" + delim);
	    }
        //ArrayList<String> rows = new ArrayList<String>();	    
	    while(res.next()){

	        String row="";
	    	for (int i = 1; i <= columnCount; i++) {
		    	String delim=",";
		    	if(i==columnCount)
		    	{
		    		delim="";
		    	} 
		    	row += res.getString(i) + delim;
		    	
	        }
	        System.out.println(row);
	    }

		}
	}
	
}
