package hiveJDBC;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
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


public class HiveJDBCTestV1 {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	 protected static final Logger logger= Logger.getLogger(HiveJDBCTestV1.class);
	/**
	 * @param args
	 * @throws SQLException
	 * @throws IOException 
	 */
	public static void main(String[] args) throws SQLException, IOException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	    InputStream is = null;
	       

	        
	        String path = new File("src/resources/config/hiveJDBC.properties").getAbsolutePath();
            logger.info("PATH:"+path);
            
            is = new FileInputStream(path);
	        Properties p = new Properties();
	        p.load(is);
	        String var_env= p.getProperty("environment");
	        String var_useKeyTab=p.getProperty("useKeyTab");
	        String var_keytabLoc= "noEnv";
	        String var_outputFolder="noFolder";
	        String var_userPrincipal=p.getProperty("userPrincipal");
	        String var_useHS2ConnType=p.getProperty("useHS2ConnType");
	        String var_jdbcConnString="noProp";
	        
	        if (var_env.equals("Windows"))
	        {
	        	if(var_useKeyTab.equals("True"))
	        	{
	        		var_keytabLoc=p.getProperty("keytabLocationWindows");	
	        	}
	        	
	        	var_outputFolder=p.getProperty("outputFolderPathWindows");
	        }
	        else if (var_env.equals("Unix"))
	        {
	        	if(var_useKeyTab.equals("True"))
	        	{
		        	var_keytabLoc=p.getProperty("keytabLocationUnix");
	        	}

	        	var_outputFolder=p.getProperty("outputFolderPathUnix");
	        }
	        else 
	        {
	        	logger.warn("environment needs to be set either to Windows or Unix in config file");
	        }
	        
	        if(var_useHS2ConnType.equals("Direct"))
	        {
	        	var_jdbcConnString=p.getProperty("jdbcConnStringDirect");
	        }
	        else if(var_useHS2ConnType.equals("Knox"))
	        {
	        	var_jdbcConnString=p.getProperty("jdbcConnStringKnox");
	        }
		org.apache.hadoop.conf.Configuration hdpConfig = new org.apache.hadoop.conf.Configuration();
		hdpConfig.set("hadoop.security.authentication", "Kerberos");
		UserGroupInformation.setConfiguration(hdpConfig);
		UserGroupInformation.loginUserFromKeytab(var_userPrincipal, var_keytabLoc);
		Connection con = DriverManager.getConnection(var_jdbcConnString, "", "");

		// This is the knox connection string, needs ca cert to be imported into the java truststore being used
		//Connection con = DriverManager.getConnection("jdbc:hive2://knox.tech.hdp.newyorklife.com/;ssl=true;transportMode=http;httpPath=gateway/default/hive","T93KOAI","xxxx");

		String var_queryType=p.getProperty("hiveQueryType");
		String var_query=p.getProperty("hiveQuery");
		Statement stmt = con.createStatement();
		String sql = (var_query);
		ResultSet res=null;
		if(var_queryType.equals("DDL"))
		{
			stmt.execute(sql);
		}
		else if(var_queryType.equals("DML"))
		{
			 res = stmt.executeQuery(sql);
		}
		if (res!=null)
		{
		ResultSetMetaData metadata = res.getMetaData();
		 FileOutputStream out = new FileOutputStream("flatfile.txt");
		 BufferedInputStream buffer;
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
        ArrayList<String> rows = new ArrayList<String>();	    
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
	        //System.out.println(row);
	        rows.add(row);
	    }
	    String fileNamePostFix = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
        Path file = Paths.get(URI.create("file:///"+var_outputFolder+"hive_output"+fileNamePostFix+".out"));
        Files.write(file, rows, Charset.forName("UTF-8"));
	    out.close();
		}
	}
}
