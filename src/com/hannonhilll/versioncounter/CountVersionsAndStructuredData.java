/*
 * Created on Sep 7, 2007 by Bradley Wagner
 * 
 * Copyright(c) 2000-2007 Hannon Hill Corporation.  All rights reserved.
 */
package com.hannonhilll.versioncounter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import oracle.jdbc.driver.OracleDriver;
import oracle.jdbc.pool.OracleDataSource;

import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.jdbcx.JtdsDataSource;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import java.io.*;

/**
 * 
 * @author  Bradley Wagner
 */
public class CountVersionsAndStructuredData
{
    DataSource dataSource;
    String serverName;
    String databaseName;
    String user;
    String password;
    int port;
    boolean useCursors; //only used for SQL Server
    String driverType; //only used for Oracle
    String serviceName; //only used for Oracle
    String networkProtocol; //only used for Oracle
    
    
    public static void main(String[] args) throws SQLException
    {
    	InputStreamReader inputStreamReader = new InputStreamReader(System.in);
    	BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    	
    	System.out.println("(1) MySQL");
    	System.out.println("(2) SQL Server");
    	System.out.println("(3) Oracle");
    	System.out.println("Enter database type: ");
    	
    	try {
			String dbChoiceString = bufferedReader.readLine();
			int dbChoice = Integer.parseInt(dbChoiceString);
							
			switch(dbChoice){
			case 1: useMySQL();
					break;
			case 2: useSQLServer();
					break;
			case 3: useOracle();
					break;
			}
			
		} catch (IOException e) {
			System.out.println("Error reading database type.");
		}
    	
		try {
			inputStreamReader.close();
			bufferedReader.close();
		} catch (IOException e) {
			System.out.println("Error closing InputStreamReader.");
		}
    	
    	
        //new CountVersionsAndStructuredData().run();
    }
    
    public static CountVersionsAndStructuredData getConnectionParameters(String dbType){
    	InputStreamReader inputStreamReader = new InputStreamReader(System.in);
    	BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
	
    	CountVersionsAndStructuredData connection = new CountVersionsAndStructuredData();
    	
    	System.out.println("Server name: ");
    	try {
			String serverName = bufferedReader.readLine();
			connection.serverName = serverName;
		} catch (IOException e) {
			System.out.println("Error retrieving server name.");
		}
    	
    	System.out.println("Database name: ");
    	try {
			String databaseName = bufferedReader.readLine();
			connection.databaseName = databaseName;
		} catch (IOException e) {
			System.out.println("Error retrieving database name.");
		}
    	
    	System.out.println("User: ");
    	try {
			String user = bufferedReader.readLine();
			connection.user = user;
		} catch (IOException e) {
			System.out.println("Error retrieving user.");
		}
    	
    	System.out.println("Password: ");
    	try {
			String password = bufferedReader.readLine();
			connection.password = password;
		} catch (IOException e) {
			System.out.println("Error retrieving password");
		}
    	
    	System.out.println("Port: ");
    	try {
			String portString = bufferedReader.readLine();
			int port = Integer.parseInt(portString);
			connection.port = port;
		} catch (IOException e) {
			System.out.println("Error retrieving port.");
		}
		
		if(dbType == "oracle"){
			System.out.println("Service name: ");
			
			try {
				String serviceName = bufferedReader.readLine();
				connection.serviceName = serviceName;
			} catch (IOException e) {
				System.out.println("Error retrieving service name.");
			}
			
		}
		
    	return connection;
    }
    
    
    public static void useMySQL(){
    	
    	CountVersionsAndStructuredData mysqlConnection = getConnectionParameters("mysql");
    	
    	MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setServerName(mysqlConnection.serverName);
        mysqlDataSource.setDatabaseName(mysqlConnection.databaseName);
        mysqlDataSource.setPortNumber(mysqlConnection.port);
        mysqlDataSource.setUser(mysqlConnection.user);
        mysqlDataSource.setPassword(mysqlConnection.password);      
        mysqlConnection.dataSource = mysqlDataSource;
        mysqlConnection.run();
        
    }
    
    public static void useSQLServer(){
    	
    	CountVersionsAndStructuredData sqlServerConnection = getConnectionParameters("sqlserver");
        
    	JtdsDataSource sqlServerDataSource = new JtdsDataSource();
    	sqlServerDataSource.setServerType(Driver.SQLSERVER);
        sqlServerDataSource.setServerName(sqlServerConnection.serverName);
        sqlServerDataSource.setDatabaseName(sqlServerConnection.databaseName);
        sqlServerDataSource.setPortNumber(sqlServerConnection.port);
        sqlServerDataSource.setUser(sqlServerConnection.user);
        sqlServerDataSource.setPassword(sqlServerConnection.password);
        sqlServerDataSource.setUseCursors(sqlServerConnection.useCursors);
        sqlServerConnection.dataSource = sqlServerDataSource;
        sqlServerConnection.run();
        
    }
    
    public static void useOracle() throws SQLException{
    	
    	CountVersionsAndStructuredData oracleConnection = getConnectionParameters("oracle");
    	
    	OracleDataSource oracleDataSource = new OracleDataSource();
    	oracleDataSource.setServerName(oracleConnection.serverName);
    	oracleDataSource.setDatabaseName(oracleConnection.databaseName);
    	oracleDataSource.setUser(oracleConnection.user);
    	oracleDataSource.setPassword(oracleConnection.password);
    	oracleDataSource.setPortNumber(oracleConnection.port);
    	oracleDataSource.setServiceName(oracleConnection.serviceName);
    	oracleDataSource.setNetworkProtocol(oracleConnection.networkProtocol);
    	oracleDataSource.setDriverType(oracleConnection.driverType);
    	oracleConnection.dataSource = oracleDataSource;
    	oracleConnection.run();
    	
    }

    public CountVersionsAndStructuredData() {
    	
		serverName = null;
		databaseName = null;
		user = null;
		password = null;
		port = 0;
		serviceName = null;
		useCursors = true;
		driverType = "thin";
		networkProtocol = "tcp";
	
        }

    

	public void run()
    {
        String id = "0050121d814f4e1c003badef39e61c92";

        ResultSet rs = null;
        ResultSet rs2 = null;
        PreparedStatement stmt = null;
        PreparedStatement stmt2 = null;
        Connection conn = null;
        int sdNodes = 0;
        int versions = 0;

        String nextId = id;

        try
        {
            while (nextId != null)
            {
                conn = dataSource.getConnection();

                stmt = conn.prepareStatement("select prevVersionId from cxml_foldercontent where id=?");
                stmt.setString(1, nextId);
                rs = stmt.executeQuery();
                stmt2 = conn.prepareStatement("select count(id) from cxml_structureddata where ownerEntityId = ?");
                stmt2.setString(1, nextId);
                rs2 = stmt2.executeQuery();

                nextId = null;

                if (rs.next())
                {
                    nextId = rs.getString(1);
                }

                int nodes = 0;
                if (rs2.next())
                {
                    nodes = rs2.getInt(1);
                    sdNodes += nodes;
                }

                versions++;

                System.out.println("Version " + versions + " has " + nodes + " SD nodes");

                rs.close();
                rs2.close();
                stmt.close();
                stmt2.close();
            }

            System.out.println("Asset had " + versions + " versions and " + sdNodes + " structured data nodes total");
        }
        catch (Exception e)
        {
            System.out.println("Error occurred: " + e.getMessage());
        }
    }
}

