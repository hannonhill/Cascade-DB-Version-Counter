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

/**
 * 
 * @author  Bradley Wagner
 */
public class CountVersionsAndStructuredData
{
    DataSource dataSource;

    public static void main(String[] args) throws SQLException
    {
        new CountVersionsAndStructuredData().run();
    }

    CountVersionsAndStructuredData() throws SQLException
    {
        MysqlDataSource mysqlDataSource = new MysqlDataSource();
        mysqlDataSource.setServerName("localhost");
        mysqlDataSource.setDatabaseName("eib");
        mysqlDataSource.setPortNumber(3306);
        mysqlDataSource.setUser("root");
        mysqlDataSource.setPassword("");

        JtdsDataSource jtdsSource = new JtdsDataSource();
        jtdsSource.setServerType(Driver.SQLSERVER);
        jtdsSource.setServerName("sqlserver");
        jtdsSource.setPortNumber(1450);
        jtdsSource.setDatabaseName("TCKT-1968_Hofstra");
        jtdsSource.setUseCursors(true);
        jtdsSource.setUser("sa");
        jtdsSource.setPassword("h&nn0n");
        
        OracleDataSource oracleDataSource = new OracleDataSource();
        oracleDataSource.setServerName("huxley");
        oracleDataSource.setPortNumber(1521);
        oracleDataSource.setUser("indiana");
        oracleDataSource.setPassword("indiana");
        oracleDataSource.setDatabaseName("indiana");
        oracleDataSource.setDriverType("thin");
        oracleDataSource.setNetworkProtocol("tcp");
        oracleDataSource.setServiceName("orcl");
        
        this.dataSource = oracleDataSource;
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
