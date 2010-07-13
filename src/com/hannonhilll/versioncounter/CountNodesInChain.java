/*
 * Created on Sep 7, 2007 by Bradley Wagner
 * 
 * Copyright(c) 2000-2007 Hannon Hill Corporation.  All rights reserved.
 */
package com.hannonhilll.versioncounter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/**
 * Counts the number of SD nodes in a chain starting with
 * one id. Includes any groups in the calculation.
 * 
 * @author  Bradley Wagner
 */
public class CountNodesInChain
{
    private MysqlDataSource mysqlSource;
    private Connection conn;

    public static void main(String[] args)
    {
        new CountNodesInChain().run();
    }

    public CountNodesInChain()
    {
        mysqlSource = new MysqlDataSource();
        mysqlSource.setServerName("localhost");
        mysqlSource.setDatabaseName("duketest");
        mysqlSource.setPortNumber(3306);
        mysqlSource.setUser("root");
        mysqlSource.setPassword("");
    }

    public void run()
    {
        try
        {
            String id = "46cef3b99803641d011486c529c18137";
            conn = mysqlSource.getConnection();
            int nodes = countInChain(id);
            System.out.println("Total of " + nodes + " in sd chain.");
            conn.close();
        }
        catch (Exception e)
        {
            System.out.println("Error close or getting connection: " + e.getMessage());
        }
    }

    private int countInChain(String id)
    {
        ResultSet rs = null;
        PreparedStatement stmt = null;

        int sdNodes = 0;

        String nextId = id;
        String groupId = null;
        try
        {
            while (nextId != null)
            {

                stmt = conn.prepareStatement("select structuredDataId, groupStructuredDataId from cxml_structureddata where id=?");
                stmt.setString(1, nextId);
                rs = stmt.executeQuery();

                String currentId = nextId;
                nextId = null;

                if (rs.next())
                {
                    nextId = rs.getString(1);
                    groupId = rs.getString(2);
                }

                int groupNodes = 0;
                if (groupId != null)
                {
                    groupNodes = countInChain(groupId);
                    System.out.println("Group had " + groupNodes + " nodes in chain starting with id: " + currentId);
                }

                sdNodes += groupNodes + 1;

                rs.close();
                stmt.close();
            }

            return sdNodes;
        }
        catch (Exception e)
        {
            System.out.println("Error occurred: " + e.getMessage());
            return 0;
        }
    }
}
