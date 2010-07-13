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
public class SDViz
{
    private MysqlDataSource mysqlSource;
    private Connection conn;

    public static void main(String[] args) throws Exception
    {
        new SDViz().run();
    }

    public SDViz()
    {
        mysqlSource = new MysqlDataSource();
        mysqlSource.setServerName("rage");
        mysqlSource.setDatabaseName("hh_production");
        mysqlSource.setPortNumber(3306);
        mysqlSource.setUser("root");
        mysqlSource.setPassword("bmw+cool");
    }

    public void run() throws Exception
    {
        try
        {
            String id = "ddbb4d090a00016b01533ff421c177e8";
            conn = mysqlSource.getConnection();
            printPage(id);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("Error close or getting connection: " + e.getMessage());
        }
        finally
        {
            conn.close();
        }
    }

    void printPage(String id) throws Exception
    {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try
        {
            ps = conn.prepareStatement("select pageStructuredDataId from cxml_foldercontent where id = ?");
            ps.setString(1, id);
            rs = ps.executeQuery();
            if (rs.next())
            {
                String sdId = rs.getString(1);
                System.out.println("Printing structured data for page with id: " + id);
                Node node = getChain(sdId);
                printNode(node, 0);
            }
        }
        finally
        {
            rs.close();
            ps.close();
        }
    }

    void printNode(Node node, int indent)
    {
        if (node == null)
        {
            return;
        }

        Node current = node;
        while (current != null)
        {
            System.out.println("* " + indent(indent) + current);
            printNode(current.group, indent + 5);

            current = current.next;
        }
    }

    Node getChain(String id) throws Exception
    {
        Node node = createNode(id);
        Node current = node;
        while (current != null)
        {
            current.next = createNode(current.nextId);
            current.group = getChain(current.groupId);

            current = current.next;
        }
        return node;
    }

    Node createNode(String id) throws Exception
    {
        if (id == null)
        {
            return null;
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        try
        {
            ps = conn.prepareStatement("select id, structuredDataId, groupStructuredDataId, name, textData from cxml_structureddata where id=?");
            ps.setString(1, id);
            rs = ps.executeQuery();
            if (rs.next())
            {
                Node node = new Node();
                node.id = rs.getString(1);
                node.nextId = rs.getString(2);
                node.groupId = rs.getString(3);
                node.name = rs.getString(4);
                node.text = rs.getString(5);
                return node;
            }
            else
            {
                throw new Exception("No node with id " + id + " found");
            }
        }
        finally
        {
            rs.close();
            ps.close();
        }
    }

    class Node
    {
        String id;
        String name;
        String nextId;
        String groupId;
        String text;
        Node next;
        Node group;

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString()
        {
            String text = this.text != null ? this.text : "";
            text = text.length() < 50 ? text : (text.substring(0, 50) + "...");
            return id + " : " + name + " : " + text;
        }
    }

    String indent(int level)
    {
        String result = "";
        for (int idx = 0; idx < level; idx++)
        {
            result += "  ";
        }
        return result;
    }

}
