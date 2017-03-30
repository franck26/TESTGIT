package mySQL;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import shiklolit.Shiklolit;

/**
 *
 * @author user
 */
public class ConnectionMySql {

    public Connection conn = null;
    public Statement stmt;
    

	public ResultSet rs;
    public static ConnectionMySql instance = null;
    
    public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

	public Statement getStmt() {
		return stmt;
	}

	public void setStmt(Statement stmt) {
		this.stmt = stmt;
	}

	public ResultSet getRs() {
		return rs;
	}

	public void setRs(ResultSet rs) {
		this.rs = rs;
	}

	public static void setInstance(ConnectionMySql instance) {
		ConnectionMySql.instance = instance;
	}

    
    private ConnectionMySql(){
    	connectionToDB("192.115.152.50","franck", "franck", "electron26");
    }
    
    public static  ConnectionMySql getInstance(){			
		if (instance == null)
		{ 	
                    instance = new ConnectionMySql();	
		}
		return instance;
	}

    public void connectionToDB(String mysqlIP, String defaultDbName, String user, String pass)//doing the connection to mySql and creating statement.
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://" + mysqlIP + ":12006/" + defaultDbName + "?"
                    + "user=" + user + "&password=" + pass );
            
            
        } catch (Exception ex1) {
            conn = null;
        }

        try {
            stmt = conn.createStatement();
        } catch (SQLException ex) {
            Logger.getLogger(ConnectionMySql.class.getName()).log(Level.SEVERE, null, ex);
            stmt = null;
        }
        
    }

    public ResultSet doQuery(Statement stmt, String sql)//gets statement and string of query and executes it.
    {
        try {
            rs = stmt.executeQuery(sql);
        } catch (SQLException ex) {
        	System.out.println(ex.getMessage());
            rs = null;
            return rs;
            //  Logger.getLogger(ConnectionMySql.class.getName()).log(Level.SEVERE, null, ex);
        }
        return rs;
    }
}