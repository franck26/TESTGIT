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

import javax.swing.JFrame;
import javax.swing.JOptionPane;
/**
 *
 * @author user
 */
public class ConnectionMySql {

	public Connection conn = null;
	public Statement stmt;
	public String sharat = ""
			+ "";
	private String schema;

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

		String [] sharatim = {"154", "150", "151"};
		String host = "",user = "",password = "" ;
		this.schema = "franck";

		this.sharat = (String) JOptionPane.showInputDialog(new JFrame(), "שרת : ", "??? " + "איזה שרת אתה צריך", JOptionPane.QUESTION_MESSAGE, null, sharatim, sharatim[0]);

		switch (this.sharat) {
		case "151":
			host = "62.219.41.234:15106";//141.226.6.125:15106
			this.schema = "Franck_new";
			user = "franck";
			password = "electron26";
//			connectionToDB("141.226.6.125:15166","franck", "franck", "electron26");
			break;

		case "150":
			host = "192.168.24.150:3306";
			user = "root";
			password = "kd14NK97";
			
//			connectionToDB("192.168.24.150:3306","franck", "root", "kd14NK97");//141.226.6.125:15006
			break;

		case "154":
			host = "192.168.24.154:3306";//141.226.6.125:15466
			user = "root";
			password = "kq28BF99";
			
//			connectionToDB("141.226.6.125:15466","franck", "root", "kq28BF99");
			break;

			//		case 158:
			//			connectionToDB("141.226.6.125:15806","franck", "root", "netech2up");
			//			break;

		default:
			break;
		}

		connectionToDB(host,this.schema, user, password);
		
		System.out.println("שרת " + this.sharat);
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
			conn = DriverManager.getConnection("jdbc:mysql://" + mysqlIP + "/" + defaultDbName + "?"
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

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}
}