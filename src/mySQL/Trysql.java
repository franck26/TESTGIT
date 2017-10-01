package mySQL;

import java.sql.*;


public class Trysql {
	ConnectionMySql connectionMySql;
	
	public ConnectionMySql getConnectionMySql() {
		return connectionMySql;
	}

	public void setConnectionMySql(ConnectionMySql connectionMySql) {
		this.connectionMySql = connectionMySql;
	}

	ResultSet rs=null;
	
	static Trysql t = null;

	private  Trysql(){

		connectionMySql=ConnectionMySql.getInstance();

	}
	
	public static Trysql getInstance(){
		if(t == null){
			t = new Trysql();
		}
		return t;
	}

	public Statement getStmt(){
		return getConnectionMySql().getStmt();
	}


	public boolean Insertintodb(String request) throws SQLException{
		if(connectionMySql.stmt==null) {
			try {
				connectionMySql.stmt = connectionMySql.conn.createStatement();
			} catch (SQLException ex) {
				connectionMySql.stmt=null;
				System.out.println(ex.getMessage());  
			}
		}
		try{
//			System.out.println(request);
			connectionMySql.stmt.executeUpdate(request);
		}catch(SQLException s){

			System.out.println("sql error : " + request + "\n" + s.getMessage());
			System.out.println("error") ;
			return false;
		}
		return true;



	}



	public Integer Readfromdb(String d) throws SQLException{

		Integer a=0;
		try{
			rs=connectionMySql.doQuery(connectionMySql.stmt, d);}
		catch(Exception e){
			return  -1;
		}
		if(rs==null)
			return -1;
		if(rs.next())
			a= rs.getInt(1);

		if (a!=null)
			return a;
		else 
			return -1;

	}
	public String ReadStringfromdb(String d) throws SQLException{

		String a=null;
		rs=connectionMySql.doQuery(connectionMySql.stmt, d);
		if(rs.next()){
			a= rs.getString(1);
			return a;}
		return null;
	}

	public ResultSet gettabledb(String d) throws SQLException
	{
		//    
		//    Integer a=0;
		rs=connectionMySql.doQuery(connectionMySql.stmt, d);
		//   if(rs.next())

		return  rs;
		//   else return null;
	}

	public void Insertintodb1(String request) throws SQLException{
		if(connectionMySql.stmt==null) {
			try {
				connectionMySql.stmt = connectionMySql.conn.createStatement();
			} catch (SQLException ex) {
				connectionMySql.stmt=null;
				System.out.println(ex + "\n\n\n");  
			}}
		try{
//			System.out.println(request);
			
			connectionMySql.stmt.executeUpdate(request);
		}catch(SQLException s){
			System.out.println(s.getSQLState());
			System.out.println("sql error : " + request + "\n" + s.toString());
			System.out.println("error \n\n\n") ;
		}

	}
}
