package sinal;

import java.sql.SQLException;
import java.util.Iterator;

import mySQL.Trysql;

public class Sinal {

	private Trysql t;
	
	public Sinal(){
		this.t = Trysql.getInstance();
	}

	public void createTable(String nameSchema, String nameTable, String fields) throws SQLException{
		String s = "";
		String [] fieldss = fields.split(", ");
		
		for (String string : fieldss) {
			
		}
		
		t.Insertintodb(s);
	}
	
}
