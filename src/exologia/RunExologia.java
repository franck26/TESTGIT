package exologia;

import java.sql.SQLException;

public class RunExologia {
	
	static Exologia e = new Exologia();
	
	public static void mainExologia(String name_schema, String name_table_temp, String name_table_exologia, String path) throws SQLException{
		
		e.createTableTempExologia(name_schema, name_table_temp);
		e.createExologia(name_schema, name_table_exologia);
		e.load(path, name_schema, name_table_temp);
		e.insertToExologia(name_schema, name_table_exologia, 11111, 2015, name_table_temp);
		
		System.out.println("finish !");
	}

}
