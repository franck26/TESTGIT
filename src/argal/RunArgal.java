package argal;

import java.sql.SQLException;

public class RunArgal {
	
	static Argal a = Argal.getInstance();
	
	public static void mainArgal(String name_schema, String name_table, String name_table_101, int cid) throws SQLException{
		 a.create_table(name_schema, name_table);
		 a.create_tabel_101(name_schema, name_table_101);
		 
		 for(int i = 2009; i <= 2016; i++){
			 a.load(name_schema, name_table, i);
		 }
		 
		 a.changeSymbolSymbolname(name_schema, name_table);
		 
		 a.insertTo101(name_schema, name_table_101, name_table, cid);
//		 
		 a.updateTotal(name_schema, name_table_101);
		 
		 System.out.println("finish for " + name_table_101);
	}

}
