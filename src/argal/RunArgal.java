package argal;

import java.sql.SQLException;

public class RunArgal {

	static Argal a = Argal.getInstance();

	public static void mainArgal(String name_schema, String name_table, String name_table_101, int cid, String path, int year1, int year2, String fields) throws SQLException{
		
//		a.create_table(name_schema, name_table);
//		a.create_tabel_101(name_schema, name_table_101);
//
//		for(int i = year1; i <= year2; i++){
//			a.load(name_schema, name_table, i, path,fields);
//		}
//
//		a.changeSymbolSymbolname(name_schema, name_table);
//
//		a.insertTo101(name_schema, name_table_101, name_table, cid);
//
//		a.updateTotal(name_schema, name_table_101);

		//alphon

		a.create_table_alphon(name_schema, name_table + "_alphon");

		a.load_alphon(name_schema, name_table + "_alphon", path, cid);

		a.create_tabel_101_details(name_schema, name_table_101 + "_details");

		a.insertTo101Details(name_schema, name_table_101 + "_details", name_table + "_alphon", cid);


		System.out.println("finish for " + name_table_101);
	}

}
