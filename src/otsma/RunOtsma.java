package otsma;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class RunOtsma {

	static Otsma o = Otsma.getInstance();

	public static void mainOtsma(String name_schema, String shem_hevra) throws SQLException{

		String name_table = "tbl_" + shem_hevra;
		String name_table_101 = "tbl_"+ shem_hevra.toUpperCase() + "_101";
		//		//
//		o.create_table(name_schema, name_table);
//		////		
//		for (int j = 2009; j <= 2009; j++) {
//			o.load_data_emon(name_schema, name_table,"CHARSET hebrew ", "\r\n", j);
//		}
//		for (int j = 1; j <= 1; j++) {
//			o.load_data_emon(name_schema, name_table," ", "\n", j);
//		}
//		
//		for (int j = 2; j <= 2; j++) {
//			o.load_data_emon(name_schema, name_table,"CHARSET hebrew ", "\r\n", j);
//		}
//		//
//		o.Malam_replace_comma(name_schema, name_table);
//
//
//		o.create_101(name_schema, name_table_101);
//		o.convert_to_101_9(name_schema, name_table_101,name_table);
//
//		o.update_total(name_schema, name_table_101);
//		o.update_symbol(name_schema, name_table_101);
		////		//		
		List<String> a = new ArrayList<String>();
		Statement s = o.returnStatement();

		a.addAll(Otsma.create_symbol("franck", name_table_101));



		for(int i =0;i<a.size();i++)
		{
			System.out.println(a.get(i));
			System.out.println(s.executeUpdate(a.get(i)) + "\n\n");
		}

		//              TODO fonction avec le fichier des tat mifal

		System.out.println("finish");

	}
}
