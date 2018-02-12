package otsma;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class RunOtsma {

	static Otsma o = Otsma.getInstance();

	public static void mainOtsma(String name_schema, String shem_hevra, String pathFile, int year1, int year2, int cid) throws SQLException{

		String name_table = shem_hevra + "_" + year1 + "_" + year2;
		String name_table_101 = name_table.toUpperCase() + "_101";
		
		String name_table_alphon = "alphon_" + shem_hevra;
		String name_table_alphon_101 = name_table_alphon + "_101";
		String charset = "  ";
				
//		o.create_table(name_schema, name_table, cid);
//				
//		for (int j = year1; j <= year2; j++) {
//			o.load_data_emon(name_schema, name_table, charset, "\\n", j, pathFile, cid);
//		}
		
//		o.Malam_replace_comma(name_schema, name_table);
////
////
//		o.create_101(name_schema, name_table_101, cid);
//		o.convert_to_101_9(name_schema, name_table_101,name_table);

//		o.update_total(name_schema, name_table_101);
//		o.update_symbol(name_schema, name_table_101);
//
//		o.modify_symbol_and_hagdaroth(name_schema, name_table_101);
		
		
		//alphon
		
		o.Create_Table_Alphone(name_schema, name_table_alphon);
//		
		for(int i = year1; i <= year2; i++){
			o.load_data_Alphone(name_schema, name_table_alphon, i, pathFile);
		}
//		
		o.Create_Table_Alphone_101(name_schema, name_table_alphon_101);
//		
		o.convert_Alphone_to_101_details(name_schema, name_table_alphon_101, name_table_alphon, cid);
		
//      TODO fonction avec le fichier des tat mifal

		System.out.println("finish ALIYAT OTSMA");

	}
}
