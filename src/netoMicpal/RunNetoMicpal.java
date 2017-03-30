package netoMicpal;

import java.sql.SQLException;

public class RunNetoMicpal {

	private static NetoMicpal n =  NetoMicpal.getInstance();

	public static void mainNeto(String name_schema, String shem_hevra, int year1, int year2, int cid, String pathfile, String tbl_101_sofy) throws SQLException{
		String name_table = "neto_" + shem_hevra;
		String name_table_101 = name_table.toUpperCase() + "_101";

		n.create_table_neto(name_schema, name_table);
		n.create_table_101(name_schema, name_table_101);
		
		for(int year = year1; year <= year2; year++)
				n.load_data_neto(name_schema, name_table, year, pathfile);
		
		n.convert_to_101_neto(name_schema, name_table_101, name_table, cid);
	
		n.update_total(name_schema, name_table_101, tbl_101_sofy);
		
		System.out.println("neto shel \"" + shem_hevra + "\" ok");
		
	}

}
