package exologia;

import java.sql.SQLException;

public class RunExologia {

	static Exologia e = new Exologia();

	public static void mainExologia(String name_schema, String name_table_temp, String name_table_exologia, String path, int cid, int year1, int year2) throws SQLException{

		String fields = "symbol, "
				+ "tokef, gmar_tokef, ofi, "
				+ "symbolname_male, symbolname_katsar, taarif, "
				+ "ahpala, yeridat_mida, mas_hachnassa, "
				+ "soug_lemass, bl, ";

		for (int i = 1; i<= 29; i++){
			fields += "a" + i + ", ";
		}

		fields += "reshut, a, shovy, dyear";

		e.createTableTempExologia(name_schema, name_table_temp, fields);
		e.createExologia(name_schema, name_table_exologia);
		for(int i = year1; i <= year2; i++){
			e.load(path, name_schema, name_table_temp, fields, i);
		}


		e.insertToExologia(name_schema, name_table_exologia, cid, name_table_temp);
		
		System.out.println("finish !");
	}

}
