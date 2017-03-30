/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package malam;

import java.sql.SQLException;

/**
 *
 * @author user1
 */
public class RunMalam {

	private Malam malam;

	public RunMalam() {
		malam = Malam.getInstance();
	}

	/*
	 * עםזה אפשר לעלות כל הקבצים שמשתמשים את התוכנת שכר במו החברה DAN
	 */
	public void mainDan(String name_schema) throws SQLException {

		malam.Malam_create_first_table(name_schema, "amnon");
		malam.Malam_create_table_get_the_right_coulms(name_schema, "amnon_temp");
		malam.create_101_avidar(name_schema, "Tbl_amnon_101");

		for (int year = 2016; year <= 2016; year++) {
			malam.Malam_load_data_1(name_schema, year, "amnon", "");
			malam.Malam_replace_comma(name_schema, "amnon");
		}

		malam.Malam_insert_into_temp(name_schema, "amnon_temp", "amnon");
		malam.set_symble(name_schema, "amnon_temp");
		// u.create_101_malam(name_schema, "Tbl_amnon_101");
		malam.Malam_insert_into_101_fatal(name_schema, "Tbl_amnon_101", "amnon_temp");
		malam.update_total(name_schema, "Tbl_amnon_101");

	}

	public void mainFuntazia() throws SQLException {
		String name_schema = "franck";
		String name_table = "3856";
		String name_table_101 = name_table + "_101";
		String name_table_sofi = name_table_101 + "_sofi";
		String pathFile = "/home/user1/Documents/hevra/" + name_table + "/";
		//
		//
		malam.create_table_fanitzia(name_schema, name_table);
		malam.truncate(name_schema, name_table);
		malam.Malam_create_table_get_the_right_coulms_isrotel(name_schema, name_table_101);
		malam.truncate(name_schema, name_table_101);
		malam.create_101_malam(name_schema, name_table_sofi);
		malam.truncate(name_schema, name_table_sofi);
		// malam.Malam_create_table_get_the_right_coulms_isrotel(name_schema,
		// name_table_sofi);

		// for (int year = 2009; year <= 2012; year++) {
		//
		// System.out.println("Start " + year);
		// create_101_temp(pathFile, name_schema, year, name_table,
		// name_table_101, 3793, 14);
		// System.out.println("Finish " + year);
		// }

		for (int year = 2009; year <= 2012; year++) {

			System.out.println("Start " + year);
			create_101_temp(pathFile, name_schema, year, name_table, name_table_101, 3856, 14);
			System.out.println("Finish " + year);
		}

		for (int year = 2013; year <= 2016; year++) {

			System.out.println("Start " + year);
			create_101_temp(pathFile, name_schema, year, name_table, name_table_101, 3856, 2);
			System.out.println("Finish " + year);
		}

		System.out.println("101 sofi cree");
		malam.deleteEmpty(name_schema, name_table_101);
		malam.insert_into_101_isrotel(name_schema, name_table_sofi, name_table_101);
		System.out.println("101 sofi fini");
		malam.update_total(name_schema, name_table_sofi);

	}

	public void create_101_temp(String pathFile, String name_schema, int year, String name_table, String name_table_101,
			int cid, int ignoreLine) throws SQLException {
		malam.Malam_load_data_fanitzia(pathFile, name_schema, year, name_table, cid, ignoreLine);
		malam.Malam_insert_into_temp_isrotel(name_schema, name_table_101, name_table, year);
		malam.Malam_replace_comma(name_schema, name_table_101);
		// malam.truncate(name_schema, name_table);
	}

	public void emon() throws SQLException {

		String name_schema = "franck";
		String name_table = "amal";
		String name_table_101 = name_table + "_101";

		malam.create_table_emon(name_schema, name_table);
		malam.load_data_emon(name_schema, name_table, 2016);
		malam.Malam_replace_comma(name_schema, name_table);
		// //u.create_symbels_numbers_and_insert_into_main_table_ikea(name_schema,
		// "tbl_symbol_avidar15", "tbl_emonnn");
		malam.create_101_avidar(name_schema, name_table_101);
		malam.convert_to_101_9(name_schema, name_table_101, name_table, 123456);
		////
		malam.create_symbels_numbers_and_insert_into_main_table_ikea(name_schema, "tbl_temp", name_table_101);
		malam.update_total(name_schema, name_table_101);
	}

	public void fatal(String name_schema, String name_table, String name_table_101) throws SQLException {

		String fields = "misrad, shem_misrad, shem_oved, mis_oved, t_z, m_n, darog, darga, taarih_avoda, "
				+ "vetek, agaf, yehida, soug, symbolname, m1, "
				+ "m1_1, m2, m2_2, m3, m3_3, m4, m4_4, m5, m5_5, m6, m6_6, m7, m7_7, m8, m8_8, m9, m9_9, m10, m10_10, m11, m11_11, m12, m12_12, "
				+ "sah_sakum, sah_kamut";
		
		String path = "/home/user1/hevra/malam/fatal/2016/";
		
		malam.createTable(name_schema, name_table, fields);
//		
		for (int j = 1; j <= 38; j++) {
//			
			malam.Malam_load_data_fatal(name_schema, "2016", name_table, path + j, "charset hebrew", fields, 6);
		}

		malam.create_101_malam(name_schema, name_table_101);
//		malam.Malam_insert_into_temp(name_schema, name_table_101, name_table);
		malam.Malam_replace_comma(name_schema, name_table);
		malam.Malam_insert_into_101_fatal(name_schema, name_table_101, name_table);
		malam.set_symble(name_schema, name_table_101);
		
		malam.update_total(name_schema, name_table_101);
		System.out.println("finish");
	}
	
}
