/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package micpal;

import java.io.File;
import java.io.FileWriter;
import java.sql.SQLException;

import MAIN.Main;
import semelim.MainSemelim;


public class RunMicpal {
	final Micpal m;

	String name_hevra = Main.getName_hevra();

	String
	tbl_tashlumim       =   "tbl_tashlumim_" + name_hevra, 
	tbl_guemel          =   "tbl_gemel_" + name_hevra, 
	tbl_hova            =   "tbl_hova_" + name_hevra,
	tbl_reshut          =   "tbl_reshut_" + name_hevra,
	tbl_ovdim           =   "tbl_ovdim_" + name_hevra,
	tbl_guemel_101	    =   "tbl_101_guemel_" + name_hevra,
	tbl_hova_101        =   "tbl_101_hova_" + name_hevra,
	tbl_reshut_101      =   "tbl_101_reshut_" + name_hevra,
	tbl_tashlumim_101   =   "tbl_101_tashlumim_" + name_hevra,
	tbl_ovdim_101       =   "tbl_101_ovdim_" + name_hevra;

	public RunMicpal() throws SQLException, ClassNotFoundException {
		this.m = Micpal.getInstance();
	}

	// אפשר לבנות את הטבלאה עם תוכנת שכר מיכפל
	public void mainMicpalOvdimPerMonths(String path_file, String name_schema, String tbl_101_sofi, int cid, int year1, int year2)
			throws SQLException, ClassNotFoundException, InterruptedException {



		create_tables(name_schema, tbl_101_sofi);

		for (int year = year1; year <= year2 ; year++) {
//			m.load_data_micpal_kupot_gemel(path_file,name_schema,tbl_guemel, year, cid);
//
			m.load_data_micpal_tashlumim(path_file,name_schema,tbl_tashlumim,year);
//			m.load_data_micpal_nikouy_hova(path_file, name_schema, tbl_hova, year);
//			m.load_data_micpal_nikuy_reshut(path_file,name_schema,tbl_reshut,year);
//			m.load_data_micpal_tamhir_ovdim(path_file,name_schema,tbl_ovdim,year);

		}

//		m.aliaAlphon(path_file, name_schema, tbl_guemel, "101_alfon_micpal_" + name_hevra);

		insertTo101(name_schema, tbl_101_sofi, year1, year2, cid);


		System.out.println("finish !!");
	}

	// מישתמשים את זה מתי שאין קבצים נפרד לכל השנות
	public void mainBliKvatzimLekolHashanoth(String path_file, String name_schema, String tbl_101_sofi, int year1, int year2, int cid)
			throws SQLException, ClassNotFoundException {

		create_tables(name_schema, tbl_101_sofi);

//				m.load_data_micpal_kupot_gemel(path_file, name_schema, tbl_guemel, cid);
//				m.load_data_micpal_nikouy_hova(path_file, name_schema, tbl_hova);
//				m.load_data_micpal_nikuy_reshut(path_file, name_schema, tbl_reshut);
//				m.load_data_micpal_tashlumim1(path_file, name_schema, tbl_tashlumim);
//		
//				for (int i = year1; i <= year2; i++) {
//		
//					m.load_data_micpal_tamhir_ovdim(path_file, name_schema, tbl_ovdim, i);
//				}

				
//				m.aliaAlphon(path_file, name_schema, tbl_guemel, "101_alfon_micpal_" + name_hevra);
		insertTo101(name_schema, tbl_101_sofi, year1, year2, cid);
	}

	private void create_tables(String name_schema, String tbl_101_sofi)
			throws ClassNotFoundException, SQLException {

//		m.create_tabel_micpal_koupot_gemel(name_schema,tbl_guemel);
//		m.create_tabel_micpal_tashlumim(name_schema,tbl_tashlumim);
////
//		m.create_tabel_micpal_nikuy_hova(name_schema, tbl_hova);
//		m.create_tabel_micpal_nikuy_reshut(name_schema,tbl_reshut);
//		m.create_tabel_micpal_tamhir_ovdim(name_schema,tbl_ovdim);

//		m.create_101_micpal_koupot_gemel(name_schema,tbl_guemel_101);
//		m.create_101_micpal_nikuy_hova(name_schema, tbl_hova_101);
//		m.create_101_micpal_nikuy_reshut(name_schema,tbl_reshut_101);
		m.create_101_micpal_tamhir_ovdim(name_schema,tbl_ovdim_101);
//		m.create_101_micpal_tashlumim(name_schema,tbl_tashlumim_101);
//
		m.create_101_micpal_sofi(name_schema,tbl_101_sofi);
	}

	private void insertTo101(String name_schema, String tbl_101_sofi, int year1, int year2, int cid)
			throws ClassNotFoundException, SQLException {


//		m.convert_to_101_kupot_gemel(name_schema, tbl_guemel_101, tbl_guemel, cid);
//		m.Micpal_to_101_tashlumim(name_schema,cid, tbl_tashlumim_101, tbl_tashlumim);
//		m.Micpal_to_101_nikuy_hova(name_schema, tbl_hova_101, tbl_hova, year1, year2, cid);
//		m.Micpal_to_101_tamchir_ovdim_per_month(name_schema, cid, tbl_ovdim_101, tbl_ovdim);
//		m.create_symbels_numbers_and_insert_into_main_table_pa(name_schema,
//				"tbl_symbels_ep", tbl_reshut);
//		m.Micpal_to_101_nicoy_reshut(name_schema, cid,tbl_reshut_101,
//				tbl_reshut);
		//				

		m.insert_total_101_micpal(name_schema,tbl_101_sofi,
				tbl_tashlumim_101, tbl_hova_101, tbl_reshut_101, tbl_ovdim_101,
				tbl_guemel_101);


		m.update_total(name_schema, tbl_101_sofi);


		m.modifyTypeToExology(name_schema, tbl_101_sofi);
	}
}
