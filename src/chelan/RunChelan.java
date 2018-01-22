/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chelan;

import java.sql.SQLException;

//import xlsxToCsv.XlsxToCsv;

/**
 *
 * @author user1
 */
public class RunChelan {
	Chelan c;

	public RunChelan(){
		this.c = Chelan.getInstance();
	}

	public void mainChelanPerYear(String name_schema, int cid, String pathFile, String name_hevra, int year1, int year2) throws SQLException, InterruptedException{

		//		XlsxToCsv.convertToCsv(pathFile);

		String      
		tbl_alphon			=	name_hevra + "_alphon",
		tbl_alphon_101		=	name_hevra.toUpperCase() + "_Alphon_101",
		tbl_temp			=	name_hevra + "",
		tbl_sofi            =   name_hevra.toUpperCase() + "_101",
		
		
		fields 				= 	"f_name, l_name, num_worker, id, s, symbol, symbolname, m1, m1111111, m2, m22, m3, m33, m4, m44, m5, m55, m6, m66, m7, m77, m8, m88, m9, m99, m10, m1010, m11, m1111, m12, m1212";
		
		
		
		String fieldsExo = "symbol, "
				+ "tokef, gmar_tokef, ofi, "
				+ "symbolname_male, symbolname_katsar, taarif, "
				+ "ahpala, yeridat_mida, mas_hachnassa, "
				+ "soug_lemass, bl, ";

		for (int i = 1; i<= 29; i++){
			fieldsExo += "a" + i + ", ";
		}

		fieldsExo += "reshut, a, shovy, dyear";
		

		
		String fieldsAlphon = "tat, num_worker, last_name, first_name, id, birthday, ";
//		for (int i = 9; i<= 38; i++){
//			fieldsAlphon += "a" + i + ", ";
//		}
		fieldsAlphon += "martital_status, is_male, cid";

		
		
		
		//alphon
//		c.createDetails(name_schema, tbl_alphon, fieldsAlphon);
//		c.loadAlphon(name_schema, tbl_alphon, cid, year1, year2, pathFile, fieldsAlphon);
////		c.modifAlphon(name_schema, tbl_alphon);
//		c.create101Details(name_schema, tbl_alphon_101);
//		c.insertInto101Details(name_schema, tbl_alphon, tbl_alphon_101);


		//sahar
 		c.create_table_chelan_per_year(name_schema, tbl_temp, fields);
		int numLine = 14;
		for (int year = year1; year <= year2; year++){
				
				if(year == 2017){
					numLine = 1;
//					fields.replaceAll("m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12", "m7, m6, m5, m4, m3, m2, m1");
				}
				c.load_data_chelan_per_year_2(name_schema, pathFile,tbl_temp,year, cid, fields, numLine);
		}
				
		c.Malam_replace_comma(name_schema, tbl_temp);
		c.create_tabel_101_chelan(name_schema, tbl_sofi);
		c.chelan_convert_to_101(name_schema, tbl_sofi, tbl_temp, cid);
		c.chelan_delete_extra_rows(name_schema, tbl_sofi);
		
		
		c.update_total(name_schema, tbl_sofi);

		
		//exologia
		c.createTableExo(name_schema, name_hevra, fieldsExo);
		c.loadExo(name_schema, name_hevra, pathFile, fieldsExo, year1, year2);
		
		c.updateHagdaroth101(name_schema, name_hevra, tbl_sofi);		
		
		System.out.println("convert 101  to "+ tbl_sofi + " perfect !");
	}
}
