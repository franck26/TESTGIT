/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chelan;

import java.sql.SQLException;

/**
 *
 * @author user1
 */
public class RunChelan {
	Chelan c;

	public RunChelan(){
		this.c = Chelan.getInstance();
	}

	public void mainChelanPerYear(String name_schema, int cid, String pathFile, String name_hevra) throws SQLException{

		String      
		tbl_temp			=	name_hevra + "_temp",
		tbl_sofi            =   name_hevra.toUpperCase() + "_101",
		fields 				= 	"mis_oved, l_name, f_name, t_z, "
//				+ "tat, taor_tat, "
				+ "symbol, symbolname, m1"
				+ "m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12";
		
		
		c.create_table_chelan_per_year(name_schema, tbl_temp, fields);
//
//		
//		//14
////		for (int year = 2016; year <= 2016; year++){
////			c.load_data_chelan_per_year_1(name_schema, pathFile,tbl_temp,year, cid);
////			
////		}	
//
//		//2
		for (int year = 2009; year <= 2016; year++){
			c.load_data_chelan_per_year_2(name_schema, pathFile,tbl_temp,year, cid, fields);
			
		}
//		
		c.Malam_replace_comma(name_schema, tbl_temp);
		c.create_tabel_101_chelan(name_schema, tbl_sofi);
		c.chelan_convert_to_101(name_schema, tbl_sofi, tbl_temp, cid);
		c.chelan_delete_extra_rows(name_schema, tbl_sofi);
		c.update_total(name_schema, tbl_sofi);

		System.out.println("convert 101  to "+ tbl_sofi + " perfect !");
	}
}
