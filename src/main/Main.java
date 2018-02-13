/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

import chelan.RunChelan;
import enums.EnumMalam;
import enums.Tohnoth;
import exologia.RunExologia;

import java.sql.SQLException;
import argal.RunArgal;
import malam.RunMalam;
import micpal.RunMicpal;
import mySQL.Trysql;
import netoMicpal.RunNetoMicpal;
import oketz.RunOketz;
import otsma.RunOtsma;
import shiklolit.RunShiklolit;

/**
 *
 * @author user1
 */

public class Main {


	static String name_hevra = "discret" ;

	static Tohnoth tohnatSahar = Tohnoth.MICPAL
			
			;

	public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException{

		boolean insertIn101 = false;
		Trysql t = Trysql.getInstance();

		String name_schema = t.getConnectionMySql().getSchema();

		//      tbl_companies
		String name_hevra_companies = "מתן חן";

		int
		year1               =   2010,
		year2               =   2017,
		cid 				=   924151814;
//		hp					= 	515356962;

		String name_table = name_hevra.toLowerCase() + "_" + year1 + "_" + year2;

		String name_table_101 = name_table.toUpperCase() + "_101";



		String pathfile = "/home/user1/hevra/" + tohnatSahar.toString().toLowerCase() + "/" + name_hevra;
		System.out.println(pathfile);

		System.out.println(" you work with : " + name_hevra.toUpperCase() + ", tohnat sachar : " + tohnatSahar.toString() + " מ " + year1 + " עד " + year2 + " in schema \"" + name_schema + "\"");
		Thread.sleep(5*1000);

		System.out.println("ready ???");
		Thread.sleep(5*1000);

		switch(tohnatSahar){

		case SINAL:
			System.out.println("bonjour");
			break;

		case EXOLOGIA: 
			RunExologia.mainExologia(name_schema, "tbl_e_" + name_hevra, "tbl_exology_" + name_hevra.toUpperCase(), pathfile, cid, year1, year2);
			break;

		case ARGAL :
			
			String fields = "sikoum, "
//					+ "tat, "
										+ "num_worker, "
//					+ "id, "
					+ "last_name, first_name, "
					+ "id, "
					+ "type, symbol, symbolName, m12, m11, m10, m9, m8, m7, m6, m5, m4, m3, m2, m1";

			RunArgal.mainArgal(name_schema, name_hevra, name_table_101, cid, pathfile, year1, year2, fields);
			break;

		case OTSMA : 
			RunOtsma.mainOtsma(name_schema, name_hevra, pathfile, year1, year2, cid);
			break;

		case CHELAN: 
			RunChelan runChelan =   new RunChelan();


			//choix de chelan
			int perMonth = 1;
			
			switch(perMonth){
			case 0 : 
				break;

			case 1 :
				runChelan.mainChelanPerYear(name_schema, cid, pathfile, name_table, year1, year2);
				break;

			}
			break;  




		case MICPAL:
			RunMicpal runMicpal  =   new RunMicpal();

			int i = 1;
			switch(i){
			case 1 :
				runMicpal.mainMicpalOvdimPerMonths( pathfile,  name_schema, name_table_101, cid, year1, year2);
				break;

			case 0 : 
				runMicpal.mainBliKvatzimLekolHashanoth( pathfile,  name_schema, name_table_101, year1, year2,  cid);
				break;
			}

			RunNetoMicpal.mainNeto(name_schema, name_hevra, year1, year2, cid, pathfile, name_table_101, i);

			break;

		case SHIKLULIT:  
			RunShiklolit run = new RunShiklolit();

			int permonth = 1;

			switch (permonth) {
			case 0:
				break;
			case 1:
				run.mainTamhirPerMonth(name_schema, name_hevra, pathfile, name_table_101, year1, year2, cid);
				break;
			default:
				break;
			}

			break;

		case MALAM: 
			RunMalam runMalam = new RunMalam();

			//choix de malam
			EnumMalam malam = EnumMalam.DAN;

			switch(malam){
			case DAN :
				runMalam.mainDan("franck", name_hevra, name_table_101, year1, year2, cid);
				break;
			case FANTAZIA :
				runMalam.mainFuntazia();
				break;
			case AVIDAR :
				runMalam.emon();
				break;

			case FATAL : 
				runMalam.fatal("franck", "tbl_fatal_2016", "tbl_fatal_101_2016");
			}

			break;

		case OKETZ:  
			RunOketz runOketz = new RunOketz();
			runOketz.mainOketz(name_schema, name_hevra, name_table, name_table_101, cid, pathfile,  year1, year2);
			break; 

		default:
			System.out.println("faux choix !!!!!!!!!!!!");
			break;
		} 


		if(insertIn101)
			insertTo101AndSicumimAndCompanies(name_schema, name_table_101, cid, year1, year2, name_hevra_companies);


		System.out.println("\n\nfinish ! \n\n");


	}


	public static void insertTo101AndSicumimAndCompanies(String name_schema, String name_table_101, int cid, int year1, int year2, String name_hevra_companies) throws SQLException{

		System.out.println("insertTo101AndSicumimAndCompanies");

		String a = "";

		switch (tohnatSahar) {
		case EXOLOGIA:
			System.out.println("alya lamaareheth exology");
			a = "INSERT INTO `diff_taxes_reports`.`tbl_exology`\n" +
					"(\n" +
					"`cid`,\n" +
					"`dyear`,\n" +
					"`Symbol`,\n" +
					"`SymbolName`,\n" +
					"`IsMasMaasikim`,\n" +
					"`IsMasSachar`,\n" +
					"`IsBil`,\n" +
					"`IsMasHachnasa`,\n" +
					"`IsMasBruto`,\n" +
					"`Special`,\n" +
					"`type`,\n" +
					"`note`,\n" +
					"`m1`,\n" +
					"`m2`,\n" +
					"`m3`,\n" +
					"`m4`,\n" +
					"`m5`,\n" +
					"`m6`,\n" +
					"`m7`,\n" +
					"`m8`,\n" +
					"`m9`,\n" +
					"`m10`,\n" +
					"`m11`,\n" +
					"`m12`,\n" +
					"`total`,\n" +
					"`run_version`,\n" +
					"`SymbolName___arabic`,\n" +
					"`SymbolName___chinese`,\n" +
					"`SymbolName___english`,\n" +
					"`SymbolName___french`,\n" +
					"`SymbolName___hebrew`,\n" +
					"`SymbolName___portuguese`,\n" +
					"`SymbolName___russian`,\n" +
					"`SymbolName___spanish`,\n" +
					"`SymbolName___Italiano`,\n" +
					"`template`,\n" +
					"`comp_name`,\n" +
					"`to_edit`,\n" +
					"`version`,\n" +
					"`permission`)\n" +


					"select "
					+ ""
					+ "`cid`,\n" +
					"`dyear`,\n" +
					"`Symbol`,\n" +
					"`SymbolName`,\n" +
					"`IsMasMaasikim`,\n" +
					"`IsMasSachar`,\n" +
					"`IsBil`,\n" +
					"`IsMasHachnasa`,\n" +
					"`IsMasBruto`,\n" +
					"`Special`,\n" +
					"`type`,\n" +
					"`note`,\n" +
					"`m1`,\n" +
					"`m2`,\n" +
					"`m3`,\n" +
					"`m4`,\n" +
					"`m5`,\n" +
					"`m6`,\n" +
					"`m7`,\n" +
					"`m8`,\n" +
					"`m9`,\n" +
					"`m10`,\n" +
					"`m11`,\n" +
					"`m12`,\n" +
					"`total`,\n" +
					"`run_version`,\n" +
					"`SymbolName___arabic`,\n" +
					"`SymbolName___chinese`,\n" +
					"`SymbolName___english`,\n" +
					"`SymbolName___french`,\n" +
					"`SymbolName___hebrew`,\n" +
					"`SymbolName___portuguese`,\n" +
					"`SymbolName___russian`,\n" +
					"`SymbolName___spanish`,\n" +
					"`SymbolName___Italiano`,\n" +
					"`template`,\n" +
					"`comp_name`,\n" +
					"`to_edit`,\n" +
					"`version`,\n" +
					"`permission` from " + name_schema + ".tbl_exology_" + name_hevra.toUpperCase()+ ";";
			t.Insertintodb(a);
			break;

		default:

			a = "INSERT ignore INTO `diff_taxes_reports`.`tbl_101`\n" +
					"(\n" +
					"`cid`,\n" +
					"`dyear`,\n" +
					"`id`,\n" +
					"`original_id`,\n" +
					"`FullName`,\n" +
					"`Symbol`,\n" +
					"`SymbolName`,\n" +
					"`m1`,\n" +
					"`m2`,\n" +
					"`m3`,\n" +
					"`m4`,\n" +
					"`m5`,\n" +
					"`m6`,\n" +
					"`m7`,\n" +
					"`m8`,\n" +
					"`m9`,\n" +
					"`m10`,\n" +
					"`m11`,\n" +
					"`m12`,\n" +
					"`total`,\n" +
					"`division`,\n" +
					"`run_version`,\n" +
					"`date_value`,\n" +
					"`source`,\n" +
					"`type`,\n" +
					"`num_worker`,\n" +
					"`permission`,\n" +
					"`type_for_gemel`)\n" +
					"select " + cid + ", dyear, id, original_id, FullName, Symbol, SymbolName, "
					+ "m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, "
					+ "run_version, date_value, 'original_'" + tohnatSahar.toString().toLowerCase() +", type, num_worker, permission, type_for_gemel "
					+ "\n\n from " + name_schema + "." + name_table_101;
			System.out.println(a);
			t.Insertintodb(a);
			//		
			int isId = (tohnatSahar.toString().toLowerCase().equals("micpal") || tohnatSahar.toString().toLowerCase().equals("oketz")) ? 0 : 1;
			a = "INSERT INTO `system_management`.`tbl_companies`(`cid`,`comp_name`, user, h_p, dmei_nihul_shotefet_pre, dmei_nihul_zoveret_pre, is_id) "
					+ "VALUES (" + cid + ", \"" + name_hevra_companies + "\", 'franck', " + hp + ", 3, 0.3, " + isId + ");";
			t.Insertintodb(a);
			//		
			for(int i = year1; i <= year2; i++){
				a = "INSERT INTO `system_management`.`tbl_sicumin`(`cid`,`dyear`, `editors`) VALUES (" + cid + ", " + i + ", \";dorin.xetax;shoshana;tzippora;franck;ethel;irit;rivka;rivi;naomi2;tehila;yehudaa;yigal;yannick.atlan;yana;cto;avi_kaduri;avia;admin;gittler;tova;rut;root;\");" ; 
				t.Insertintodb(a);
			}

			break;
		}

	}

	public static String getName_hevra() {
		return name_hevra;
	}
}
