/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package MAIN;

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

	static Trysql t = Trysql.getInstance();

	static String name_schema = "franck";
	static String name_hevra = "maadane_331";
	static String pathfile = "/home/user1/hevra/micpal/" + name_hevra;
	static String name_table_101 = name_hevra.toUpperCase() + "_101";
	
//      tbl_sicumin
        static String name_hevra_companies = "מעדני מניה - 331";


	static int
	year1               =   2010,
	year2               =   2013,
	cid 				=   905256715;

	static Tohnoth tohnatSahar1 = Tohnoth.MICPAL;

	public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException{
		
		System.out.println(" you work with : " + name_hevra.toUpperCase() + ", tohnat sachar : " + tohnatSahar1.toString() + " מ " + year1 + " עד " + year2 + " -> " + (year2 - year1 + 1) + " שנים");
		Thread.sleep(5*1000);

		System.out.println("ready ???");
		Thread.sleep(5*1000);

		switch(tohnatSahar1){

		case NETO_MICPAL :
			break;

		case SINAL:
			System.out.println("bonjour");
			break;

		case EXOLOGIA: 
			RunExologia.mainExologia(name_schema, "tbl_exology__temp", "tbl_exology_", pathfile);
			break;

		case ARGAL : 
			RunArgal.mainArgal(name_schema, "", name_table_101, cid);
			break;

		case OTSMA : 
			RunOtsma.mainOtsma(name_schema, "mikhlelet");
			break;

		case CHELAN: 
			RunChelan runChelan =   new RunChelan();


			//choix de chelan
			int perMonth = 1;

			switch(perMonth){
			case 0 : 
				break;

			case 1 :
				runChelan.mainChelanPerYear(name_schema, 952003531, "/home/user1/hevra/chelan/neguev", "tbl_neguev");
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

			RunNetoMicpal.mainNeto(name_schema, name_hevra, year1, year2, cid, pathfile, name_table_101);

			break;

		case SHIKLULIT:  
			RunShiklolit run = new RunShiklolit();

			int permonth = 1;

			switch (permonth) {
			case 0:
				break;
			case 1:
				run.mainTamhirPerMonth();
				break;
			default:
				break;
			}

			break;

		case MALAM: 
			RunMalam runMalam = new RunMalam();

			//choix de malam
			EnumMalam malam = EnumMalam.FATAL;

			switch(malam){
			case DAN :
				runMalam.mainDan("franck");
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
			runOketz.mainOketz();
			break; 

		default:
			System.out.println("faux choix !!!!!!!!!!!!");
			break;
		} 
		
//		insertTo101AndSicumimAndCompanies(name_schema, name_table_101, cid, year1, year2, name_hevra_companies);
		
		
	}

	public static void insertTo101AndSicumimAndCompanies(String name_schema, String name_table_101, int cid, int year1, int year2, String name_hevra) throws SQLException{

		String a = "INSERT INTO `diff_taxes_reports`.`tbl_101`\n" +
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
				"select " + cid + ", dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel "
				+ "from " + name_schema + "." + name_table_101;
		System.out.println(a);
		t.Insertintodb(a);
		
		a = "INSERT INTO `system_management`.`tbl_companies`(`cid`,`comp_name`, user) VALUES (" + cid + ", \"" + name_hevra + "\", 'franck');";
		t.Insertintodb(a);
//		
		for(int i = year1; i <= year2; i++){
			a = "INSERT INTO `system_management`.`tbl_sicumin`(`cid`,`dyear`, `editors`) VALUES (" + cid + ", " + i + ", \";dorin.xetax;shoshana;tzippora;franck;ethel;irit;rivka;rivi;naomi2;tehila;yehudaa;yigal;yannick.atlan;yana;cto;avi_kaduri;avia;admin;gittler;tova;rut;root;\");" ; 
			t.Insertintodb(a);
		}
			
		
		System.out.println("\n\nfinish ! \n\n");

	}

	public static String getName_hevra() {
		return name_hevra;
	}
}
