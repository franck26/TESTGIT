/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chelan;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import javax.swing.JOptionPane;

import mySQL.Trysql;

/**
 *
 * @author user1
 */
public class Chelan {
	Trysql tr;

	private static Chelan instance;

	private Chelan(){
		tr = Trysql.getInstance();
	}

	static Chelan getInstance(){
		if (instance == null){
			instance = new Chelan();
			return instance;
		} else {
			return instance;
		}
	}

	//בונה טבלה מתאים לחילן
	public void create_tabel_chelan_per_month(String name_schema,String name_table) throws SQLException {

		String create = "CREATE TABLE  if not exists  `"+name_schema+"`." + name_table + " ("
				+ "`in_id` INT NOT NULL AUTO_INCREMENT,"

                + "`id_Worker` VARCHAR(200) NULL,"
                + " `l_name` VARCHAR(200) NULL,"
                + " `f_name` VARCHAR(200) NULL,"
                + " `id` VARCHAR(200) NULL,"
                + " `symbol` VARCHAR(200) NULL,"
                + "`symbolName` VARCHAR(200) NULL,"
                + "`value` VARCHAR(200) NULL,"
                + "`m` VARCHAR(200) NULL,"
                + "`dyear` VARCHAR(200) NULL,"
                + "PRIMARY KEY (`in_id`))";

		tr.Insertintodb1(create);

	}

	//בונה טבלה 101
	public void create_tabel_101(String name_schema, String name) throws SQLException {

		String a
		= "CREATE TABLE  if not exists " + name_schema + "." + name + "(\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
				+ "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n"
				+ "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n"
				+ "  `FullName` varchar(51) NOT NULL,\n"
				+ "  `Symbol` int(11) NOT NULL,\n"
				+ "  `SymbolName` varchar(100) NOT NULL,\n"
				+ "  `m1` double NOT NULL DEFAULT '0',\n"
				+ "  `m2` double NOT NULL DEFAULT '0',\n"
				+ "  `m3` double NOT NULL DEFAULT '0',\n"
				+ "  `m4` double NOT NULL DEFAULT '0',\n"
				+ "  `m5` double NOT NULL DEFAULT '0',\n"
				+ "  `m6` double NOT NULL DEFAULT '0',\n"
				+ "  `m7` double NOT NULL DEFAULT '0',\n"
				+ "  `m8` double NOT NULL DEFAULT '0',\n"
				+ "  `m9` double NOT NULL DEFAULT '0',\n"
				+ "  `m10` double NOT NULL DEFAULT '0',\n"
				+ "  `m11` double NOT NULL DEFAULT '0',\n"
				+ "  `m12` double NOT NULL DEFAULT '0',\n"
				+ "  `total` double NOT NULL DEFAULT '0',\n"
				+ "  `division` bigint(20) unsigned NOT NULL DEFAULT '0',\n"
				+ "  `run_version` int(11) DEFAULT NULL,\n"
				+ "  `date_value` varchar(50) DEFAULT NULL,\n"
				+ "  `source` varchar(100) DEFAULT NULL,\n"
				+ "  `type` varchar(400) DEFAULT NULL,\n"
				+ "  `num_worker` int(11) unsigned DEFAULT '0',\n"
				+ "  `permission` int(11) unsigned DEFAULT NULL,\n"
				+ "  `type_for_gemel` varchar(400) DEFAULT NULL,\n"
				+ "  PRIMARY KEY (`in_id`),\n"
				+ "  UNIQUE KEY `ind_unique` (`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,`division`) USING BTREE,\n"
				+ "  KEY `indi3` (`cid`,`dyear`,`Symbol`,`SymbolName`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";

		tr.Insertintodb1(a);
	}

	//ממלה את הטבלה 101 עם הנתונים של הטבלה רישונה
	public void convert_to_101(String name_schema, String name,String name_table, int cid) throws SQLException {
		for (int i = 1; i < 13; i++) {
			String load = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT   concat(`f_name`,' ',`l_name`)," + cid + ",`dyear`,`id`, `symbol`,`symbolName`,`value`\n"
					+ "            FROM  "+name_schema+"."+name_table +"  where m=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load);
		}

	}

	//עושה את החשבון של כל החודשים
	public void update_total(String name_schema, String name_table) throws SQLException {

		System.out.println("update total");
		String a = "update " + name_schema + "." + name_table + " set total = m1+m2+m3+m4+m5+m6+m7+m8+m9+m10+m11+m12";
		tr.Insertintodb1(a);

	}


	//diff_taxes_reports.tbl_101שם את הטבלה 101 סופית ב
	void Final_Table(String name_schema, String name_table) throws SQLException {

		String a = "insert into diff_taxes_reports.tbl_101\n"
				+ "("
				+ "cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel"
				+ ")\n"
				+ "SELECT "
				+ "cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel"
				+ "\n"
				+ "FROM `" + name_schema + "`.`" + name_table + "`\n";
		tr.Insertintodb1(a);

	}





	//per year    


	public void create_table_chelan_per_year(String name_schema, String name_table, String fields) throws SQLException {

		String create = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "                `in_id`              INT NOT NULL AUTO_INCREMENT,\n";
		String [] fieldss = fields.split(", ");

		for (String string : fieldss) {
			create += "`" + string + "` VARCHAR(200) NULL,\n";
		}

		create += "               `dyear` VARCHAR(200) NULL,\n"
				+ "               PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(create);
		// 
		truncate(name_schema, name_table);

	}

	void load_data_chelan_per_year_1(String name_schema, String name_file, String name_tabel, int year, int cid) throws SQLException {

		System.out.println("load year : " + year);
		String a = "LOAD DATA  LOCAL INFILE"
				+ " '" + name_file + "/" + year + ".csv'"
				+ " INTO TABLE `" + name_schema + "`." + name_tabel
				//                + "  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 12 LINES "
				//                + "  (num_Worker, l_name, f_name, id_Worker, symbol,  symbolName, m12, m11, m10, m9, m8, m7, m6, m5, m4, m3, m2, m1)"
				+ "  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 14 LINES "
				+ "  (num_Worker, id_Worker, l_name, f_name, symbol,  symbolName, total, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12) "
				//                + "  (num_Worker, id_Worker, l_name, f_name, symbol,  symbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12) "
				+ "set dyear=" + year;
		//in_id, name_company, tax_year, id_Worker, full_name, m, symbol, symbolName, value, sicom_camot, dyear, name_machlaka
		tr.Insertintodb1(a);

		System.out.println("end load year : " + year + "\n\n");

	}

	void load_data_chelan_per_year_2(String name_schema, String name_file, String name_table, int year, int cid, String fields, int numLine) throws SQLException, InterruptedException {

		System.out.println("load year : " + year);
		String a = "LOAD DATA  LOCAL INFILE"
				+ " '" + name_file + "/sahar/" + year + ".csv'"
				+ " INTO TABLE `" + name_schema + "`." + name_table
				//                + "  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 12 LINES "
				//                + "  (num_Worker, l_name, f_name, id_Worker, symbol,  symbolName, m12, m11, m10, m9, m8, m7, m6, m5, m4, m3, m2, m1)"
				+ "  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE " + numLine + " LINES "
				+ "  (" + fields + ") "
				//                + "  (num_Worker, id_Worker, l_name, f_name, symbol,  symbolName, total, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11"
				//                + ", m12"
				//                + ") "
				+ "set dyear=" + year + " ;";
		//in_id, name_company, tax_year, id_Worker, full_name, m, symbol, symbolName, value, sicom_camot, dyear, name_machlaka
		tr.Insertintodb1(a);

		System.out.println("end load year : " + year + " \n\n");

		Thread.sleep(2*1000);

	}

	void load_data_chelan_with_months_101_without_total(String name_schema, String name_file, String name_tabel, int year, int cid) throws SQLException {


		System.out.println("load kovets");

		String a = "LOAD DATA  LOCAL INFILE"
				+ " '" + name_file + "/" + year + ".csv'"
				+ " INTO TABLE `" + name_schema + "`." + name_tabel + " CHARACTER SET hebrew "
				+ "  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
				+ "  ( id_Worker, num_Worker, "
				//                + "l_name, "
				+ "f_name,  "
				+ "symbol, symbolName, m1"
				//                + ", m111, m2, m22, m3, m33, m4, m44, m5, m55, m6, m66, m7, m77, m8, m88, m9, m99, m10, m1010, m11, m1111, m12, m1212"
				+ ") "
				+ "set dyear=" + year + ", name_company = " + cid + "";
		//in_id, name_company, tax_year, id_Worker, full_name, m, symbol, symbolName, value, sicom_camot, dyear, name_machlaka
		tr.Insertintodb1(a);
		System.out.println("load fini");
	}

	public void create_tabel_101_chelan(String name_schema, String name) throws SQLException {

		System.out.println("create table " + name_schema + "." + name);

		String a
		= "CREATE TABLE  if not exists " + name_schema + "." + name + "(\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
				+ "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n"
				+ "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n"
				+ "  `FullName` varchar(51) NOT NULL,\n"
				+ "  `Symbol` int(11) NOT NULL,\n"
				+ "  `SymbolName` varchar(100) NOT NULL,\n"
				+ "  `m1` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m2` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m3` varchar(200) NULL DEFAULT '0',\n"
				+ "  `m4` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m5` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m6` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m7` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m8` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m9` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m10` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m11` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m12` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `total` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `division` bigint(20) unsigned NOT NULL DEFAULT '0',\n"
				+ "  `run_version` int(11) DEFAULT NULL,\n"
				+ "  `date_value` varchar(50) DEFAULT NULL,\n"
				+ "  `source` varchar(100) DEFAULT NULL,\n"
				+ "  `type` varchar(400) DEFAULT NULL,\n"
				+ "  `num_worker` int(11) unsigned DEFAULT '0',\n"
				+ "  `permission` int(11) unsigned DEFAULT NULL,\n"
				+ "  `type_for_gemel` varchar(400) DEFAULT NULL,\n"
				+ "  PRIMARY KEY (`in_id`),\n"
				+ "  UNIQUE KEY `ind_unique` (`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,`division`) USING BTREE,\n"
				+ "  KEY `indi3` (`cid`,`dyear`,`Symbol`,`SymbolName`)\n, KEY `ind` (`Symbol`)"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";

		tr.Insertintodb1(a);

		truncate(name_schema, name);
	}

	public void chelan_convert_to_101(String name_schema, String name_table_101, String name_table_temp, int cid) throws SQLException {


		System.out.println("convert 101");

		String a = "insert ignore  into " + name_schema + "." + name_table_101 + "\n"
				+ "            (cid, dyear, id, FullName, Symbol, SymbolName, m1"
				+ ", m2, m3, m4, m5, m6"
				+ ", m7"
				+ ", m8, m9, m10, m11, m12"
				+ ", num_worker, source"
//				+ ", original_id"
				+ ") \n"
				+ "   SELECT " + cid + " as cid,`dyear`, id,   concat(`f_name`,' ',`l_name`) as FullName,\n"
				+ "    symbol as Symbol,symbolName as SymbolName , sum(ifnull(m1,'0')),"
				+ "sum(ifnull(m2,'0')), sum(ifnull(m3,'0')), sum(ifnull(m4,'0')), sum(ifnull(m5,'0')), "
				+ "sum(ifnull(m6,'0'))"
				+ ","
				+ " sum(ifnull(m7,'0')), "
				+ "sum(ifnull(m8,'0')), sum(ifnull(m9,'0')), sum(ifnull(m10,'0')), sum(ifnull(m11,'0')), sum(ifnull(m12,'0')), "
				+ "num_worker, 'original_chelan'"
//				+ ", tat"
				+ "  FROM " + name_schema + "." + name_table_temp + " group by symbol, symbolname, id, dyear ;";

		tr.Insertintodb1(a);



	}

	void chelan_delete_extra_rows(String name_schema, String name_table) throws SQLException {
		String a = "DELETE FROM `" + name_schema + "`.`" + name_table + "` WHERE `id`='0'; ";
		String b = "DELETE   FROM " + name_schema + "." + name_table + " where Symbol=0 and SymbolName=''; ";
		String c = "DELETE FROM " + name_schema + "." + name_table + " where Symbol=5015 and SymbolName like '%אחוז משרה%' ; ";
		tr.Insertintodb1(a);
		tr.Insertintodb1(b);
//		tr.Insertintodb1(c);
	}

	public void Malam_replace_comma(String name_schema, String name_table) throws SQLException {

		String a = "update " + name_schema + "." + name_table + "\n"
				+ "set m1=replace(m1,',',''),\n"
				+ "m2=replace(m2,',',''),\n"
				+ "m3=replace(m3,',',''),\n"
				+ "m3=replace(m3,',',''),\n"
				+ "m4=replace(m4,',',''),\n"
				+ "m5=replace(m5,',',''),\n"
				+ "m6=replace(m6,',',''),\n"
				+ "m7=replace(m7,',',''),\n"
				+ "m8=replace(m8,',',''),\n"
				+ "m9=replace(m9,',',''),\n"
				+ "m10=replace(m10,',',''),\n"
				+ "m11=replace(m11,',',''),\n"
				+ "m12 =replace(m12,',','')"
				+ ";"
				;
		tr.Insertintodb1(a);

	}

	void truncate(String name_scema, String name_table) throws SQLException {
		String a="TRUNCATE `"+name_scema+"`.`"+name_table+"`";

		System.out.println(a);
		tr.Insertintodb1(a);
	}

	void drop(String name_scema, String name_table) throws SQLException {
		String a=" DROP TABLE `"+name_scema+"`.`"+name_table+"`";
		tr.Insertintodb1(a);
	}


	//alphon

	public void createDetails (String schema, String nameTable, String fieldsAlphon) throws SQLException{

		create_table_chelan_per_year(schema, nameTable, fieldsAlphon);
	}

	public void loadAlphon(String schema, String nameTable, int cid, int year1, int year2, String path, String fieldsAlphon) throws SQLException{

		for(int i = year1; i <= year2; i++){
		String a = "LOAD DATA  LOCAL INFILE"
				+ " '" + path + "/alphon/" + i + ".csv'"
				+ " INTO TABLE `" + schema + "`." + nameTable
				+ "  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
				+ "  (" + fieldsAlphon + ")"
				+ "set cid = " + cid + " ;";

		tr.Insertintodb(a);}

	}

	public void modifAlphon(String schema, String nameTable) throws SQLException{
		String sheilta = "";


		sheilta = "update " + schema + "." + nameTable + " set birthday = concat('0', birthday) where substring_index(birthday,'/', 1) < 10;";
		tr.Insertintodb(sheilta);

		sheilta =  " update " + schema + "." + nameTable + " set birthday = concat(substring_index(birthday, '/', 1), '/0', substring_index(substring_index(birthday, '/', 2), '/', -1), '/', substring_index(birthday, '/', -1)) "
				+ "where substring_index(substring_index(birthday, '/', 2), '/', -1) < 10;\n";

		tr.Insertintodb(sheilta);


		//		System.out.println(sheilta);

		sheilta = "UPDATE " + schema + "." + nameTable + " SET is_male = replace(is_male, 'זכר', 1) , is_male = replace(is_male, 'נקבה', 0) ;";

		tr.Insertintodb(sheilta);

		//		System.out.println(sheilta);

	}

	public void create101Details (String schema, String nameTable101) throws SQLException{
		String sheilta = "CREATE TABLE if not exists " + schema + "." + nameTable101 + " (\n" +
				"  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `cid` int(10) NOT NULL,\n" +
				"  `dyear` int(6) NOT NULL,\n" +
				"  `id` int(11) NOT NULL,\n" +
				"  `num_worker` int(11) NOT NULL,\n" +
				"  `first_name` varchar(50) DEFAULT NULL,\n" +
				"  `last_name` varchar(50) DEFAULT NULL,\n" +
				"  `month` int(6) NOT NULL,\n" +
				"  `run_version` int(11) DEFAULT '0',\n" +
				"  `is_toshav` varchar(45) DEFAULT '1',\n" +
				"  `aliya_date` varchar(45) DEFAULT '-1',\n" +
				"  `marital_status` varchar(45) DEFAULT NULL,\n" +
				"  `birthday` varchar(45) DEFAULT '-1',\n" +
				"  `age_in_months` int(11) unsigned DEFAULT '0',\n" +
				"  `is_mekabel_kitzba` tinyint(1) DEFAULT '0',\n" +
				"  `is_baal_shlita` tinyint(1) NOT NULL DEFAULT '0',\n" +
				"  `is_oved_zar` tinyint(1) NOT NULL DEFAULT '0',\n" +
				"  `another_work` tinyint(1) DEFAULT '0',\n" +
				"  `main_kitzva_type` varchar(20) DEFAULT NULL,\n" +
				"  `intra_division` varchar(45) NOT NULL,\n" +
				"  `intra_division_exp` varchar(45) NOT NULL,\n" +
				"  `time_nechut` varchar(20) DEFAULT NULL,\n" +
				"  `get_nechut_date` varchar(45) NOT NULL DEFAULT '-1',\n" +
				"  `nechut_perecent` varchar(45) NOT NULL DEFAULT '-1',\n" +
				"  `nechut_start` varchar(45) NOT NULL DEFAULT '-1',\n" +
				"  `nechut_end` varchar(45) NOT NULL DEFAULT '-1',\n" +
				"  `start_service_date` varchar(45) DEFAULT '-1',\n" +
				"  `finished_service_date` varchar(45) DEFAULT '-1',\n" +
				"  `is_male` tinyint(1) NOT NULL DEFAULT '1',\n" +
				"  `city` varchar(100) DEFAULT NULL,\n" +
				"  `street` varchar(200) DEFAULT NULL,\n" +
				"  `street_num` varchar(45) DEFAULT NULL,\n" +
				"  `zip_code` varchar(45) DEFAULT NULL,\n" +
				"  `phone_munber` varchar(45) DEFAULT NULL,\n" +
				"  `cell_phone` varchar(20) DEFAULT NULL,\n" +
				"  `bank` varchar(45) DEFAULT NULL,\n" +
				"  `branch` varchar(45) DEFAULT NULL,\n" +
				"  `account_number` varchar(45) DEFAULT NULL,\n" +
				"  `numOfChildren_fullPoints` int(3) unsigned DEFAULT '0',\n" +
				"  `numOfChildren_halfPoints` int(3) unsigned DEFAULT '0',\n" +
				"  `zikuy_points` double DEFAULT '0',\n" +
				"  `numOfChildren` int(3) unsigned DEFAULT '0',\n" +
				"  `typeOfIncomes_thisEmployer` varchar(100) DEFAULT NULL,\n" +
				"  `typeOfIncomes_otherEmployer` varchar(100) DEFAULT '9',\n" +
				"  `spouse_id` varchar(45) DEFAULT NULL,\n" +
				"  `spouse_Lname` varchar(45) DEFAULT NULL,\n" +
				"  `spouse_Fname` varchar(45) DEFAULT NULL,\n" +
				"  `spouse_dob` varchar(45) DEFAULT NULL,\n" +
				"  `spouse_aliya_date` varchar(45) DEFAULT NULL,\n" +
				"  `spouse_income` varchar(100) DEFAULT NULL,\n" +
				"  `degree_date` varchar(45) DEFAULT '-1',\n" +
				"  `degree_kod` int(3) unsigned DEFAULT '0',\n" +
				"  `start_pay` date DEFAULT NULL,\n" +
				"  `finish_pay` date DEFAULT NULL,\n" +
				"  `bank_precent` double DEFAULT '100',\n" +
				"  `version` int(11) DEFAULT NULL,\n" +
				"  `city_for_mh` varchar(100) NOT NULL DEFAULT '0',\n" +
				"  `kvutzat_gil` varchar(100) DEFAULT 'מגיל 18 עד גיל פרישה',\n" +
				"  `mas_shuly_percent` double NOT NULL DEFAULT '0',\n" +
				"  `const_mas` double NOT NULL DEFAULT '0',\n" +
				"  `prisha_date` varchar(45) DEFAULT '-1',\n" +
				"  `notes` longtext,\n" +
				"  `MSV_branch_num` varchar(3) NOT NULL DEFAULT '000',\n" +
				"  `MSV_bank_num` varchar(45) NOT NULL DEFAULT '00',\n" +
				"  `MSV_account_num` int(45) unsigned NOT NULL DEFAULT '0',\n" +
				"  `more_extra_zikuy_points` double NOT NULL DEFAULT '0',\n" +
				"  `start_working_date` varchar(45) DEFAULT '-1',\n" +
				"  `is_101` tinyint(1) DEFAULT '0',\n" +
				"  `files` varchar(100) DEFAULT NULL,\n" +
				"  `job_precent` double DEFAULT '100',\n" +
				"  `months2calc` int(11) DEFAULT NULL,\n" +
				"  `is_year_mh_calc` tinyint(1) DEFAULT '1',\n" +
				"  `start_month_forMh` int(10) NOT NULL DEFAULT '1',\n" +
				"  `derog` int(10) unsigned DEFAULT '0',\n" +
				"  `dargat_sachar` int(10) unsigned DEFAULT '0',\n" +
				"  `vetek` double DEFAULT '0',\n" +
				"  `tafkid` int(10) DEFAULT NULL,\n" +
				"  `is_pensyoner` tinyint(4) DEFAULT '0',\n" +
				"  `source` varchar(100) DEFAULT NULL,\n" +
				"  `permission` int(11) unsigned DEFAULT NULL,\n" +
				"  PRIMARY KEY (`in_id`),\n" +
				"  UNIQUE KEY `unique_1` (`cid`,`dyear`,`month`,`id`,`run_version`) USING BTREE,\n" +
				"  KEY `Index_2` (`cid`,`dyear`,`id`,`month`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='InnoDB free: 1508352 kB; InnoDB free: 2484224 kB; InnoDB fre';";

		tr.Insertintodb(sheilta);

		truncate(schema, nameTable101);
	}

	public void insertInto101Details(String schema, String nameTable, String nameTable101) throws SQLException{


		String sheilta = "INSERT INTO " + schema + "." + nameTable101 + "\n" +
				"(\n" +
				"`cid`,\n" +
				"`id`,\n" +
				"`num_worker`,\n" +
				"`first_name`,\n" +
				"`last_name`,\n" +
				"`birthday`,\n" +
				"`is_male`, month, intra_division, intra_division_exp, dyear)\n" +
				"SELECT"
				+ " `cid`,\n" +
				"`id`,\n" +
				"`num_worker`,\n" +
				"`first_name`,\n" +
				"`last_name`,\n" +
				"`birthday`,\n" +
				"`is_male`, '0', '0', '0', 0 \n" +
				"from " + schema + "." + nameTable + " group by id"; 

		System.out.println(sheilta);

		tr.Insertintodb(sheilta);
	}

	public void createTableExo(String name_schema, String name_hevra, String fieldsExo) throws SQLException {
		System.out.println("create tbl temp");



		String s = "CREATE TABLE if not exists " + name_schema + "." + name_hevra +"_exo (\n" +
"  `symbol` varchar(200) DEFAULT NULL,\n" +
"  `tokef` varchar(200) DEFAULT NULL,\n" +
"  `gmar_tokef` varchar(200) DEFAULT NULL,\n" +
"  `ofi` varchar(200) DEFAULT NULL,\n" +
"  `symbolname_male` varchar(200) DEFAULT NULL,\n" +
"  `symbolname_katsar` varchar(200) DEFAULT NULL,\n" +
"  `taarif` varchar(200) DEFAULT NULL,\n" +
"  `ahpala` varchar(200) DEFAULT NULL,\n" +
"  `yeridat_mida` varchar(200) DEFAULT NULL,\n" +
"  `mas_hachnassa` varchar(200) DEFAULT NULL,\n" +
"  `soug_lemass` varchar(200) DEFAULT NULL,\n" +
"  `bl` varchar(200) DEFAULT NULL,\n" +
"  `a1` varchar(200) DEFAULT NULL,\n" +
"  `a2` varchar(200) DEFAULT NULL,\n" +
"  `a3` varchar(200) DEFAULT NULL,\n" +
"  `a4` varchar(200) DEFAULT NULL,\n" +
"  `a5` varchar(200) DEFAULT NULL,\n" +
"  `a6` varchar(200) DEFAULT NULL,\n" +
"  `a7` varchar(200) DEFAULT NULL,\n" +
"  `a8` varchar(200) DEFAULT NULL,\n" +
"  `a9` varchar(200) DEFAULT NULL,\n" +
"  `a10` varchar(200) DEFAULT NULL,\n" +
"  `a11` varchar(200) DEFAULT NULL,\n" +
"  `a12` varchar(200) DEFAULT NULL,\n" +
"  `a13` varchar(200) DEFAULT NULL,\n" +
"  `a14` varchar(200) DEFAULT NULL,\n" +
"  `a15` varchar(200) DEFAULT NULL,\n" +
"  `a16` varchar(200) DEFAULT NULL,\n" +
"  `a17` varchar(200) DEFAULT NULL,\n" +
"  `a18` varchar(200) DEFAULT NULL,\n" +
"  `a19` varchar(200) DEFAULT NULL,\n" +
"  `a20` varchar(200) DEFAULT NULL,\n" +
"  `a21` varchar(200) DEFAULT NULL,\n" +
"  `a22` varchar(200) DEFAULT NULL,\n" +
"  `a23` varchar(200) DEFAULT NULL,\n" +
"  `a24` varchar(200) DEFAULT NULL,\n" +
"  `a25` varchar(200) DEFAULT NULL,\n" +
"  `a26` varchar(200) DEFAULT NULL,\n" +
"  `a27` varchar(200) DEFAULT NULL,\n" +
"  `a28` varchar(200) DEFAULT NULL,\n" +
"  `a29` varchar(200) DEFAULT NULL,\n" +
"  `reshut` varchar(200) DEFAULT NULL,\n" +
"  `a` varchar(200) DEFAULT NULL,\n" +
"  `shovy` varchar(200) DEFAULT NULL,\n" +
"  `dyear` varchar(200) DEFAULT NULL,\n" +
"  KEY `ind` (`symbol`)\n" +
") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

		//		System.out.println(s);

		tr.Insertintodb(s);

				truncate(name_schema, name_hevra + "_exo");

	}

	public void loadExo(String name_schema, String name_hevra, String pathFile, String fieldsExo, int year1, int year2) throws SQLException {
		System.out.println("load Exologia ");

		for(int i = year1; i <= year2; i++){
			String s = "LOAD DATA  LOCAL INFILE '" + pathFile + "/exo/" + i + ".csv' "
					+ " INTO TABLE  " + name_schema + "." + name_hevra + "_exo    " 
					+ " FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 2 LINES   \n"
					+ "("+ fieldsExo + ") set dyear = " + i + ";";

			tr.Insertintodb(s);
		}

	}

	public void updateHagdaroth101(String name_schema, String name_hevra, String tbl_sofi) throws SQLException {

		Vector<String> sheiltoth = new Vector<>();

		//tashlumim

		String sheilta = "update " + name_schema + "." + tbl_sofi + " set type = ';b_tlush;' where Symbol = '5001'";

		sheiltoth.add(sheilta);


		sheilta = "Update " + name_schema + "." + tbl_sofi + " as t1 "
				+ "inner join " + name_schema + "." + name_hevra + "_exo as t2 "
				+ "on t1.symbol = t2.symbol and t1.dyear = t2.dyear "
				+ "set t1.type = ';tlush_tashlum;' "
				+ "where t2.ofi in (1, 2, 12, 5) ;";

		sheiltoth.add(sheilta);



		sheilta = "Update " + name_schema + "." + tbl_sofi + " as t1 "
				+ "inner join " + name_schema + "." + name_hevra + "_exo as t2 "
				+ "on t1.symbol = t2.symbol  and t1.dyear = t2.dyear "
				+ "set t1.type = concat(type, ';tlush_shovy;')    "
				+ "where t2.ofi in (5) ;";

		sheiltoth.add(sheilta);

		sheilta = "Update " + name_schema + "." + tbl_sofi + " as t1 "
				+ "inner join " + name_schema + "." + name_hevra + "_exo as t2 "
				+ "on t1.symbol = t2.symbol  and t1.dyear = t2.dyear "
				+ "set t1.type = ';tlush_tashlum;freeBil;b_tlush;'    "
				+ "where t2.ofi in (1, 2, 12) and t2.bl = '' ;";

		sheiltoth.add(sheilta);


		//pensya

		sheilta = "Update " + name_schema + "." + tbl_sofi + " as t1 "
				+ "inner join " + name_schema + "." + name_hevra + "_exo as t2 "
				+ "on t1.symbol = t2.symbol  and t1.dyear = t2.dyear "
				+ "set t1.type_for_gemel = ';shulam_oved;pensya;sum_shulam;' , t1.type = ';tlush_reshut;'   "
				+ "where t2.ofi in (50) ;";

		sheiltoth.add(sheilta);

		sheilta = "Update " + name_schema + "." + tbl_sofi + " as t1 "
				+ "inner join " + name_schema + "." + name_hevra + "_exo as t2 "
				+ "on t1.symbol = t2.symbol  and t1.dyear = t2.dyear "
				+ "set t1.type_for_gemel = ';shulam_maavid;pensya;sum_shulam;'    "
				+ "where t2.ofi in (90) ;";

		sheiltoth.add(sheilta);




		//neto

		sheilta = "Update " + name_schema + "." + tbl_sofi + " as t1 "
				+ "inner join " + name_schema + "." + name_hevra + "_exo as t2 "
				+ "on t1.symbol = t2.symbol  and t1.dyear = t2.dyear "
				+ "set t1.type = ';tlush_neto;'    "
				+ "where t2.ofi in (99) ;";

		sheiltoth.add(sheilta);

		sheilta = "update " + name_schema + "." + tbl_sofi + " set type = ';nikuy_chova;semelMh;', type_for_gemel = '' where Symbol = '581'";

		sheiltoth.add(sheilta);

		sheilta = "update " + name_schema + "." + tbl_sofi + " set type = ';nikuy_chova;semelbb;', type_for_gemel = '' where Symbol = '584'";

		sheiltoth.add(sheilta);

		sheilta = "update " + name_schema + "." + tbl_sofi + " set type = ';nikuy_chova;semelbilo;', type_for_gemel = '' where Symbol = '582'";

		sheiltoth.add(sheilta);

		sheilta = "update " + name_schema + "." + tbl_sofi + " set type = ';semelbilm;' where Symbol = '982'";

		sheiltoth.add(sheilta);

		sheilta = "update " + name_schema + "." + tbl_sofi + " set type = ';tlush_tashlum;' where Symbol = '1'";

//		sheiltoth.add(sheilta);

		for (int i = 0; i < sheiltoth.size(); i++) {
			System.out.println(sheiltoth.get(i));
			tr.Insertintodb(sheiltoth.get(i));
		}

	}


}
