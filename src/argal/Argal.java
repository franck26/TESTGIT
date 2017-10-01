package argal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Statement;

import mySQL.Trysql;

public class Argal {
	Trysql tr = Trysql.getInstance();
	private static Argal a = null;

	public static Argal getInstance() {
		if (a == null)
			a = new Argal();
		return a;
	}

	public void create_table(String name_schema, String name_table) throws SQLException {

		System.out.println("create table " + name_schema + "." + name_table + "....");
		String s = "CREATE TABLE  if not exists  " + name_schema + "." + name_table
				+ "(`in_id` INT NOT NULL AUTO_INCREMENT," + "`id` VARCHAR(200) NULL,"  + "`num_worker` VARCHAR(200) NULL," 
				+ "`last_name` VARCHAR(200) NULL, " + "`first_name` VARCHAR(200) NULL, " 
				+ "`symbol` VARCHAR(200) NULL," + "`symbolName` VARCHAR(200) NULL," + "`total` VARCHAR(200) NULL,"
				+ "`type` VARCHAR(200) NULL," + "`m1` VARCHAR(200) NULL," + "`m2` VARCHAR(200) NULL,"
				+ "`m3` VARCHAR(200) NULL," + "`m4` VARCHAR(200) NULL," + "`m5` VARCHAR(200) NULL,"
				+ "`m6` VARCHAR(200) NULL," + "`m7` VARCHAR(200) NULL," + "`m8` VARCHAR(200) NULL,"
				+ "`m9` VARCHAR(200) NULL," + "`m10` VARCHAR(200) NULL," + "`m11` VARCHAR(200) NULL," 
				+ "`m12` VARCHAR(200) NULL," + "`tat` VARCHAR(200) NULL,"

				+ "`memoutsa` VARCHAR(200) NULL," + "`dyear` VARCHAR(200) NULL," + "`sikoum` VARCHAR(200) NULL, " + "PRIMARY KEY (`in_id`))";

		tr.Insertintodb1(s);

		s = "truncate table " + name_schema + "." + name_table;

		tr.Insertintodb1(s);

		
		System.out.println("create table ok !");

	}

	public void load(String name_schema, String name_table, int year, String path) throws SQLException {
		System.out.println("load data year " + year + " in process .......");

		String s = "LOAD DATA  LOCAL INFILE '" + path + "/" + year + ".csv' " + " "
				+ " INTO TABLE "
				+ name_schema + "." + name_table
				+ " FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' "
				+ "IGNORE 1 LINES   ( "
				+ "sikoum, "
				+ "tat, "
				+ "num_worker, "
				+ " last_name, first_name, "
//				+ "id,"
				+ " type,  symbol, symbolName, "
//				+ "total, "
				+ "m12, m11, m10, m9, m8, m7, m6, m5, m4, m3, m2, m1) "
				+ "set dyear=" + year;

		tr.Insertintodb(s);

		s = "delete from " + name_schema + "." + name_table + " where sikoum = 'סיכום  למפעל : '";

		tr.Insertintodb1(s);

		System.out.println("load data year " + year + " ok !");

	}

	public void create_tabel_101(String name_schema, String name_table) throws SQLException {

		System.out.println("create table 101 ...");

		String a = "CREATE TABLE  if not exists " + name_schema + "." + name_table + "(\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n" + "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n" + "  `FullName` varchar(51) NOT NULL,\n"
				+ "  `Symbol` int(11) NOT NULL,\n" + "  `SymbolName` varchar(100) NOT NULL,\n"
				+ "  `m1` double NOT NULL DEFAULT '0',\n" + "  `m2` double NOT NULL DEFAULT '0',\n"
				+ "  `m3` double NOT NULL DEFAULT '0',\n" + "  `m4` double NOT NULL DEFAULT '0',\n"
				+ "  `m5` double NOT NULL DEFAULT '0',\n" + "  `m6` double NOT NULL DEFAULT '0',\n"
				+ "  `m7` double NOT NULL DEFAULT '0',\n" + "  `m8` double NOT NULL DEFAULT '0',\n"
				+ "  `m9` double NOT NULL DEFAULT '0',\n" + "  `m10` double NOT NULL DEFAULT '0',\n"
				+ "  `m11` double NOT NULL DEFAULT '0',\n" + "  `m12` double NOT NULL DEFAULT '0',\n"
				+ "  `total` double NOT NULL DEFAULT '0',\n"
				+ "  `division` bigint(20) unsigned NOT NULL DEFAULT '0',\n" + "  `run_version` int(11) DEFAULT NULL,\n"
				+ "  `date_value` varchar(50) DEFAULT NULL,\n" + "  `source` varchar(100) DEFAULT NULL,\n"
				+ "  `type` varchar(400) DEFAULT NULL,\n" + "  `num_worker` int(11) unsigned DEFAULT '0',\n"
				+ "  `permission` int(11) unsigned DEFAULT NULL,\n" + "  `type_for_gemel` varchar(400) DEFAULT NULL,\n"
				+ "  PRIMARY KEY (`in_id`),\n"
				+ "  UNIQUE KEY `ind_unique` (`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,`division`) USING BTREE,\n"
				+ "  KEY `indi3` (`cid`,`dyear`,`Symbol`,`SymbolName`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";

		tr.Insertintodb1(a);

		a = "Truncate table " + name_schema + "." + name_table;
		tr.Insertintodb1(a);

		System.out.println("create table 101 ok !");
	}

	public void insertTo101(String name_schema, String name_table_101, String name_table, int cid) throws SQLException {

		System.out.println("insert in 101");

		String s = "";
		
		s = "insert ignore into " + name_schema + "." + name_table_101
				+ "(cid, dyear, FullName, id, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, type, source, num_worker) "
				+ "\n select " + cid
				+ ", dyear, concat(last_name, ' ' , first_name), num_worker, symbol, concat(symbolName, ' - ', type) , m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, type, 'original_argal'  , num_worker  "
				+ " from " + name_schema + "." + name_table+""
//				+ " where type = 'השתתפות'"
				+ "";

		System.out.println(s);
		
		tr.Insertintodb1(s);
		
//		s = "insert ignore into " + name_schema + "." + name_table_101
//				+ "(cid, dyear, FullName, id, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, type, num_worker) "
//				+ "select " + cid
//				+ ", dyear, concat(last_name, ' ' , first_name), id, symbol, symbolName , m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, type, id "
//				+ "from " + name_schema + "." + name_table+""
//				+ " where type <> 'השתתפות'";
//		System.out.println(s);
		
//		tr.Insertintodb1(s);

		
		s = "delete from " + name_schema + "." + name_table_101 + "   where id = 0 ;";
		tr.Insertintodb1(s);
		
		//bruto
		s = "update " + name_schema + "." + name_table_101 + "  set type = ';b_tlush;' where symbolname regexp 'תשלומים - סיכומים' or symbolname regexp 'שווה כסף - סיכומים';";
		tr.Insertintodb1(s);
		
		s = "update " + name_schema + "." + name_table_101 + "  set type = ';tlush_shovy;tlush_tashlum;' where symbolname regexp 'שווה כסף$' ;";
		tr.Insertintodb1(s);
		
		s = "update " + name_schema + "." + name_table_101 + "  set type = ';tlush_tashlum;' where type regexp 'תשלומים' ;";
		tr.Insertintodb1(s);
		
		//neto
		
		s = "update " + name_schema + "." + name_table_101 + "  set type = ';tlush_neto;' where symbolname regexp 'נטו לתשלום - סיכומים' ;";
		tr.Insertintodb1(s);
		
		
		
		s = "update " + name_schema + "." + name_table_101 + "  set type = ';nikuy_chova;' where type regexp 'נכויי חובה' ;";
		tr.Insertintodb1(s);
		
		s = "update " + name_schema + "." + name_table_101 + "  set type = ';nikuy_chova;semelbb;' where symbolname regexp 'מס בריאות - נכויי חובה' ;";
		tr.Insertintodb1(s);
		
		s = "update " + name_schema + "." + name_table_101 + "  set type = ';nikuy_chova;semelbilo;' where symbolname regexp 'ביטוח לאומי - נכויי חובה' ;";
		tr.Insertintodb1(s);
		
		
		
		s = "update " + name_schema + "." + name_table_101 + "  set type = ';tlush_reshut;', type_for_gemel = ';shulam_oved;pensya;sum_shulam;' where type regexp 'נכויי רשות';";
		tr.Insertintodb1(s);
		
		
		//pensya
		s = "update " + name_schema + "." + name_table_101 + "  set type_for_gemel = ';shulam_maavid;pensya;sum_shulam;' where type regexp 'השתתפות';";
		tr.Insertintodb1(s);
		
		//bituah leumi
		s = "update " + name_schema + "." + name_table_101 + "  set type = ';semelbilm;' where symbolname regexp 'ביטוח לאומי - השתתפות';";
		tr.Insertintodb1(s);
		
		
		
		System.out.println("insert perfect !!!");

	}

	public void updateTotal(String name_schema, String name_table_101) throws SQLException {
		String s = "update " + name_schema + "." + name_table_101
				+ " set total = m1+m2+m3+m4+m5+m6+m7+m8+m9+m10+m11+m12";
		tr.Insertintodb1(s);

		System.out.println("update total ok !!");
	}

	public void changeSymbolSymbolname(String name_schema, String name_table) throws SQLException {
		
		System.out.println("changeSymbolSymbolname");
		
		String s = "select distinct sikoum from " + name_schema + "." + name_table + " where sikoum <> ''";
		ResultSet rs = tr.gettabledb(s);
////		String a = "";
//		for (int i = 1; rs.next(); i++) {
////			a = "UPDATE " + name_schema + "." + name_table + " " + "SET symbolName='" + rs.getString(1) + "', symbol = "
////					+ i * 1000 + "WHERE sikoum ='" + rs.getString(1) + "' ;\n\n";
//
//			Connection conn = tr.getConnectionMySql().getConn();
//			PreparedStatement statement = conn.prepareStatement("UPDATE " + name_schema + "." + name_table + " "
//					+ "SET symbolName=?, symbol = '" + i * 1000 + "'" + "WHERE sikoum =? ;\n\n");
//
//			statement.setString(1, rs.getString(1));
//			statement.setString(2, rs.getString(1));
//
//			statement.executeUpdate();
//
//		}

		// s = "SELECT distinct symbol, symbolName FROM " + name_schema + "." +
		// name_table;
		// rs = tr.gettabledb(s);
		// while(rs.next()){
		//
		//// System.out.println(j + " " + rs.getString("symbolName"));
		//// a = "UPDATE " + name_schema + "." + name_table + " "
		//// + "SET symbol = '" + j + "' "
		//// + "WHERE symbolName ='" + rs.getString(2) + "' ;\n\n";
		////
		// Connection conn = tr.getConnectionMySql().getConn();
		// PreparedStatement statement = conn.prepareStatement("UPDATE " +
		// name_schema + "." + name_table + ""
		// + " SET symbol = '" + j + "' "
		// + "WHERE symbolName =?;\n\n");
		////
		// statement.setString(1,rs.getString(2));
		////
		// statement.executeUpdate();
		//////
		// System.out.println(j++);
		// }

		s = "SELECT distinct symbol, symbolName, type FROM " + name_schema + "." + name_table + " where symbolName like '=%' "
				+ "or symbolName = '' " + "or symbolName = 'ותק===='";
		System.out.println(s);
		rs = tr.gettabledb(s);
		while (rs.next()) {
			Connection conn = tr.getConnectionMySql().getConn();
			PreparedStatement statement = conn.prepareStatement("UPDATE " + name_schema + "." + name_table + ""
					+ " SET symbolName = ?" + "WHERE symbolName =?;\n\n");
			//
			statement.setString(1, rs.getString(3));
			statement.setString(2, rs.getString(2));

			statement.executeUpdate();
//			System.out.println(rs.getString(2));
		}
		
		s = "SELECT distinct SymbolName, Symbol FROM " + name_schema +"." + name_table + " where symbol = 0;";
		System.out.println(s);
		rs = tr.gettabledb(s);
		int k = 200;
		while (rs.next()) {
			Connection conn = tr.getConnectionMySql().getConn();
			PreparedStatement statement = conn.prepareStatement("UPDATE " + name_schema + "." + name_table + ""
					+ " SET symbol = ? " + " WHERE symbolName =?;\n\n");
			//
			statement.setInt(1, k++);
			statement.setString(2, rs.getString(1));

			statement.executeUpdate();
//			System.out.println(rs.getString(2));
		}
	}

    void create_table_alphon(String name_schema, String string) throws SQLException {
        
        String a = "CREATE TABLE if not exists " + name_schema +"." + string + " (\n" +
"  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n" +
"  `num_worker` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
"  `id` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
"  `last_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
"  `first_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
"  `mahlaka` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
"  `tat` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
"  `birthday` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
"  `is_male` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
"  `cid` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
"  PRIMARY KEY (`in_id`)\n" +
") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
        
        tr.Insertintodb(a);
        
        tr.Insertintodb("truncate table " + name_schema +"." + string);
        
        
    }

    void load_alphon(String name_schema, String string, String path, int cid) throws SQLException {
        
        System.out.println("argal.Argal.load_alphon()");
        
        String a = "load data local infile '" + path + "/alphon.csv' "
                + "into table " + name_schema + "." + string + " " +
                "FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n'  IGNORE 1 LINES "
                + "(id, num_worker,  last_name, first_name, "
             
//                + "mahlaka, "
//                + "tat, "
                + "is_male, birthday ) set cid = " + cid + " ;";
        
        tr.Insertintodb(a);
        
        
        a = "update " + name_schema + "." + string + " set is_male = 1 where is_male regexp 'ז';";
        tr.Insertintodb(a);
        
        a = "update " + name_schema + "." + string + " set is_male = 0 where is_male regexp 'נ';";
        tr.Insertintodb(a);
        
        a = "update " + name_schema + "." + string + " set id = substring_index(id, '.', 1);";
        tr.Insertintodb(a);
        
        a = "update " + name_schema + "." + string + " set birthday = concat(0, birthday) where substring_index(birthday, '/', 1) < 10;" ; 
        tr.Insertintodb(a);
        
        a = "update " + name_schema + "." + string + " set birthday = concat( substring_index(birthday, '/', 1),  '/0',  substring_index(substring_index(birthday, '/', 2), '/', -1),  '/',  substring_index(birthday, '/', -1)) where substring_index(substring_index(birthday, '/', 2), '/', -1) < 10;" ; 
        tr.Insertintodb(a);
        
        
    }

    void create_tabel_101_details(String name_schema, String string) throws SQLException {
        String a  = "CREATE TABLE if not exists " + name_schema + "." + string + " (\n" +
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
"  KEY `Index_2` (`cid`,`dyear`,`id`,`month`),\n" +
"  KEY `index4` (`cid`,`id`),\n" +
"  KEY `index5` (`cid`,`num_worker`)\n" +
") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='InnoDB free: 1508352 kB; InnoDB free: 2484224 kB; InnoDB fre';";
        
        tr.Insertintodb(a);
        
        tr.Insertintodb("truncate table " + name_schema +"." + string);
    }

    void insertTo101Details(String name_schema, String string101, String string, int cid) throws SQLException {
        
        String a = "INSERT INTO " + name_schema +"." + string101 + "\n" +
"(cid, last_name, first_name, birthday, is_male, num_worker, id, dyear, month, intra_division, intra_division_exp ) \n" +
"select cid, last_name, first_name, birthday, is_male, num_worker, id , 0, '0', '', '' from " + name_schema +"." + string + " ;";
        
        tr.Insertintodb(a);
    }
    
    

}
