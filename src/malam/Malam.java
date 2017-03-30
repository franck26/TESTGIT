/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package malam;

import java.sql.SQLException;
import java.util.Vector;

import mySQL.Trysql;

/**
 *
 * @author Franck
 */
public class Malam {

	Malam(){
		tr = Trysql.getInstance();
	}



	private static Malam instance = null;
	private Trysql tr;

	public static Malam getInstance(){			
		if (instance == null)
		{ 	
			instance = new Malam();	
		}
		return instance;
	}

	public void createTable(String name_schema, String name_table, String fields) throws SQLException{

		String[] fieldss = fields.split(", ");

		String s = "CREATE TABLE  if not exists  " + name_schema + "." + name_table + "\n"
				+ "(`in_id` INT NOT NULL AUTO_INCREMENT,\n"
				+ "`dyear` VARCHAR(200),\n";

		for (String string : fieldss) {
			s += "`" + string + "` VARCHAR(200), \n";
		}

		s += "PRIMARY KEY (`in_id`));";

		//		System.out.println(s);

		tr.Insertintodb(s);

		truncate(name_schema, name_table);
	}

	public void Malam_replace_comma(String name_schema, String name_table) throws SQLException {

		String a = "update " + name_schema + "." + name_table + "\n"
				+ "set m1=replace(m1,',','')\n"
				//				+ "m2=replace(m2,',',''),\n"
				//				+ "m3=replace(m3,',',''),\n"
				//				+ "m3=replace(m3,',',''),\n"
				//				+ "m4=replace(m4,',',''),\n"
				//				+ "m5=replace(m5,',',''),\n"
				//				+ "m6=replace(m6,',',''),\n"
				//				+ "m7=replace(m7,',',''),\n"
				//				+ "m8=replace(m8,',',''),\n"
				//				+ "m9=replace(m9,',',''),\n"
				//				+ "m10=replace(m10,',',''),\n"
				//				+ "m11=replace(m11,',',''),\n"
				//				+ "m12 =replace(m12,',','');"
				;
		tr.Insertintodb1(a);

	}

	public void Malam_create_first_table(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE  if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `office_num` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `office_name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `numWorker` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `idWorker` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,  \n"
				+ "  `m_n` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `derug` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `darga` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `start_date` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `vetek` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `department` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `unit` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `symbol_type` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `symbolName` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m1` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m1_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m2` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m2_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m3` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m3_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m4` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m4_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m5` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m5_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m6` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m6_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m7` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m7_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m8` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m8_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m9` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m9_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m10` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m10_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m11` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m11_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m12` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m12_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `all_sum` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `all_amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "   `dyear` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL\n"
				+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		tr.Insertintodb1(a);

		truncate(name_schema, name_table);

	}

	public void Malam_load_data_1(String name_schema, int year, String name_table, String kidod) throws SQLException {

		String a = "LOAD DATA  LOCAL INFILE "
				+ " '/home/user1/hevra/hevroth/amnon/" + year + ".csv'"
				+ "    INTO TABLE " + name_schema + ".`" + name_table + "` \n"
				+ kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n"
				+ "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE 6 LINES\n"
				+ "     ( office_num, office_name, `name`, numWorker, idWorker, m_n, derug, "
				+ "darga, start_date, vetek, department, unit, symbol_type, symbolName, m1, m1_amount, "
				+ "m2, m2_amount, m3, m3_amount, m4, m4_amount, m5, m5_amount, m6, m6_amount, m7, m7_amount, "
				+ "m8, m8_amount, m9, m9_amount, m10, m10_amount, m11, m11_amount, m12, m12_amount, all_sum, all_amount)\n"
				+ "     set dyear=" + year;

		tr.Insertintodb1(a);

		a = "truncate table " + name_schema + "." + name_table;
		tr.Insertintodb1(a);
	}

	public void Malam_load_data_fatal(String name_schema, String year, String name_table, String name_file, String kidod, String fields, int numLinesIgnores)
			throws SQLException {
		System.out.println("load file " + name_file);
		String a = "LOAD DATA  LOCAL INFILE  '" + name_file + ".csv'"
				+ "    INTO TABLE " + name_schema + ".`" + name_table + "` \n" + kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n" 
				+ "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE " + numLinesIgnores + " LINES\n" + "     ( " + fields + ")\n"
				+ "     set dyear=" + year;
		tr.Insertintodb1(a);
		System.out.println("end load file " + name_file);
	}


	public void Malam_create_table_get_the_right_coulms(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists `" + name_schema + "`.`" + name_table + "`\n"
				+ "(\n"
				+ "`in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
				+ "`dyear` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`id` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`FullName` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`Symbol` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`SymbolName` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`symbol_type` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`m1` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"    
				+ "`m2` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "`m3` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "`m4` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "`m5` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "`m6` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "`m7` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "`m8` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "`m9` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "`m10` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "`m11` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "`m12` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "`cid` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n"
				+ "PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		tr.Insertintodb1(a);

		truncate(name_schema, name_table);
	}




	public void Malam_insert_into_temp(String name_schema, String name_table_101, String name_table) throws SQLException {

		String a = "INSERT  INTO `" + name_schema + "`.`" + name_table_101 + "`\n"
				+ "(\n"
				+ "`dyear`,\n"
				+ "`id`,\n"
				+ "`FullName`,\n"
				+ "`Symbol`,\n"
				+ "`SymbolName`,\n"
				+ "`symbol_type`,\n"
				+ "`m1`,\n"
				+ "`m2`,\n"
				+ "`m3`,\n"
				+ "`m4`,\n"
				+ "`m5`,\n"
				+ "`m6`,\n"
				+ "`m7`,\n"
				+ "`m8`,\n"
				+ "`m9`,\n"
				+ "`m10`,\n"
				+ "`m11`,\n"
				+ "`m12`, \n"
				+ "`cid`\n"
				+ ")\n"
				//				0544939268
				//				
				//				0737860860
				+ "SELECT\n"
				+ "dyear,idWorker, `name`,\n"
				+ "SUBSTRING_INDEX(SymbolName,'-',1),\n"
				+ "SUBSTR(SymbolName,instr(SymbolName,'-')+1,LENGTH(SymbolName)),`symbol_type`,\n"
				+ "sum(m1) as m1, sum(m2) as m2, sum(m3) as m3, sum(m4) as m4, sum(m5) as m5, sum(m6) as m6, sum(m7) as m7, sum(m8) as m8,\n"
				+ "sum(m9) as m9, sum(m10) as m10, sum(m11) as m11, sum(m12) as m12, misrad\n"
				+ "FROM " + name_schema + "." + name_table + "\n"
				+ "group by dyear,idWorker,SymbolName,`symbol_type` \n"
				+ " on duplicate key update m1=values(m1),m2=values(m2),m3=values(m3),m4=values(m4),m5=values(m5),m6=values(m6),m7=values(m7),m8=values(m8),m9=values(m9),m10=values(m10),m11=values(m11),m12=values(m12);";
		tr.Insertintodb1(a);

	}

	public void set_symble(String name_schema, String name_table) throws SQLException {

		String a = "UPDATE `" + name_schema + "`.`" + name_table + "`\n"
				+ "SET symbol= '6565'\n"
				+ "WHERE symbolname='עלות מעביד';";
		tr.Insertintodb1(a);

	}

	public void create_101_avidar(String name_schema, String name_table) throws SQLException {


		String a = "CREATE TABLE if not exists  "+name_schema+".`" + name_table + "` (\n"+
				"  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `cid` int(10) unsigned NOT NULL,\n" +
				"  `dyear` smallint(6) NOT NULL,\n" +
				"  `id` int(11) NOT NULL,\n" +
				"  `original_id` int(11) DEFAULT NULL,\n" +
				"  `FullName` varchar(51) DEFAULT NULL,\n" +
				"  `Symbol` varchar(100) NOT NULL DEFAULT '0' ,\n" +
				"  `SymbolName` varchar(100) NOT NULL,\n" +
				"  `m1` double NOT NULL DEFAULT '0',\n" +
				"  `m2` double NOT NULL DEFAULT '0',\n" +
				"  `m3` double NOT NULL DEFAULT '0',\n" +
				"  `m4` double NOT NULL DEFAULT '0',\n" +
				"  `m5` double NOT NULL DEFAULT '0',\n" +
				"  `m6` double NOT NULL DEFAULT '0',\n" +
				"  `m7` double NOT NULL DEFAULT '0',\n" +
				"  `m8` double NOT NULL DEFAULT '0',\n" +
				"  `m9` double NOT NULL DEFAULT '0',\n" +
				"  `m10` double NOT NULL DEFAULT '0',\n" +
				"  `m11` double NOT NULL DEFAULT '0',\n" +
				"  `m12` double NOT NULL DEFAULT '0',\n" +
				"  `total` double NOT NULL DEFAULT '0',\n" +
				"  `division` bigint(20) unsigned NOT NULL DEFAULT '0',\n" +
				"  `run_version` int(11) DEFAULT NULL,\n" +
				"  `date_value` varchar(50) DEFAULT NULL,\n" +
				"  `source` varchar(100) DEFAULT NULL,\n" +
				"  `type` varchar(400) DEFAULT NULL,\n" +
				"  `num_worker` int(11) unsigned DEFAULT '0',\n" +
				"  `permission` int(11) unsigned DEFAULT NULL,\n" +
				"  `type_for_gemel` varchar(400) DEFAULT NULL,\n" +
				"  `Symbol1` varchar(100) NOT NULL DEFAULT '0' ,\n" +
				"  PRIMARY KEY (`in_id`),\n" +
				"  UNIQUE KEY `ind_unique` (`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,`division`) USING BTREE,\n" +
				"  KEY `indi3` (`cid`,`dyear`,`Symbol`,`SymbolName`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";
		tr.Insertintodb1(a);

		truncate(name_schema, name_table);

	}

	public void update_total(String name_schema, String name_table_101) throws SQLException {

		String a = "update " + name_schema + "." + name_table_101 + " set total=m1+m2+m3+m4+m5+m6+m7+m8+m9+m10+m11+m12";
		tr.Insertintodb1(a);

		System.out.println("finish table : " + name_table_101);

	}

	public void Malam_create_table_get_the_right_coulms_isrotel(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists `" + name_schema + "`.`" + name_table + "`\n"
				+ "(\n"
				+ "`in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
				+ "`cid` varchar(45)  DEFAULT NULL,\n"
				+ "`dyear` varchar(45)  DEFAULT NULL,\n"
				+ "`id` varchar(45)  DEFAULT NULL,\n"
				+ "`FullName` varchar(200)  DEFAULT NULL,\n"
				+ "`Symbol` varchar(45)  DEFAULT NULL,\n"
				+ "`SymbolName` varchar(200)  DEFAULT NULL,\n"
				+ "`m1` varchar(45)  DEFAULT NULL,\n"
				+ "`m2` varchar(45)  DEFAULT NULL,\n"
				+ "`m3` varchar(45)  DEFAULT NULL,\n"
				+ "`m4` varchar(45)  DEFAULT NULL,\n"
				+ "`m5` varchar(45)  DEFAULT NULL,\n"
				+ "`m6` varchar(45)  DEFAULT NULL,\n"
				+ "`m7` varchar(45)  DEFAULT NULL,\n"
				+ "`m8` varchar(45)  DEFAULT NULL,\n"
				+ "`m9` varchar(45)  DEFAULT NULL,\n"
				+ "`m10` varchar(45)  DEFAULT NULL,\n"
				+ "`m11` varchar(45)  DEFAULT NULL,\n"
				+ "`m12` varchar(45)  DEFAULT NULL,\n"
				+ "PRIMARY KEY (`in_id`)\n"
				+ ") ;";
		tr.Insertintodb1(a);


	}

	public void truncate(String name_scema, String name_table) throws SQLException {

		String a="    TRUNCATE table `"+name_scema+"`.`"+name_table+"`";

		tr.Insertintodb1(a);

		System.out.println(a);
	}

	public void insert_into_101_isrotel(String name_schema, String name_table_101, String name_table_temp) throws SQLException {
		String a="INSERT INTO  `"+name_schema+"`.`"+name_table_101+"`\n" +
				"(\n" +
				"`cid`,\n" +
				"`dyear`,\n" +
				"`id`,\n" +
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
				"`m12`"
				+ ")\n" +

                "SELECT \n" +
                "`cid`\n" +              
                "  ,`dyear`,\n" +
                "    `id`,\n" +
                "    `FullName`,\n" +
                "  `Symbol`,\n" +
                "   `SymbolName`,\n" + 
                "sum(m1), sum(m2),sum(m3),sum(m4),sum(m5),sum(m6),sum(m7),sum(m8),sum(m9),sum(m10),sum(m11),sum(m12)\n"
                +"FROM  `"+name_schema+"`.`"+name_table_temp+"`"
                + " group by  cid,  dyear, id, Symbol, SymbolName \n";

		tr.Insertintodb1(a);

	}

	public void deleteEmpty(String name_scema, String name_table) throws SQLException{

		String a=" Delete from `"+name_scema+"`.`"+name_table+"` "
				+ "where SymbolName = '' AND Symbol = '' ";
		tr.Insertintodb1(a);

	}

	public void create_table_fanitzia(String name_schema,String name_table) throws SQLException{


		String a="CREATE TABLE  if not exists  "+ name_schema +".`"+name_table+"` (\n" +
				"  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n" +
				"  `F_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `L_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `num_oved` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `id` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `seif_taktzivi` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `Symbol` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `Symbol_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m1_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m1_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m2_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m2_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m3_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m3_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m4_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m4_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m5_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m5_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m6_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m6_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m7_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m7_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m8_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m8_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m9_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m9_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m10_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m10_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m11_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m11_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m12_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m12_c` varchar(45) CHARACTER SET utf8 DEFAULT NULL,\n" +
				"  `dyear` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `cid` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  PRIMARY KEY (`in_id`)\n" +
				") ";
		tr.Insertintodb1(a);


	}

	public void Malam_load_data_fanitzia(String pathFile, String name_schema, int  year, String name_table,int cid, int numLine) throws SQLException {

		String a = "LOAD DATA  LOCAL INFILE "
				+ " '" + pathFile + year + ".csv'"
				+ "    INTO TABLE " + name_schema + ".`" + name_table + "` \n"
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n"
				+ "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE " + numLine + " LINES\n"
				+"( num_oved, id,L_name,F_name,Symbol, Symbol_name, m1_s, m2_s,m3_s,m4_s,m5_s,m6_s,m7_s,m8_s,m9_s,m10_s,m11_s,m12_s,dyear, cid)"
				+ "     set dyear=" + year+" , cid="+cid;
		tr.Insertintodb1(a);

	}

	public void Malam_load_data_fanitzia2(String pathFile, String name_schema, int  year, String name_table,int cid, int numLine) throws SQLException {

		String a = "LOAD DATA  LOCAL INFILE "
				+ " '" + pathFile + year + ".csv'"
				+ "    INTO TABLE " + name_schema + ".`" + name_table + "` \n"
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n"
				+ "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE " + numLine + " LINES\n"
				+"( num_oved, L_name, F_name,Symbol, Symbol_name, m1_s, m2_s,m3_s,m4_s,m5_s,m6_s,m7_s,m8_s,m9_s,m10_s,m11_s,m12_s,dyear, cid)"
				+ "     set dyear=" + year+" , cid="+cid;
		tr.Insertintodb1(a);

	}

	public void Malam_insert_into_temp_isrotel(String name_schema, String name_table, String name_table1,int year) throws SQLException {

		String a = "INSERT  INTO `" + name_schema + "`.`" + name_table + "`\n"
				+ "(\n" 
				+ "`cid`,\n"
				+ "`dyear`,\n"
				+ "`id`,\n"
				+ "`FullName`,\n"
				+ "`Symbol`,\n"
				+ "`SymbolName`,\n"

                + "`m1`,\n"
                + "`m2`,\n"
                + "`m3`,\n"
                + "`m4`,\n"
                + "`m5`,\n"
                + "`m6`,\n"
                + "`m7`,\n"
                + "`m8`,\n"
                + "`m9`,\n"
                + "`m10`,\n"
                + "`m11`,\n"
                + "`m12`\n"
                + ")SELECT cid,  \n" 

+year+", `id`,  concat(`F_name`,' ',`L_name`),\n" +
"   `Symbol`,`Symbol_name`,`m1_s`,`m2_s`,`m3_s`,`m4_s`,`m5_s`,`m6_s`,`m7_s`,`m8_s`,`m9_s`,`m10_s`,`m11_s`,`m12_s`\n" +
"  \n" +
"FROM  `"+name_schema+"`.`"+name_table1+"`"
+ "where Symbol <> '' AND Symbol_name <> '';";

		tr.Insertintodb1(a);

	}
	public void Malam_insert_into_temp_isrotel2(String name_schema, String name_table, String name_table1,int year) throws SQLException {

		String a = "INSERT  INTO `" + name_schema + "`.`" + name_table + "`\n"
				+ "(\n" 
				+ "`cid`,\n"
				+ "`dyear`,\n"
				+ "`FullName`,\n"
				+ "`Symbol`,\n"
				+ "`SymbolName`,\n"

                + "`m1`,\n"
                + "`m2`,\n"
                + "`m3`,\n"
                + "`m4`,\n"
                + "`m5`,\n"
                + "`m6`,\n"
                + "`m7`,\n"
                + "`m8`,\n"
                + "`m9`,\n"
                + "`m10`,\n"
                + "`m11`,\n"
                + "`m12`\n"
                + ")SELECT cid,  \n" 

+year+",  concat(`F_name`,' ',`L_name`),\n" +
"   `Symbol`,`Symbol_name`,`m1_s`,`m2_s`,`m3_s`,`m4_s`,`m5_s`,`m6_s`,`m7_s`,`m8_s`,`m9_s`,`m10_s`,`m11_s`,`m12_s`\n" +
"  \n" +
"FROM  `"+name_schema+"`.`"+name_table1+"`"
+ "where Symbol <> '' AND Symbol_name <> '';";

		tr.Insertintodb1(a);

	}



	public void load_data_avidar12(String name_schema,String name_table) throws SQLException {


		String b="LOAD DATA LOCAL INFILE "
				+ " '/home/user1/hevra/hevroth/amnon/2016"
				+ ".csv'"+
				"INTO TABLE " + name_schema + "."+name_table+" FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES\n" +
				" (mis_oved, tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n" +
				"  m12, m1, m2, m3, m4, m5, m6, m7, m8, m9, \n" +
				" m10, m11, total, avg, avg_p, sub,dyear) set dyear=2016";
		tr.Insertintodb1(b);

	}

	public  void create_table_avidar12(String name_schema,String name_table) throws SQLException{
		String a="CREATE TABLE   if not exists `"+name_schema+"`.`"+name_table+"` (\n" +
				"  `in_id` INT NOT NULL AUTO_INCREMENT,\n" +
				"  `mis_oved` VARCHAR(45) NULL,\n" +
				"  `tat_mifal` VARCHAR(60) NULL,\n" +
				"  `agaf` VARCHAR(45) NULL,\n" +
				"  `machlaka` VARCHAR(45) NULL,\n" +
				"  `name_machlaka` VARCHAR(45) NULL,\n" +
				"  `derog` VARCHAR(45) NULL,\n" +
				"  `tear_derog` VARCHAR(45) NULL,\n" +
				"  `darga` VARCHAR(45) NULL,\n" +
				"  `teor_darga` VARCHAR(45) NULL,\n" +
				"  `tchilat_avoda` VARCHAR(45) NULL,\n" +
				"  `id` VARCHAR(45) NULL,\n" +
				"  `kod_esok` VARCHAR(45) NULL,\n" +
				"  `tear_isok` VARCHAR(45) NULL,\n" +
				"  `symbol` VARCHAR(45) NULL,\n" +
				"  `symbol_name` VARCHAR(60) NULL,\n" +
				"  `m12` VARCHAR(45) NULL,\n" +
				"  `m1` VARCHAR(45) NULL,\n" +
				"  `m2` VARCHAR(45) NULL,\n" +
				"  `m3` VARCHAR(45) NULL,\n" +
				"  `m4` VARCHAR(45) NULL,\n" +
				"  `m5` VARCHAR(45) NULL,\n" +
				"  `m6` VARCHAR(45) NULL,\n" +
				"  `m7` VARCHAR(45) NULL,\n" +
				"  `m8` VARCHAR(45) NULL,\n" +
				"  `m9` VARCHAR(45) NULL,\n" +
				"  `m10` VARCHAR(45) NULL,\n" +
				"  `m11` VARCHAR(45) NULL,\n" +
				"  `total` VARCHAR(45) NULL,\n" +
				"  `avg` VARCHAR(45) NULL,\n" +
				"  `avg_p` VARCHAR(45) NULL,\n" +
				"  `sub` VARCHAR(45) NULL,\n" +
				"  `dyear` VARCHAR(45) NULL,\n" +
				"  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

		truncate(name_schema, name_table);

	}

	public void create_symbels_numbers_and_insert_into_main_table_ikea(String name_scema, String name_tabletemp, String name_table_101) throws SQLException {
		String a = "CREATE TABLE  if not exists `" + name_scema + "`.`" + name_tabletemp + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n"
				+ "  `symble` VARCHAR(200) NULL,\n"
				+ "  `symblenum` VARCHAR(200) NULL,\n"
				+ "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);
		String x = "TRUNCATE `" + name_scema + "`.`" + name_tabletemp + "`;";
		tr.Insertintodb1(x);
		String s = "ALTER TABLE `" + name_scema + "`.`" + name_tabletemp + "`  AUTO_INCREMENT = 2001";
		tr.Insertintodb1(s);

		String j = "insert into " + name_scema + "." + name_tabletemp + "(symble,symblenum)  SELECT Symbol,  SymbolName FROM " + name_scema + "." + name_table_101 + " group by Symbol ,  SymbolName;";
		tr.Insertintodb1(j);

		//        String insert = "UPDATE  " + name_scema + "." + name_table_101 + "  SET Symbol=(\n"
		//                + "   SELECT in_id FROM " + name_scema + "." + name_tabletemp + " WHERE  " + name_tabletemp + ".symblenum = " + name_table_101 + ".SymbolName  COLLATE utf8_general_ci);";
		//        tr.Insertintodb1(insert);

		//          String insert = "update  "+name_scema+"."+name_table_101+"  as t1, "+name_scema+"."+name_tabletemp +" as sy  set t1.Symbol1=sy.in_id\n" +
		//"    where  \n" +
		//"        (     t1.SymbolName COLLATE utf8_general_ci=sy.symble  \n" +
		//"  and  t1.Symbol COLLATE utf8_general_ci=sy.symblenum )";
		//         tr.Insertintodb1(insert);


		String    aa=" update   " + name_scema + "."+name_table_101+"  as t1, " + name_scema + "."+name_tabletemp+" as sy \n" +
				"      set t1.Symbol=sy.in_id\n" +
				"    where  \n" +
				"        (     t1.Symbol COLLATE utf8_general_ci=sy.symble  \n" +
				"  and  t1.SymbolName COLLATE utf8_general_ci=sy.symblenum )";
		tr.Insertintodb1(aa);

	}


	public  void create_table_emon(String name_schema,String name_table) throws SQLException{
		String a="CREATE TABLE   if not exists `"+name_schema+"`.`"+name_table+"` (\n" +
				"  `in_id` INT NOT NULL AUTO_INCREMENT,\n" +
				"  `mis_oved` VARCHAR(45) NULL,\n" +
				"  `tat_mifal` VARCHAR(60) NULL,\n" +
				"  `first_name` VARCHAR(45) NULL,\n" +
				"  `last_name` VARCHAR(60) NULL,\n" +
				"  `agaf` VARCHAR(45) NULL,\n" +
				"  `machlaka` VARCHAR(45) NULL,\n" +
				"  `name_machlaka` VARCHAR(45) NULL,\n" +
				"  `derog` VARCHAR(45) NULL,\n" +
				"  `tear_derog` VARCHAR(45) NULL,\n" +
				"  `darga` VARCHAR(45) NULL,\n" +
				"  `teor_darga` VARCHAR(45) NULL,\n" +
				"  `tchilat_avoda` VARCHAR(45) NULL,\n" +
				"  `id` VARCHAR(45) NULL,\n" +
				"  `kod_esok` VARCHAR(45) NULL,\n" +
				"  `tear_isok` VARCHAR(45) NULL,\n" +
				"  `symbol` VARCHAR(45) NULL,\n" +
				"  `symbol_name` VARCHAR(60) NULL,\n" +
				"  `m1` VARCHAR(45) NULL,\n" +
				"  `m2` VARCHAR(45) NULL,\n" +
				"  `m3` VARCHAR(45) NULL,\n" +
				"  `m4` VARCHAR(45) NULL,\n" +
				"  `m5` VARCHAR(45) NULL,\n" +
				"  `m6` VARCHAR(45) NULL,\n" +
				"  `m7` VARCHAR(45) NULL,\n" +
				"  `m8` VARCHAR(45) NULL,\n" +
				"  `m9` VARCHAR(45) NULL,\n" +
				"  `m10` VARCHAR(45) NULL,\n" +
				"  `m11` VARCHAR(45) NULL,\n" +
				"  `m12` VARCHAR(45) NULL,\n" +
				"  `total` VARCHAR(45) NULL,\n" +
				"  `avg` VARCHAR(45) NULL,\n" +
				"  `avg_p` VARCHAR(45) NULL,\n" +
				"  `sub` VARCHAR(45) NULL,\n" +
				"  `dyear` VARCHAR(45) NULL,\n" +
				"  `symbol1` VARCHAR(45) NULL,\n" +
				"  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

		truncate(name_schema, name_table);


	}

	public void load_data_emon(String name_schema,String name_table,int year) throws SQLException {
		String b="LOAD DATA LOCAL INFILE "
				+ " '/home/user1/hevra/hevroth/nikayon/"+year+".csv'"+
				"INTO TABLE " + name_schema + "." + name_table +" FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n' IGNORE 2 LINES \n" +
				" (mis_oved,tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n" +
				" m12,m1, m2, m3, m4, m5, m6, m7, m8, m9, \n" +
				" m10, m11,  total, avg, avg_p, sub,dyear) set dyear="+year;
		tr.Insertintodb1(b);
	}

	public void convert_to_101_9(String name_schema, String name,String name_table,int cid) throws SQLException {

		//		for (int i = 1; i < 13; i++) {



		String load8 = "insert into " + name_schema + "." + name + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,Symbol,`SymbolName`,m12,type, total) \n"
				+ "            SELECT CONCAT ( `first_name`, ' ' , `last_name` )  , "+ cid+",`dyear`,`id`,`symbol`, `symbol_name`,sum(m12),sub, sum(total)\n"
				+ "            FROM " + name_schema + "."+name_table+"  group by dyear,id,symbol,`symbol_name`, sub"
				+ "   on duplicate key update m12=values(m12);";

		tr.Insertintodb1(load8);
		//		}
	}


	public void create_101_malam(String name_schema, String name_table) throws SQLException {

		System.out.println("create table 101");

		String a = "CREATE TABLE  if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "  `cid` int(45) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(45) NOT NULL,\n" + "  `id` int(45) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n" + "  `FullName` varchar(200) NOT NULL,\n"
				+ "  `Symbol` int(45) NOT NULL,\n" + "  `SymbolName` varchar(100) NOT NULL,\n"
				+ "  `m1` double  DEFAULT NULL ,\n" + "  `m2` double  DEFAULT NULL ,\n"
				+ "  `m3` double  DEFAULT NULL ,\n" + "  `m4` double  DEFAULT NULL ,\n"
				+ "  `m5` double  DEFAULT NULL ,\n" + "  `m6` double  DEFAULT NULL ,\n"
				+ "  `m7` double DEFAULT NULL ,\n" + "  `m8` double  DEFAULT NULL ,\n"
				+ "  `m9` double  DEFAULT NULL  ,\n" + "  `m10` double  DEFAULT NULL ,\n"
				+ "  `m11` double  DEFAULT NULL ,\n" + "  `m12` double  DEFAULT NULL ,\n"
				+ "  `total` double  DEFAULT NULL ,\n"
				+ "  `division` bigint(20) unsigned NOT NULL DEFAULT '0',\n" + "  `run_version` int(11) DEFAULT NULL,\n"
				+ "  `date_value` varchar(50) DEFAULT NULL,\n" + "  `source` varchar(100) DEFAULT NULL,\n"
				+ "  `type` varchar(400) DEFAULT NULL,\n" + "  `num_worker` int(11) unsigned DEFAULT '0',\n"
				+ "  `permission` int(11) unsigned DEFAULT NULL,\n" + "  `type_for_gemel` varchar(400) DEFAULT NULL,\n"
				+ "  PRIMARY KEY (`in_id`),\n"
				+ "  UNIQUE KEY `ind_unique` (`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,`division`) USING BTREE,\n"
				+ "  KEY `indi3` (`cid`,`dyear`,`Symbol`,`SymbolName`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";
		tr.Insertintodb1(a);

		truncate(name_schema, name_table);

	}


	public void Malam_insert_into_101_fatal(String name_schema, String name_table_101, String name_table) throws SQLException {

		String a = "";
		System.out.println("insert to 101 fatal");

		a += "insert ignore into " + name_schema + "." + name_table_101 + "\n"
				+ "(cid, dyear, id, fullname, symbol, symbolname, m1, num_worker, type"
								+ ", m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12"
				+ ")"
				+"Select misrad, dyear, t_z, shem_oved, substring_index(symbolname, ' - ', 1), substring_index(symbolname, ' - ', -1), m1, mis_oved, soug "
								+ ", m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12 " 
				+ "From " + name_schema + "." + name_table + ";"
				;

		tr.Insertintodb1(a);

	}

}
