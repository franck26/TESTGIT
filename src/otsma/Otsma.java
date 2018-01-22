package otsma;

import java.io.File;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import mySQL.Trysql;

public class Otsma {

	static Otsma o = null;
	private Trysql tr;

	public Trysql getTr() {
		return tr;
	}

	private Otsma() {
		tr = Trysql.getInstance();
	}

	static Otsma getInstance() {
		if (o == null) {
			o = new Otsma();
		}

		return o;
	}

	public void create_table(String name_schema, String name_table, int cid) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" 
				+ "  `cid` int(10) unsigned NOT NULL DEFAULT '"+cid+"',\n"
				+ "  `num_worker` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(45) NULL,\n" + "  `l_name` VARCHAR(45) NULL,\n"
				+ "  `tat_mifal` VARCHAR(60) NULL,\n" + "  `agaf` VARCHAR(45) NULL,\n"
				+ "  `machlaka` VARCHAR(45) NULL,\n" + "  `name_machlaka` VARCHAR(45) NULL,\n"
				+ "  `derog` VARCHAR(45) NULL,\n" + "  `tear_derog` VARCHAR(45) NULL,\n"
				+ "  `darga` VARCHAR(45) NULL,\n" + "  `teor_darga` VARCHAR(45) NULL,\n"
				+ "  `tchilat_avoda` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `kod_esok` VARCHAR(45) NULL,\n" + "  `tear_isok` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(60) NULL,\n"
				+ "  `m1` VARCHAR(45) NULL,\n" + "  `m2` VARCHAR(45) NULL,\n" + "  `m3` VARCHAR(45) NULL,\n"
				+ "  `m4` VARCHAR(45) NULL,\n" + "  `m5` VARCHAR(45) NULL,\n" + "  `m6` VARCHAR(45) NULL,\n"
				+ "  `m7` VARCHAR(45) NULL,\n" + "  `m8` VARCHAR(45) NULL,\n" + "  `m9` VARCHAR(45) NULL,\n"
				+ "  `m10` VARCHAR(45) NULL,\n" + "  `m11` VARCHAR(45) NULL,\n" + "  `m12` VARCHAR(45) NULL,\n"
				+ "  `total` VARCHAR(45) NULL,\n" + "  `avg` VARCHAR(45) NULL,\n" + "  `avg_p` VARCHAR(45) NULL,\n"
				+ "  `sub` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  `symbol1` VARCHAR(45) NULL, `kovets` VARCHAR(45) NULL,\n"
				+ "  PRIMARY KEY (`in_id`)"
				+ ") COLLATE=utf8_unicode_ci;";

		tr.Insertintodb1(a);

		truncate(name_schema, name_table);

	}

	public void truncate(String name_schema, String name_table) throws SQLException {
		System.out.println("truncate " + name_table);
		String a = "    TRUNCATE `" + name_schema + "`.`" + name_table + "`";
		tr.Insertintodb1(a);
	}

	public void load_data_emon(String name_schema, String name_table, String charset, String endLine, int year, String pathFile, int cid) throws SQLException {

		System.out.println("load file in year : " + year);

		// /home/shoshana/Downloads/emon/moked_emon/sacar
		// String b="LOAD DATA LOCAL INFILE "
		// + " '/home/shoshana/Downloads/emon/moked_emon/sacar/"+year+".csv'"+
		// "INTO TABLE `Upload_file`."+name_tabel+" CHARACTER SET hebrew FIELDS
		// TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1
		// LINES\n" +
		// " (mis_oved, tat_mifal, agaf, machlaka, name_machlaka, derog,
		// tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok,
		// tear_isok, symbol, symbol_name,\n" +
		// " m1, m2, m3, m4, m5, m6, m7, m8, m9, \n" +
		// " m10, m11, m12, total, avg, avg_p, sub,dyear) set dyear="+year;
		// tr.Insertintodb1(b);

		// String b="LOAD DATA LOCAL INFILE "
		// + " '/home/user1/hevra/otsma/amal/"+year+".csv'"+
		// "INTO TABLE " + name_schema + "."+name_tabel+" CHARSET hebrew FIELDS
		// TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES\n" +
		// " (mis_oved,f_name,l_name, tat_mifal, agaf, machlaka, name_machlaka,
		// derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok,
		// tear_isok, symbol, symbol_name,\n" +
		// " m1, m2, m3, m4, m5, m6, m7, m8, m9, \n" +
		// " m10, m11, m12, total, avg, avg_p, sub,dyear) set dyear="+year;
		// tr.Insertintodb1(b);

		File f = new File(pathFile + "/"+ year);

		File[] listefichier = f.listFiles();

		int i = 0;

		for (int a = 0; a < listefichier.length; a++){
			if (listefichier[a].getAbsolutePath().endsWith(".csv")) {
				i++;
			}
		} 

		System.out.println(i);

		for(int j = 1; j <= i; j++){


			System.out.println(j + ".csv");
			String b = "LOAD DATA LOCAL INFILE '" + pathFile + "/" + year + "/" + j + ".csv' " + 
					"INTO TABLE "
					+ name_schema + "." + name_table + " " + charset
					+ "   FIELDS TERMINATED BY ','enclosed by '\"'LINES TERMINATED BY '" + endLine + "' "
					+ "IGNORE 2 LINES\n"
					+ " (num_worker,"
					+ "f_name,l_name,  tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, "
					+ "symbol, symbol_name,\n"
					+ " m1"
					+ ", m2, m3, m4, m5, m6, m7, m8, m9, \n" 
					+ " m10, m11, m12, total, avg, avg_p, sub) set dyear=" + year +", kovets = " + j +" ;";

			tr.Insertintodb1(b);
		}

		// String b="LOAD DATA LOCAL INFILE "
		// + " '/home/user1/hevra/otsma/amal/"+year+".csv'"+
		// "INTO TABLE " + name_schema + "." + name_tabel+" FIELDS TERMINATED BY
		// ',' LINES TERMINATED BY '\n' IGNORE 2 LINES\n" +
		// " (mis_oved, tat_mifal, agaf, machlaka, name_machlaka, derog,
		// tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok,
		// tear_isok, symbol, symbol_name,\n" +
		// " m1, m2, m3, m4, m5, m6, m7, m8, m9, \n" +
		// " m10, m11, m12, total, avg, avg_p, sub,dyear) set dyear=2016";
		// tr.Insertintodb1(b);

		System.out.println("end load  year : " + year + ", files : " + i + "\n\n");
	}

	public void Malam_replace_comma(String name_schema, String name_table) throws SQLException {

		System.out.println("replace");

		String a = "update " + name_schema + "." + name_table + "\n" + "set m1=replace(m1,',',''),\n"
				+ "m2=replace(m2,',',''),\n" + "m3=replace(m3,',',''),\n" + "m3=replace(m3,',',''),\n"
				+ "m4=replace(m4,',',''),\n" + "m5=replace(m5,',',''),\n" + "m6=replace(m6,',',''),\n"
				+ "m7=replace(m7,',',''),\n" + "m8=replace(m8,',',''),\n" + "m9=replace(m9,',',''),\n"
				+ "m10=replace(m10,',',''),\n" + "m11=replace(m11,',',''),\n" + "m12 =replace(m12,',','');";
		tr.Insertintodb1(a);

	}

	public void create_101(String name_schema, String name_table, int cid) throws SQLException {

		String c = "CREATE TABLE if not exists  " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
				+ "  `cid` int(10) unsigned NOT NULL DEFAULT '"+cid+"',\n"
				+ "  `dyear` smallint(6) NOT NULL,\n"
				+ "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n"
				+ "  `FullName` varchar(51) NOT NULL,\n"
				+ "  `Symbol` varchar(100) NOT NULL DEFAULT '0' ,\n"
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
				+ "  `Symbol1` varchar(100) NOT NULL DEFAULT '0' ,\n"
				+ "  PRIMARY KEY (`in_id`),\n"
				+ "  UNIQUE KEY `ind_unique` (`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,`division`) USING BTREE,\n"
				+ "  KEY `indi3` (`cid`,`dyear`,`Symbol`,`SymbolName`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ROW_FORMAT=DYNAMIC ";
		tr.Insertintodb1(c);
		truncate(name_schema, name_table);

	}

	public Statement returnStatement(){
		return o.getTr().getConnectionMySql().getStmt();
	}

	void convert_to_101_9(String name_schema, String name, String name_table) throws SQLException {

		System.out.println("convert to 101");

		String load8 = "insert into " + name_schema + "." + name + "\n"
				+ "            ( `FullName`,`cid`,`dyear`,`id`,Symbol,`SymbolName`,"
				+ "m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, "
				+ "type, num_worker, original_id, source) \n"
				+ "            \n"
				+ "\n"
				+ "\n"
				+ "SELECT concat(f_name,'  ',l_name) , tat_mifal,`dyear`,`id`,`symbol`, CONCAT(`symbol_name`, IF(sub = '', '', ' : '),  IFNULL(sub,'')) AS symbolname ,"
				+ "sum(m1),sum(m2),sum(m3),sum(m4),sum(m5),sum(m6),sum(m7),sum(m8),sum(m9),sum(m10),sum(m11), sum(m12)," 
				+ " IFNULL(sub,''), num_worker, tat_mifal, 'original_otsma' "
				+ "\n"
				+ "\n"
				+ "   FROM " + name_schema + "." + name_table 
				+ "  group by dyear,id,`symbol`, symbolname;";

		// String load8 = "insert into " + name_schema + "." + name + "\n"
		// + "
		// (`FullName`,`cid`,`dyear`,`id`,Symbol,`SymbolName`,m"+i+",type,permission)
		// \n"
		// + " SELECT concat(f_name,' ',l_name) , "+
		// cid+",`dyear`,`id`,`symbol`,
		// concat(`symbol_name`,':',sub),sum(m"+i+"),sub,tat_mifal\n"
		// + " FROM Upload_file."+name_table+" group by dyear,id,`symbol`,
		// concat(`symbol_name`,':',sub) "
		// + " on duplicate key update m"+i+"=values(m"+i+");";
		//

		//
		// String load8="insert into
		// Upload_file.tbl_moked_emon_shmira_101_af\n" +
		// " (`FullName`,`cid`,`dyear`,`id`,Symbol,`SymbolName`,m1,type)\n"
		// +
		// " SELECT 'a' , 924250194,`dyear`,`id`,`symbol`,
		// `symbol_name`,sum(m1),sub\n" +
		// " FROM Upload_file.tbl_moked_emon_shmira group by
		// dyear,id,`symbol`,\n" +
		// "symbol_name` ,sub on duplicate key update m1=values(m1);"

		System.out.println(load8);

		tr.Insertintodb1(load8);
	}

	public void update_total(String name_schema, String name_table) throws SQLException {

		System.out.println("update total");

		String a = "update " + name_schema + "." + name_table + " set total=m1+m2+m3+m4+m5+m6+m7+m8+m9+m10+m11+m12";
		tr.Insertintodb1(a);

	}

	void update_symbol(String name_schema, String name_tabel) throws SQLException {

		System.out.println("update symbol ");
		String a = "  UPDATE " + name_schema + "." + name_tabel + "\n"
				+ "SET SymbolName = substring(SymbolName,1,position(':' in SymbolName)-1 )\n"
				+ "where  type not like '%כמויות%' ;";
		//		System.out.println(a);
		//		tr.Insertintodb1(a);


		String b = "  UPDATE " + name_schema + "." + name_tabel + "\n"
				+ "SET Symbol = 11111\n"
				+ "where  type regexp '%כמויות%' ;";

		//		tr.Insertintodb1(b);
	}

	public void modify_symbol_and_hagdaroth(String name_schema, String name_table) throws SQLException  {
		List<String> b = new ArrayList<String>();
		String a = "CREATE TABLE if not exists " + name_schema + ".`symbol_otzma2` ( "
				+ " `in_id` int(11) NOT NULL AUTO_INCREMENT, "
				+ "`symbol` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL, "
				+ "`symbolName` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL, "
				+ "`new_number` varchar(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL, "
				+ " `new_symbolName` varchar(100) DEFAULT NULL, "
				+ "`topic_otzma` varchar(100) NOT NULL, "
				+ "`special_type` varchar(100) NOT NULL, "
				+ "`template` varchar(100) NOT NULL, "
				+ "PRIMARY KEY (`in_id`), "
				+ "UNIQUE KEY `unique_symbol_symbolname` (`symbol`,`symbolName`,`topic_otzma`) "
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
		tr.Insertintodb(a);




		a = "\ninsert ignore into  " + name_schema + ".symbol_otzma2( symbol, symbolName,topic_otzma)\n"
				+ "select Symbol,`SymbolName`,type from "+name_schema+"." + name_table + " group by Symbol,SymbolName;";
		tr.Insertintodb(a);

		a = " update " + name_schema + ".symbol_otzma2\n"
				+ " set new_number= symbol  \n"
				+ "where symbol regexp '^[0-9]*$' ;";

		tr.Insertintodb(a);

		a = " update  " + name_schema + ".symbol_otzma2  set new_number = '9999'  where symbol = '' ;";

		tr.Insertintodb(a);



		a = " update " + name_schema + ".symbol_otzma2 "
				+ "set new_number=in_id*10,"
				+ " new_symbolName=concat(symbolName,' ',symbol)\n"
				+ "where symbol regexp '^[0-9]*$' is false and symbol<>' ';\n";
		tr.Insertintodb(a);   

		a = "\nupdate\n " + name_schema + ".symbol_otzma2\n"
				+ "set new_symbolName=symbolName\n"
				+ "where new_symbolName is null  ;\n";
		tr.Insertintodb(a);



		a="\nupdate " + name_schema + ".symbol_otzma2 \n" +
				"set special_type=';nikuy_chova;' " +
				"where topic_otzma REGEXP 'ניכוי חובה' ;";

		tr.Insertintodb(a);  

		a="\nupdate " + name_schema + ".symbol_otzma2 \n" +
				"set special_type=';tlush_reshut;' " +
				"where topic_otzma = ' ניכוי אישי';";
		tr.Insertintodb(a); 


		a="\nupdate " + name_schema + ".symbol_otzma2 \n" +
				"set special_type=';tlush_tashlum;tlush_shovy;b_tlush;' " +
				"where topic_otzma regexp 'שווי כסף';";
		tr.Insertintodb(a);


		a="\nupdate " + name_schema + ".symbol_otzma2 \n" +
				"set special_type = ';tlush_tashlum;' " +
				"where topic_otzma regexp ' ת.אישיים';";
		tr.Insertintodb(a) ;



		a="\nupdate " + name_schema + ".symbol_otzma2 \n" +
				"set special_type=';tlush_tashlum;' " +
				"where topic_otzma regexp 'מרכיבי שכר';";

		tr.Insertintodb(a); 


		a="\nupdate " + name_schema + ".symbol_otzma2 \n" +
				"set special_type=';b_tlush;' " +
				"where    new_SymbolName regexp '^סהכ תשלומים';";
		tr.Insertintodb(a);

		a="\nupdate " + name_schema + ".symbol_otzma2 \n" +
				"set special_type=';tlush_neto;' " +
				"where  new_SymbolName regexp '^נטו לתשלום';";
		tr.Insertintodb(a);

		a="\nupdate " + name_schema + ".symbol_otzma2 \n" +
				"set special_type=';nikuy_chova;semelbb;'\n" +
				"where new_SymbolName REGEXP 'B003' AND new_SymbolName REGEXP 'חובה' ;";
		tr.Insertintodb(a);

		a="\nupdate " + name_schema + ".symbol_otzma2 \n" +
				"set special_type=';nikuy_chova;semelbilo;'\n" +
				"where new_SymbolName REGEXP 'B002' AND new_SymbolName REGEXP 'חובה' ;";
		tr.Insertintodb(a);

		a="\nupdate " + name_schema + ".symbol_otzma2 \n" +
				"set special_type=';nikuy_chova;semelMh;'\n" +
				"where   new_SymbolName REGEXP 'M044' ;";
		tr.Insertintodb(a);

		a="\nupdate " + name_schema + ".symbol_otzma2 \n" +
				"set special_type=';semelbilm;'\n" +
				"where new_SymbolName REGEXP 'MBLMI';";
		tr.Insertintodb(a);

		a = "update " + name_schema + "." + name_table + " set type_for_gemel = 'shulam_oved;pensya;sum_shulam' where Symbol regexp '^K' and type = ' ניכוי חובה'; ";
		tr.Insertintodb(a); 

		a = "update " + name_schema + "." + name_table + " set type_for_gemel = 'shulam_maavid;pensya;sum_shulam' where Symbol regexp '^MK' ; ";
		tr.Insertintodb(a); 



		a = "\n"
				+ "update " + name_schema + "." + name_table + " as a "
				+ "join " + name_schema + ".symbol_otzma2 as b  "
				+ "on a.symbolName=b.symbolName  and a.symbol=b.symbol \n"
				+ "set a.symbol=b.new_number, a.symbolName=b.new_symbolName, a.type=b.special_type"
				+ ";";
		tr.Insertintodb(a);
	}



	public   String Create_Table_Alphone(String name_schema, String name_table) throws SQLException { 

		System.out.println(" CREATE TABLE if not exists `"+name_schema+"`.`"+name_table);

		String a = " CREATE TABLE if not exists `"+name_schema+"`.`"+name_table+"` ( \n"
				+ "`in_id` int(11) NOT NULL AUTO_INCREMENT,\n"
				+ "`empty` int(11) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`mis_oved` int(11) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `l_name` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `f_name` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `tz` varchar(60) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `sug_tz` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `tz_nosaf` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `birthday` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `age` int(11) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `is_male` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `start_service_date` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`vetek` double COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`kviut_date` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`kovea_date` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `tat_mifal` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`teur_tat_mifal` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `mahlaka` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `mahlaka_name` varchar(60) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `agaf` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`agaf_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `tafkid` int(10) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `teur_tafkid` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `mikum` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `teur_mikum` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`marital` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `partner_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `partner_tz` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `partner_birthday` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `partner_workplace` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `partner_known` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `street` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `street_num` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `apart_num` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `neighborhood` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`yeshuv` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `yeshuv_num` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `mikud` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `td` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `td_mikum` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`mail` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `phone` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`cellphone` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`phone_work` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `cellphone_work` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `shluca` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `bank` int(11) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `branch` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `account_number` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `status` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`teur_status` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `from_date` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`til_date` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`reason_quit` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `kod_quit` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`teur_quit` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`finished_service_date` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`derug` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`teur_derug` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`darga` int(3) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `teur_darga` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`darga_date` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `heskem` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `teur_heskem` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `percentage` double COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mone` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `mehane` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `kod_bil` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `kod_irgun` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `teur` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `mas_ergun` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `kupat_h` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `mas_ergun_teur` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `status_oved` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `tashlum_markivim` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `hatnayat_status` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `misra` tinyint(1) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " `teur_misra` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`in_tlush` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`tbl_hofesh` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`meafyen` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`ezrahut` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`leom` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`birth_land` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`alia_land` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`date_alia` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`english_fname` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`english_lname` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "`dyear` int(6) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ " PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci; \n";
		tr.Insertintodb(a);

		tr.Insertintodb("truncate table `"+name_schema+"`.`"+name_table+"`");
		return a;
	}






	public   String load_data_Alphone(String name_schema, String name_table, int year, String host) throws SQLException {

		System.out.println("'" + host +"/" + year + "/alphon.csv' ");

		String b = "LOAD DATA LOCAL INFILE "
				+ "'" + host +"/" + year + "/alphon.csv' "
				+ "INTO TABLE `"+name_schema+"`.`"+name_table+"` character set utf8 FIELDS TERMINATED BY ',' ENCLOSED BY '\\\"'  LINES TERMINATED BY '\\n' IGNORE 2 LINES\n" +
				"(mis_oved, l_name, f_name, tz, sug_tz, tz_nosaf, birthday, age, is_male, start_service_date, vetek, kviut_date, kovea_date, tat_mifal, \n" +
				"teur_tat_mifal, mahlaka, mahlaka_name, agaf, agaf_name, tafkid, teur_tafkid, mikum, teur_mikum, marital, partner_name, partner_tz, \n" +
				"partner_birthday, partner_workplace, partner_known, street, street_num, apart_num, neighborhood, yeshuv, yeshuv_num, mikud, td, td_mikum, mail, phone, \n" +
				"cellphone, phone_work, cellphone_work, shluca, bank, branch, account_number, status, teur_status, from_date, til_date, reason_quit, kod_quit, teur_quit, \n" +
				"finished_service_date, derug, teur_derug, darga, teur_darga, darga_date, heskem, teur_heskem, percentage, mone, mehane, kod_bil, kod_irgun, teur, mas_ergun, \n" +
				"kupat_h, mas_ergun_teur, status_oved, tashlum_markivim, hatnayat_status, misra, teur_misra, in_tlush, tbl_hofesh, meafyen, ezrahut, leom, birth_land, alia_land, date_alia, english_fname, english_lname, dyear)set dyear="+ year+"; \n";

		tr.Insertintodb(b);
		return b;
	}




	public  String Create_Table_Alphone_101(String name_schema, String name_table) throws SQLException{ 

		System.out.println(" CREATE TABLE `"+name_schema+"`.`"+name_table+"`");

		String a = " CREATE TABLE if not exists `"+name_schema+"`.`"+name_table+"` ( \n"
				+ "`in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `cid` int(10) NOT NULL,\n" +
				"  `dyear` int(6) NOT NULL,\n" +
				"  `id` int(11) NOT NULL,\n" +
				"  `num_worker` int(11) NOT NULL,\n" +
				"  `first_name` varchar(50) DEFAULT NULL,\n" +
				"  `last_name` varchar(50) DEFAULT NULL,\n" +
				"  `month` int(6) NOT NULL DEFAULT '0',\n" +
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
				"  `intra_division` varchar(45) NOT NULL DEFAULT '0',\n" +
				"  `intra_division_exp` varchar(45) NOT NULL DEFAULT '0',\n" +
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
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='InnoDB free: 1508352 kB; InnoDB free: 2484224 kB; InnoDB fre';\n";

		tr.Insertintodb(a);


		tr.Insertintodb("truncate table `"+name_schema+"`.`"+name_table+"`");
		return a;
	}  
	public  String convert_Alphone_to_101_details(String name_schema, String name_table_insert, String name_table, int cid) throws SQLException {

		System.out.println("INSERT ignore INTO " + name_schema + "." + name_table_insert);

		String load8 = "INSERT ignore INTO " + name_schema + "." + name_table_insert + 
				"(cid, dyear, id,num_worker, first_name, last_name, aliya_date, marital_status, birthday, age_in_months, another_work, start_service_date, finished_service_date, is_male, \n" +
				"city, street, street_num, zip_code, phone_munber, cell_phone, bank, branch, account_number, spouse_id, spouse_Lname, spouse_Fname, \n" +
				"spouse_dob, degree_date, degree_kod, job_precent, vetek, tafkid)\n"
				+ " SELECT " + cid + ", dyear,tz, mis_oved, f_name, l_name, date_alia, marital , birthday, age*12, misra, start_service_date, finished_service_date,\n" +
				" IF(is_male regexp 'זכר'  , 1, 0 ) as is_male ,\n" +
				"yeshuv, street, street_num, mikud, phone, cellphone, bank, branch, account_number, partner_tz, \n" +
				"substr(partner_name,1, instr(partner_name,' ')) as spouse_Lname, substr(partner_name, instr(partner_name, ' ')+1, length(partner_name))  as spouse_Fname,\n" +
				"partner_workplace, darga_date, darga, percentage, vetek, tafkid\n"
				+ "from " + name_schema + "." + name_table + ";";

		tr.Insertintodb(load8);

		return load8;
	}


}
