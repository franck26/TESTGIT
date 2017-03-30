package otsma;

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

	public void create_table(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
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
				+ "  `sub` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  `symbol1` VARCHAR(45) NULL,\n"
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

	public void load_data_emon(String name_schema, String name_table, String charset, String endLine, int year) throws SQLException {

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

		String b = "LOAD DATA LOCAL INFILE " + " '/home/user1/hevra/otsma/mikhlelet/" + year + ".csv' " + 
                        "INTO TABLE "
				+ name_schema + "." + name_table + " " + charset
				+ "   FIELDS TERMINATED BY ','  LINES TERMINATED BY '" + endLine + "' IGNORE 1 LINES\n"
				+ " (mis_oved,f_name,l_name,  tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n"
				+ " m2"
				+ ", m1, m3, m4, m5, m6, m7, m8, m9, \n" 
                                + " m10, m11, m12, total, avg, avg_p, sub) set dyear="
				+ year;
		tr.Insertintodb1(b);

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

		System.out.println("end load  year : " + year + "\n\n");
	}

	public void Malam_replace_comma(String name_schema, String name_table) throws SQLException {

		String a = "update " + name_schema + "." + name_table + "\n" + "set m1=replace(m1,',',''),\n"
				+ "m2=replace(m2,',',''),\n" + "m3=replace(m3,',',''),\n" + "m3=replace(m3,',',''),\n"
				+ "m4=replace(m4,',',''),\n" + "m5=replace(m5,',',''),\n" + "m6=replace(m6,',',''),\n"
				+ "m7=replace(m7,',',''),\n" + "m8=replace(m8,',',''),\n" + "m9=replace(m9,',',''),\n"
				+ "m10=replace(m10,',',''),\n" + "m11=replace(m11,',',''),\n" + "m12 =replace(m12,',','');";
		tr.Insertintodb1(a);

	}

	public void create_101(String name_schema, String name_table) throws SQLException {

		String c = "CREATE TABLE if not exists  " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
				+ "  `cid` int(10) unsigned NOT NULL,\n"
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
				+ "type, num_worker, original_id) \n"
				+ "            "
				+ ""
				+ ""
				+ "SELECT concat(f_name,'  ',l_name) , tat_mifal,`dyear`,`mis_oved`,`symbol`, concat(`symbol_name`,' : ',sub),"
				+ "sum(m1),sum(m2),sum(m3),sum(m4),sum(m5),sum(m6),sum(m7),sum(m8),sum(m9),sum(m10),sum(m11), sum(m12)," 
				+ "sub, id, tat_mifal "
				+ ""
				+ ""
				+ "   FROM " + name_schema + "." + name_table 
				+ "  group by dyear,mis_oved,`symbol`, concat(`symbol_name`,' : ',sub);";

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
                System.out.println(a);
		tr.Insertintodb1(a);


		String b = "  UPDATE " + name_schema + "." + name_tabel + "\n"
				+ "SET Symbol = 11111\n"
				+ "where  type like '%כמויות%' ;";

		tr.Insertintodb1(b);
	}

	public static List<String> create_symbol(String name_schema, String name_table)  {
		List<String> b = new ArrayList<String>();
		String a = "\ninsert ignore into  franck.symbol_otzma2( symbol, symbolName,topic_otzma)\n"
				+ "select Symbol,`SymbolName`,type from "+name_schema+"." + name_table + " group by Symbol,SymbolName;";
		b.add(a);

		a = "\nupdate \n"
				+ "franck.symbol_otzma2\n"
				+ "set new_number='9999'\n"
				+ "where symbol = ' ' ;";

		b.add(a);

		a = "\nupdate\nfranck.symbol_otzma2\n"
				+ " set new_number= symbol  \n"
				+ "where symbol regexp '^[0-9]*$' ;";

		b.add(a);
		a = "\nupdate\nfranck.symbol_otzma2\n"
				+ "set new_number=in_id*10,"
				+ " new_symbolName=concat(symbolName,' ',symbol)\n"
				+ "where symbol regexp '^[0-9]*$' is false and symbol<>' ';\n";
		b.add(a);   

		a = "\nupdate\nfranck.symbol_otzma2\n"
				+ "set new_symbolName=symbolName\n"
				+ "where new_symbolName is null  ;\n";
		b.add(a);



		a="\nupdate franck.symbol_otzma2 \n" +
				"set special_type=';nikuy_chova;',template='הגדרת ניכוי חובה'\n" +
				"where topic_otzma= 'ניכוי חובה' and new_number <> 1130 AND  new_number <> 1140 AND new_number <> 1760;";

		b.add(a);  
		
		a="\nupdate franck.symbol_otzma2 \n" +
				"set special_type=';tlush_reshut;',template='הגדרת ניכוי רשות'\n" +
				"where topic_otzma=' ניכוי אישי';";
		b.add(a); 
		
		
		a="\nupdate franck.symbol_otzma2 \n" +
				"set special_type=';tlush_tashlum;tlush_shovy;b_tlush;',template='תשלום רגיל חייב הכל;שווי - נכנס לחישוב ברוטו מס הכנסה ולא לנטו לבנק'\n" +
				"where topic_otzma='  שווי כסף';";
		b.add(a);


		a="\nupdate franck.symbol_otzma2 \n" +
				"set special_type=';tlush_tashlum;',template='תשלום רגיל חייב הכל'\n" +
				"where topic_otzma=' ת.אישיים';";
		b.add(a);

		a="\nupdate franck.symbol_otzma2 \n" +
				"set special_type=';b_tlush;',template='ברוטו_תלוש_הכולל_תשלומים_ושוויים'\n" +
				"where new_number = 9999 and new_SymbolName regexp '^סהכ תשלומים';";
		b.add(a);

		a="\nupdate franck.symbol_otzma2 \n" +
				"set special_type=';tlush_neto;',template='הגדרת נטו לבנק'\n" +
				"where new_number = 9999 and new_SymbolName regexp '^נטו לתשלום';";
		b.add(a);

		a="\nupdate franck.symbol_otzma2 \n" +
				"set special_type=';nikuy_chova;semelbb;'\n" +
				"where symbol = 'B003' and new_SymbolName = 'מס בריאות:ניכוי חובה B003';";
		b.add(a);

		a="\nupdate franck.symbol_otzma2 \n" +
				"set special_type=';nikuy_chova;semelbilo;'\n" +
				"where symbol = 'B002' and new_SymbolName = 'בטוח לאומי ניכוי:ניכוי חובה B002';";
		b.add(a);

		a="\nupdate franck.symbol_otzma2 \n" +
				"set special_type=';tlush_tashlum;',template='תשלום רגיל חייב הכל'\n" +
				"where topic_otzma='מרכיבי שכר';";

		b.add(a);  


		a = "\n"
				+ "update " + name_schema + "." + name_table + " as a "
				+ "join franck.symbol_otzma2 as b  "
				+ "on a.symbolName=b.symbolName  and a.symbol=b.symbol \n"
				+ "set a.symbol=b.new_number, a.symbolName=b.new_symbolName, a.type=b.special_type"
				+ ";";
		b.add(a);
		return b;
	}
}
