package shiklolit;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import mySQL.Trysql;

public class Shiklolit {

	private Trysql tr;

	private Shiklolit() {
		tr = Trysql.getInstance();
	}

	private static Shiklolit instance = null;

	public static  Shiklolit getInstance(){			
		if (instance == null)
		{ 	
			instance = new Shiklolit();	
		}
		return instance;
	}



	public void create_table_Shiklolit_tamchir(String name_schema,String name_table) throws SQLException {

		System.out.println("create " + name_table);

		String a = "CREATE TABLE if not exists `"+name_schema+"`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n"
				+ "  `mispar_oved` VARCHAR(200) NULL,\n"
				+ "  `L_name` VARCHAR(200) NULL,\n"
				+ "  `F_name` VARCHAR(200) NULL,\n"
				+ "  `Mascoret_Broto` VARCHAR(200) NULL,\n"

				+ "  `leumi` VARCHAR(200) NULL,\n"
				+ "  `briut` VARCHAR(200) NULL,\n"
				+ "  `ptour_leumi` VARCHAR(200) NULL,\n"
				+ "  `mas_hachnasa` VARCHAR(200) NULL,\n"
				+ "  `guemel` VARCHAR(200) NULL,\n"
				+ "  `irgun` VARCHAR(200) NULL,\n"
				+ "  `mikdama` VARCHAR(200) NULL,\n"
				+ "  `rechut` VARCHAR(200) NULL,\n"
				+ "  `neto` VARCHAR(200) NULL,\n"
				+ "  `sach_alot` VARCHAR(200) NULL,\n"
				+ "  `chaot` VARCHAR(200) NULL,\n"
				+ "  `yamim` VARCHAR(200) NULL,\n"
				+ "  `dyear` VARCHAR(200) NULL,\n"
				+ "  `mis_hodesh` VARCHAR(200) NULL,\n"
				+ "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(a);

		truncate(name_schema, name_table);

	}

	public void Shiklolit_load_tamhir_per_month(String pathFile,String name_schema, String name_table, int year1, int year2) throws SQLException {

		for (int year = year1; year <= year2; year++) {
			for (int month = 1; month <= 12; month++) {
				System.out.println("'" + pathFile + "/"+ year + "/tamhir/" + month + ".csv'");
				String a = "LOAD DATA  LOCAL INFILE "
						+ "'" + pathFile + "/"+ year + "/tamhir/" + month + ".csv'"
						+ "    INTO TABLE `"+name_schema+"`.`" + name_table + "` \n"
						+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n"
						+ "	LINES TERMINATED BY '\\n' \n"
						+ "     IGNORE 7 LINES\n"
						+ "(mispar_oved, L_name, F_name, Mascoret_Broto, leumi, briut, ptour_leumi, mas_hachnasa, guemel, rechut, irgun, mikdama, "
						+ "neto, "
						+ "sach_alot) "
						+ "     set dyear=" + year +" , mis_hodesh="+month;
				tr.Insertintodb1(a);
			}
		}
	}

//	private String recuperer_nom_colonnes(Trysql t, String name_schema, String name_table) throws SQLException{
//
//		String s = "";
//		ResultSet r = t.gettabledb("select * from " + name_schema + "." + name_table);
//		int i = 0;
//
//		ResultSetMetaData re = r.getMetaData();
//
//		for(; i <= re.getColumnCount(); i++)
//			System.out.print("\t" + re.getColumnName(i)+ "\t *");
//		s += re.getColumnName(i);
//
//		System.out.println(s);
//		return s;
//
//	}

	void create_101_shiklolit(String name_schema,String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists "+name_schema+".`" + name_table + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
				+ "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n"
				+ "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n"
				+ "  `FullName` varchar(200) NOT NULL,\n"
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

		truncate(name_schema, name_table);

	}

	void Shiklolit_insert_into_101_tamchir_per_month(String name_schema,String name_table,String name_table_101 ,int cid) throws SQLException {

		replace(name_schema, name_table);


		for (int i = 1; i <= 12; i++) {

			String a = " insert ignore into "+name_schema+"." + name_table_101  + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n "
					+ "  SELECT    concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '2000' as Symbol,'משכרת נטו '  as Mascoret_neto ,neto,'תמחיר עובדים'"
					+ " FROM "+name_schema+"."+name_table
					+ " where mis_hodesh=" + i + " \n  "
					+ "  and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(a);
			String b = " insert ignore into "+name_schema+"."+ name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type)"
					+ " \n "
					+ " SELECT    concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '3000' as Symbol,'משכרת ברוטו '  as Mascoret_Broto ,Mascoret_Broto,'תמחיר עובדים'"
					+ " FROM "+name_schema+"."+name_table 
					+ "  where mis_hodesh=" + i + " \n "
					+ "  and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(b);
			String c = "  insert ignore into "+name_schema+"." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
					+ "  SELECT    concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '4000' as Symbol,' ביטוח לאומי '  as Bituch_leomi , leumi, 'תמחיר עובדים'"
					+ " FROM "+name_schema+"."+name_table 
					+ "  where mis_hodesh=" + i + " \n "
					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(c);
			String f = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n     "
					+ "   SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '5000' as Symbol,'דמי בריאות' ,briut,'תמחיר עובדים'"
					+ " FROM "+name_schema+"."+name_table 
					+ " where mis_hodesh=" + i + " \n "
					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(f);
			String h = "insert ignore into "+name_schema+"."+ name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
					+ " SELECT  concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '6000' as Symbol,'גמל'  as khl ,guemel,'תמחיר עובדים'"
					+ " FROM "+name_schema+"."+name_table 
					+ " where mis_hodesh=" + i + " \n  "
					+ "    and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(h);

			String n = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '7000' as Symbol,'מס הכנסה ' ,mas_hachnasa,'תמחיר עובדים'"
					+ " FROM "+name_schema+"."+name_table 
					+ "  where mis_hodesh=" + i + " \n "
					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(n);
			String z = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '8000' as Symbol,'הורדות רשות', rechut,'תמחיר עובדים'"
					+ " FROM "+name_schema+"."+name_table 
					+ " where mis_hodesh=" + i + " \n  "
					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(z);

			String x = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '9000' as Symbol,'עלות מעביד'  , sach_alot,'תמחיר עובדים'"
					+ " FROM "+name_schema+"."+name_table 
					+ " where mis_hodesh=" + i + " \n  "
					+ "   and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(x);

			String d = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
					+ " SELECT  concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '10000' as Symbol,'מקדמה' , mikdama,'תמחיר עובדים'"
					+ " FROM "+name_schema+"."+name_table 
					+ "  where mis_hodesh=" + i + " \n"
					+ "   and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(d);
			//
			String m = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '11000' as Symbol,'מס אירגון' ,irgun,'תמחיר עובדים'"
					+ " FROM "+name_schema+"."+name_table 
					+ "  where mis_hodesh=" + i + " \n  "
					+ "     and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(m);
			//
			//			String k = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
			//					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '12000' as Symbol,'עתודה להבראה '  as atoda_lhavraha ,atoda_lhavraha,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table 
			//					+ "  where mis_hodesh=" + i + " \n  "
			//					+ "     and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(k);
			//
			//			String o = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
			//					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '13000' as Symbol,'סך עלות '  as sach_alot ,sach_alot,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table 
			//					+ "  where mis_hodesh=" + i + " \n "
			//					+ "   and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(o);
			//
			//			String o1 = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
			//					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '14000' as Symbol,'אחוז עלות  '  as sach_alot ,sach_alot,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table 
			//					+ "  where mis_hodesh=" + i + " \n  "
			//					+ "    and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(o1);

			//   in_id, mispar_oved, L_name, F_name, ID, Machlaka, Mascoret_neto, Mascoret_Broto, Bituch_leomi, Tagmolim_pitzum, khl, Mas_sachar, Hetal_oved_zar, Mas_masikim, atoda_lapitzuim, atoda_lachufsha, atoda_lhavraha, sach_alot, alot, dyear
		}

	}

	public void update_total(String name_schema, String name_table) throws SQLException {

		String a = "update " + name_schema + "." + name_table + " set total=m1+m2+m3+m4+m5+m6+m7+m8+m9+m10+m11+m12";
		tr.Insertintodb1(a);
		System.out.println("update " + name_table);
	}

	public void replace(String name_schema, String name_table) throws SQLException {

		String a = "update " + name_schema + "." + name_table + "\n"
				+ "set Mascoret_broto=replace(Mascoret_broto,',',''),\n"
				+ "leumi=replace(leumi,',',''),\n"
				+ "briut=replace(briut,',',''),\n"
				+ "ptour_leumi=replace(ptour_leumi,',',''),\n"
				+ "mas_hachnasa=replace(mas_hachnasa,',',''),\n"
				+ "guemel=replace(guemel,',',''),\n"
				+ "irgun=replace(irgun,',',''),\n"
				+ "mikdama=replace(mikdama,',',''),\n"
				+ "rechut=replace(rechut,',',''),\n"
				+ "neto=replace(neto,',',''),\n"
				+ "sach_alot=replace(sach_alot,',','')\n";
		tr.Insertintodb1(a);

	}

	//tashlumim
	public void create_table_Shiklolit_tashlumim(String name_schema,String name_table) throws SQLException, SQLException {

		String a = "CREATE TABLE if not exists `"+name_schema+"`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n"
				+ "  `name_tz` VARCHAR(200) NULL,\n"
				+ "  `m1` VARCHAR(200) NULL,\n"
				+ "  `m2` VARCHAR(200) NULL,\n"
				+ "  `m3` VARCHAR(200) NULL,\n"
				+ "  `m4` VARCHAR(200) NULL,\n"
				+ "  `m5` VARCHAR(200) NULL,\n"
				+ "  `m6` VARCHAR(200) NULL,\n"
				+ "  `m7` VARCHAR(200) NULL,\n"
				+ "  `m8` VARCHAR(200) NULL,\n"
				+ "  `m9` VARCHAR(200) NULL,\n"
				+ "  `m10` VARCHAR(200) NULL,\n"
				+ "  `m11` VARCHAR(200) NULL,\n"
				+ "  `m12` VARCHAR(200) NULL,\n"
				+ "  `total` VARCHAR(200) NULL,\n"
				+ "  `dyear` VARCHAR(200) NULL,\n"
				+ "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(a);

		truncate(name_schema, name_table);

	}

	public void Shiklolit_load_data_tashlumim(String pathFile, String name_schema,String name_table, String kidod, int year) throws SQLException {

		System.out.println("in load to year : " + year);
		
		String a = "LOAD DATA  LOCAL INFILE "
				+ "'" + pathFile + ".csv'"
				+ "    INTO TABLE "+name_schema+".`" + name_table + "` \n"
				//				+ kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n"
				+ "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE 8 LINES\n"
				+ "(`name_tz`, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total,dyear) "
				+ "     set dyear=" + year;
		tr.Insertintodb1(a);

	}

	void create_table_shiklolit_tashlumim_like_101(String name_schema, String name_table_101) throws SQLException {
		String a = "CREATE TABLE  if not exists "+ name_schema+".`" + name_table_101 + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
				+ "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n"
				+ "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n"
				+ "  `FullName` varchar(200) NOT NULL,\n"
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

                + "  PRIMARY KEY (`in_id`)\n"

                + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";
		tr.Insertintodb1(a);

		truncate(name_schema, name_table_101);

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
				+ "m12 =replace(m12,',','');";
		tr.Insertintodb1(a);

	}

	void shiklolit_get_the_right_rows(String name_schema, String name_table, String name_table_101, int year, int cid)
			throws SQLException {
		String s = null;
		HashMap<String, ArrayList<String>> we = new HashMap<String, ArrayList<String>>();
		HashMap<String, HashMap<String, ArrayList<String>>> al = new HashMap<String, HashMap<String, ArrayList<String>>>();
		ArrayList<String> al1 = new ArrayList<String>();
		ArrayList<String> al3 = new ArrayList<String>();
		ArrayList<String> list = new ArrayList<String>();
		HashMap<String, ArrayList<String>> al2;
		int index = 1;
		String a = "SELECT * FROM " + name_schema + "." + name_table;
		ResultSet rs = tr.gettabledb(a);
		al2 = new HashMap<String, ArrayList<String>>();
		while (rs.next()) {

			s = rs.getString("name_tz");
			if (s.contains("עובד")) {
				while (rs.next()) {
					if (!rs.getString("name_tz").contains("עובד")) {
						String Symbol = rs.getString("name_tz");
						if (list.contains(Symbol)) {
							Symbol = Symbol + " " + index;
							index++;
						}

						al1.add(rs.getString("m1"));
						al1.add(rs.getString("m2"));
						al1.add(rs.getString("m3"));
						al1.add(rs.getString("m4"));
						al1.add(rs.getString("m5"));
						al1.add(rs.getString("m6"));
						al1.add(rs.getString("m7"));
						al1.add(rs.getString("m8"));
						al1.add(rs.getString("m9"));
						al1.add(rs.getString("m10"));
						al1.add(rs.getString("m11"));
						al1.add(rs.getString("m12"));
						al1.add(rs.getString("total"));
						al1.add(rs.getString("dyear"));

						list.add(Symbol);
						// לסדר איפה לאפס
						al2.put(Symbol, al1);

						al1 = new ArrayList<String>();
					} else {

						al.put(s, al2);
						index = 1;
						al2 = new HashMap<String, ArrayList<String>>();
						list.clear();

						rs.previous();
						break;

					}

				}

			}

		}
		al.put(s, al2);

		String aa = null;
		String aaa = null;
		int get = 0;
		for (String key : al.keySet()) {
			we = al.get(key);

			for (String key1 : we.keySet()) {
				al3 = we.get(key1);
				if (key1.equals("") || key1.equals("חודש")) {

					continue;

				}

				aaa = "INSERT INTO `" + name_schema + "`.`" + name_table_101 + "`"
						+ "(cid,dyear,id,original_id,FullName,Symbol,SymbolName"
						+ ",m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,total,division,run_version,date_value,source,type,num_worker,permission,type_for_gemel)"
						+ "VALUES('" + cid + "'," + year + ",'0','0','" + escapeSQL(key) + "'," + "0" + ",'"
						+ escapeSQL(key1) + "','" + sqlNumericEscape_for_numbers(al3.get(0)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(1)) + "','" + sqlNumericEscape_for_numbers(al3.get(2))
						+ "','" + sqlNumericEscape_for_numbers(al3.get(3)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(4)) + "','" + sqlNumericEscape_for_numbers(al3.get(5))
						+ "','" + sqlNumericEscape_for_numbers(al3.get(6)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(7)) + "','" + sqlNumericEscape_for_numbers(al3.get(8))
						+ "','" + sqlNumericEscape_for_numbers(al3.get(9)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(10)) + "','" + sqlNumericEscape_for_numbers(al3.get(11))
						+ "','" + sqlNumericEscape_for_numbers(al3.get(12)) + "','0','0','0','0','0','0','0','0')";

				tr.Insertintodb1(aaa);

				String f = "select MAX(in_id )from " + name_schema + "." + name_table_101;
				int l = tr.Readfromdb(f);
				// String str1="'\"";
				// String aaaa="update Upload_file."+name_table_101+ "set
				// fullName=replace(fullName,\"שווי מס רכב -\",\"\") where
				// in_id="+l; // +l";
				// tr.Insertintodb1(aaaa);
				// String aaaaa="update Upload_file."+name_table_101+" set
				// FullName=replace(fullName,'- שווי רכב','') where in_id="+l;
				// String aaaaa="update
				// Upload_file.Tbl_Shiklolit_101_tashlumim_dor_group set
				// fullName=replace(fullName,\"שווי רכב -\",\"\") where
				// in_id="+l; // +l";
				// tr.Insertintodb1(aaaaa);
				String sss = "  update " + name_schema + "." + name_table_101 + " as t1,(select \n"
						+ "SUBSTRING_INDEX(SUBSTRING_INDEX(FullName,' - ',1),' ',-1) as num_worker,\n"
						+ "SUBSTRING_INDEX(SUBSTRING_INDEX(FullName,' - ',2),' ',-1)as id ,\n"
						+ "SUBSTRING_INDEX(SUBSTRING_INDEX(FullName,'ת.ז.',1),' - ',-1)as FullName,\n"
						+ "substr(FullName,locate('מחלקה ',FullName)+6) as division from  " + name_schema + "."
						+ name_table_101 + " where in_id=" + l + ")as t2"
						+ "  set  t1.num_worker=t2.num_worker,t1.FullName=t2.FullName,t1.id=t2.id,t1.division=t2.division where t1.in_id="
						+ l + ";";

				tr.Insertintodb1(sss);

			}
			// index=1;
			// list.clear();
		}

	}


	void truncate(String name_schema, String name_table) throws SQLException {
		String a="    TRUNCATE `"+name_schema+"`.`"+name_table+"`";
		tr.Insertintodb1(a);
		System.out.println("truncate " + name_table + "\n\n");
	}

	public void create_symbels_numbers_and_insert_into_main_table(String name_schema, String name_tabletemp, String name_table_101) throws SQLException {
		String a = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_tabletemp + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n"
				+ "  `symble` VARCHAR(200) NULL,\n"

                + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);
		String x = "TRUNCATE `" + name_schema + "`.`" + name_tabletemp + "`;";
		tr.Insertintodb1(x);
		String s = "ALTER TABLE `" + name_schema + "`.`" + name_tabletemp + "`  AUTO_INCREMENT = 2001";
		tr.Insertintodb1(s);

		String j = "insert into " + name_schema + "." + name_tabletemp + "(symble)  SELECT SymbolName FROM " + name_schema + "." + name_table_101 + " group by  SymbolName;";
		tr.Insertintodb1(j);

		String insert = "UPDATE  " + name_schema + "." + name_table_101 + "  SET Symbol=(\n"
				+ "   SELECT in_id FROM " + name_schema + "." + name_tabletemp + " WHERE  " + name_tabletemp + ".symble = " + name_table_101 + ".SymbolName  COLLATE utf8_general_ci);";
		tr.Insertintodb1(insert);



	}


	public static String escapeSQL(String s) {
		int length = s.length();
		int newLength = length;
		// first check for characters that might
		// be dangerous and calculate a length
		// of the string that has escapes.
		for (int i = 0; i < length; i++) {
			char c = s.charAt(i);
			switch (c) {
			case '\\':
			case '\"':
			case '\'':
			case '\0': {
				newLength += 1;
			}
			break;
			}
		}
		if (length == newLength) {
			// nothing to escape in the string
			return s;
		}
		StringBuffer sb = new StringBuffer(newLength);
		for (int i = 0; i < length; i++) {
			char c = s.charAt(i);
			switch (c) {
			case '\\': {
				sb.append("\\\\");
			}
			break;
			case '\"': {
				sb.append("\\\"");
			}
			break;
			case '\'': {
				sb.append("\\\'");
			}
			break;
			case '\0': {
				sb.append("\\0");
			}
			break;
			default: {
				sb.append(c);
			}
			}
		}
		return sb.toString();
	}

	String sqlNumericEscape_for_numbers(String val) {

		if (val == null) {
			return "0";
		}
		if (val.length() > 1 && val.substring(0, 1).equals("\"") && val.substring(val.length() - 1).equals("\"")) {
			val = val.substring(1, val.length() - 1);
		}
		if (val.equals(" ") || val.equals("") || val.isEmpty()) {
			return "0";
		}


		return val.replace(",", "");

	}



	public void insert_to_101_sofi(String name_schema, String table_tashlum, String table_tamhir, String table_101_sofi) throws SQLException {
		create_101_shiklolit(name_schema, table_101_sofi);
//
		String s = "";
//		s = "update " + name_schema + "." + table_tashlum + " set id = num_worker";
//		tr.Insertintodb(s);
		System.out.println("create 101 sofi !");
		s = "update " + name_schema + "." + table_tamhir + " as b inner join " + name_schema + "." + table_tashlum + " as a on b.id = a.num_worker set b.id = a.id";
		tr.Insertintodb1(s);
		
		s = "insert ignore into " + name_schema + "." + table_101_sofi
				+ "(in_id, cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) "
				+ "select * "
				+ "from " + name_schema + "." + table_tashlum;
		tr.Insertintodb(s);
		
		
		s = "insert ignore into " + name_schema + "." + table_101_sofi
				+ "(in_id, cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) "
				+ "select * "
				+ "from " + name_schema + "." + table_tamhir;

		tr.Insertintodb(s);
		System.out.println("finish 101 sofi !");
		
	}

}
