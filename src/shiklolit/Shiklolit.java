package shiklolit;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

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
				+ "  `mispar_oved` VARCHAR(200) NULL,\n"
				+ "  `L_name` VARCHAR(200) NULL,\n"
				+ "  `F_name` VARCHAR(200) NULL,\n"
				+ "  `t_z` VARCHAR(200) NULL,\n"
				+ "  `mahlaka` VARCHAR(200) NULL,\n"
				+ "  `neto` VARCHAR(200) NULL,\n"
				+ "  `bruto` VARCHAR(200) NULL,\n"
				+ "  `a` VARCHAR(200) NULL,\n"
				+ "  `leumi_maasik` VARCHAR(200) NULL,\n"
				+ "  `briut` VARCHAR(200) NULL,\n"
				+ "  `ptour_leumi` VARCHAR(200) NULL,\n"
				+ "  `mas_hachnasa` VARCHAR(200) NULL,\n"
				+ "  `guemel` VARCHAR(200) NULL,\n"
				+ "  `irgun` VARCHAR(200) NULL,\n"
				+ "  `mikdama` VARCHAR(200) NULL,\n"
				+ "  `rechut` VARCHAR(200) NULL,\n"
				+ "  `sach_alot` VARCHAR(200) NULL,\n"
				+ "  `chaot` VARCHAR(200) NULL,\n"
				+ "  `yamim` VARCHAR(200) NULL,\n"
				+ "  `dyear` VARCHAR(200) NULL,\n"
				+ "  `mis_hodesh` VARCHAR(200) NULL\n"
				+ " );";
		tr.Insertintodb1(a);

		truncate(name_schema, name_table);

	}

	public void Shiklolit_load_tamhir_per_month(String pathFile,String name_schema, String name_table, int year1, int year2) throws SQLException {

		for (int year = year1; year <= year2; year++) {
			for (int month = 1; month <= 12; month++) {


				if(!(year == 2015 && month == 11) || !(year == 2017 && month >=5)){
					String a = "LOAD DATA  LOCAL INFILE "
							+ "'" + pathFile + "/"+ year + "/tamhir/" + month + ".csv'"
							+ "    INTO TABLE `"+name_schema+"`.`" + name_table + "` \n"
							+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n"
							+ "	LINES TERMINATED BY '\\n' \n"
							+ "     IGNORE 8 LINES\n"
							+ "(mispar_oved, L_name, F_name, t_z, mahlaka, neto, bruto, a, leumi_maasik) "
							+ "     set dyear=" + year +" , mis_hodesh="+month;

					System.out.println("'" + pathFile + "/"+ year + "/tamhir/" + month + ".csv'");

					tr.Insertintodb1(a);
				}

			}
		}
	}

	void create_101_shiklolit(String name_schema,String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists "+name_schema+"." + name_table + " (\n"
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

		System.out.println("insert tarmhir to 101 .....");
		for (int i = 1; i <= 12; i++) {

			//			String a = " insert ignore into "+name_schema+"." + name_table_101  + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n "
			//					+ "  SELECT    concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '2000' as Symbol,'משכרת נטו '  as Mascoret_neto ,neto,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table
			//					+ " where mis_hodesh=" + i + " \n  "
			//					+ "  and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(a);

			//			String b = " insert ignore into "+name_schema+"."+ name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type)"
			//					+ " \n "
			//					+ " SELECT    concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '3000' as Symbol,'משכרת ברוטו '  as Mascoret_Broto ,bruto,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table 
			//					+ "  where mis_hodesh=" + i + " \n "
			//					+ "  and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(b);

			String c = "  insert ignore into "+name_schema+"." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
					+ "  SELECT    concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `t_z`,  '4000' as Symbol,' ביטוח לאומי '  as Bituch_leomi , leumi_maasik, ';semelbilm;'"
					+ " FROM "+name_schema+"."+name_table 
					+ "  where mis_hodesh=" + i + " \n "
					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(c);

			//			String f = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n     "
			//					+ "   SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '5000' as Symbol,'דמי בריאות' ,briut,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table 
			//					+ " where mis_hodesh=" + i + " \n "
			//					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(f);

			//			String h = "insert ignore into "+name_schema+"."+ name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
			//					+ " SELECT  concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '6000' as Symbol,'גמל'  as khl ,guemel,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table 
			//					+ " where mis_hodesh=" + i + " \n  "
			//					+ "    and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(h);
			//
			//			String n = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
			//					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '7000' as Symbol,'מס הכנסה ' ,mas_hachnasa,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table 
			//					+ "  where mis_hodesh=" + i + " \n "
			//					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(n);

			//			String z = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
			//					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '8000' as Symbol,'הורדות רשות', rechut,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table 
			//					+ " where mis_hodesh=" + i + " \n  "
			//					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(z);
			//
			//			String x = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
			//					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '9000' as Symbol,'עלות מעביד'  , sach_alot,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table 
			//					+ " where mis_hodesh=" + i + " \n  "
			//					+ "   and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(x);
			//
			//			String d = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
			//					+ " SELECT  concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '10000' as Symbol,'מקדמה' , mikdama,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table 
			//					+ "  where mis_hodesh=" + i + " \n"
			//					+ "   and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(d);
			//			//
			//			String m = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n  "
			//					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid + " as cid,`dyear`, `mispar_oved`,  '11000' as Symbol,'מס אירגון' ,irgun,'תמחיר עובדים'"
			//					+ " FROM "+name_schema+"."+name_table 
			//					+ "  where mis_hodesh=" + i + " \n  "
			//					+ "     and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			//			tr.Insertintodb1(m);
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

			System.out.println("finish tarmhir to 101 hodesh " + i + ".....");

		}

	}

	public void update_total(String name_schema, String name_table) throws SQLException {

		String a = "update " + name_schema + "." + name_table + " set total=m1+m2+m3+m4+m5+m6+m7+m8+m9+m10+m11+m12";
		tr.Insertintodb1(a);
		System.out.println("update " + name_table);
	}

	public void replace(String name_schema, String name_table) throws SQLException {

		String a = "update " + name_schema + "." + name_table + "\n"
				+ "set bruto=replace(bruto,',',''),\n"
				+ "leumi_maasik=replace(leumi_maasik,',',''),\n"
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
	public void create_table_Shiklolit_tashlumim(String name_schema, String name_table)
			throws SQLException, SQLException {

		String a = "CREATE TABLE if not exists " + name_schema + "." + name_table + " (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `name_tz` VARCHAR(200) NULL,\n"
				+ "  `m1` VARCHAR(200) NULL,\n" + "  `m2` VARCHAR(200) NULL,\n" + "  `m3` VARCHAR(200) NULL,\n"
				+ "  `m4` VARCHAR(200) NULL,\n" + "  `m5` VARCHAR(200) NULL,\n" + "  `m6` VARCHAR(200) NULL,\n"
				+ "  `m7` VARCHAR(200) NULL,\n" + "  `m8` VARCHAR(200) NULL,\n" + "  `m9` VARCHAR(200) NULL,\n"
				+ "  `m10` VARCHAR(200) NULL,\n" + "  `m11` VARCHAR(200) NULL,\n" + "  `m12` VARCHAR(200) NULL,\n"
				+ "  `total` VARCHAR(200) NULL,\n" + "  `dyear` VARCHAR(200) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(a);

		truncate(name_schema, name_table);

	}



	public void Shiklolit_load_data_tashlumim(String pathFile, String name_schema,String name_table, String kidod, int year) throws SQLException {

		System.out.println("in load to year : " + year);

		String a = "LOAD DATA  LOCAL INFILE "
				+ "'" + pathFile + "/" + year + "/" + year +".csv'"
				+ "    INTO TABLE "+name_schema+".`" + name_table + "` \n"
				+ kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n"
				+ "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE 8 LINES\n"
				+ "(`name_tz`, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12) "
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


	public void shiklolit_get_the_right_rows_false(String name_schema, String name_table, String name_table_101, int cid) throws SQLException{

		String a = "SELECT * FROM " + name_schema + "." + name_table;
		ResultSet rs = tr.gettabledb(a);

		ArrayList<String> sheiltoth = new ArrayList<>();

		String s = "";

		int index = 1;

		int numLine = 0;

		rs.last();

		numLine = rs.getRow();

		rs.first();

		String name = "";
		String id = "";
		String numOved = "";



		do{
			s = rs.getString("name_tz");
			if(s.contains("מחלקה")){

				String[] split = s.split(" ");

				Vector<String> v = new Vector<>();
				for (int i = 0; i < split.length; i++) {
					v.add(split[i]);
				}

				//id
				id = v.get(v.indexOf("ת.ז.") + 1 );
				id.replace('\'', ' ');


				//name
				name = "";

				for (int i = v.indexOf("עובד") + 3; i < v.indexOf("ת.ז."); i++) {
					name += v.get(i) + " ";
				}

				//numOved
				numOved = v.get(v.indexOf("עובד") + 1);

				index++;

				rs.next();

			}else{
				if(!rs.getString("name_tz").contains("חודש") || !rs.getString("name_tz").contains("")){

					do{
						ArrayList<String> al1 = new ArrayList<>();
						al1.add(rs.getString("name_tz").replace('\'', ' '));
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
						al1.add(rs.getString("dyear"));

						String sheilta = "INSERT INTO " + name_schema + "." + name_table_101 +" (cid, dyear, fullname, id, num_worker, symbolname, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, symbol)"
								+ " VALUES "
								+ "(" + cid 
								+ ", "
								+ al1.get(13) + " , '"
								+ name.replace('\'', ' ') + "', '"
								+ id + "', "
								+ numOved + ", '"
								+ al1.get(0) + "', ";

						for (int i = 1; i < al1.size() - 2; i++) {
							sheilta += (al1.get(i).equals("")) ? "'0', " : al1.get(i) + ", ";
						}

						sheilta += (al1.get(12).equals("")) ? "'0', '0') ": al1.get(12) + ", '0' );\n\n";

						sheiltoth.add(sheilta);

						index++;
						rs.next();

					}while( index < numLine && !rs.getString("name_tz").contains("מחלקה"));
				}else{
					index++;
					rs.next();
				}

			}

			if(index == 8168){
				System.out.println();
			}

		}while(index < numLine);

		for (int i = 0; i < sheiltoth.size(); i++) {
			tr.Insertintodb(sheiltoth.get(i));
			System.out.println(i);
		}

		System.out.println("finish");

	}

	void shiklolit_get_the_right_rows1(String name_schema, String name_table, String name_table_101, int cid)
			throws SQLException {

		System.out.println("get the right rows ");

		String s = null;
		HashMap<String, ArrayList<String>> we = new HashMap<String, ArrayList<String>>();
		HashMap<String, HashMap<String, ArrayList<String>>> al = new HashMap<String, HashMap<String, ArrayList<String>>>();
		ArrayList<String> al1 = new ArrayList<String>();
		ArrayList<String> al3 = new ArrayList<String>();
		ArrayList<String> list = new ArrayList<String>();

		ArrayList<String> tashlumim = new ArrayList<String>();
		ArrayList<String> shovy = new ArrayList<String>();
		ArrayList<String> chova = new ArrayList<String>();
		ArrayList<String> reshut = new ArrayList<String>();

		HashMap<String, ArrayList<String>> al2;
		
		String a = "SELECT * FROM " + name_schema + "." + name_table;
		ResultSet rs = tr.gettabledb(a);

		int numLine = 0;
		int index = 1;
		rs.last();

		numLine = rs.getRow();

		rs.first();

//		System.out.println(numLine);

		al2 = new HashMap<String, ArrayList<String>>();
		while (rs.next()) {
			
			s = rs.getString("name_tz");
			if (s.contains("מחלקה")) {


				while (rs.next()) {
					if (!rs.getString("name_tz").contains("מחלקה")) {

						String symbolName = rs.getString("name_tz");

						if (list.contains(symbolName)) {
							symbolName = symbolName + " " + index;
							System.out.println(symbolName);
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
						al1.add(rs.getString("dyear"));

						list.add(symbolName);
						// לסדר איפה לאפס
						al2.put(symbolName, al1);

						al1 = new ArrayList<String>();
					} else {

						al.put(s, al2);
						index = 1;
						al2 = new HashMap<String, ArrayList<String>>();

						list.remove("חודש");
						list.remove("שכר נטו");


						//הגדרות 1 
						int sacoumBruto = list.indexOf("סה\"כ");

						int sacoumShovy = list.indexOf("סה\"כ הכ.זקופות");

						int sacoumNeto = list.indexOf("נטו לתשלום");

						for (int i = 0; i < sacoumBruto; i++) {
							if(!tashlumim.contains(list.get(i)))
								if(list.get(i).contains("שווי"))
									shovy.add(list.get(i).replace("\"", "").replace("'", ""));
								else
									tashlumim.add(list.get(i).replace("\"", "").replace("'", ""));
						}

						for (int i = sacoumBruto + 1; i < sacoumShovy; i++) {
							if(!chova.contains(list.get(i)))
								chova.add(list.get(i).replace("\"", "").replace("'", ""));
						}

						for (int i = sacoumShovy + 1; i < sacoumNeto; i++) {
							if(!reshut.contains(list.get(i)))
								reshut.add(list.get(i).replace("\"", "").replace("'", ""));
						}

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


		for (String key/*infoOved*/ : al.keySet()) {
			we/*hashmap*/ = al.get(key);

			String[] split = key.split(" ");

			Vector<String> v = new Vector<>();
			for (int i = 0; i < split.length; i++) {
				v.add(split[i]);
			}

			//id
			String id = v.get(v.indexOf("ת.ז.") + 1 );
			id.replace('\'', ' ');


			//name
			String name = "";

			for (int i = v.indexOf("עובד") + 3; i < v.indexOf("ת.ז."); i++) {
				name += v.get(i) + " ";
			}

			//numOved
			String numOved = v.get(v.indexOf("עובד") + 1);



			for (String key1/*valeurMois*/ : we.keySet()) {
				al3 = we.get(key1);
				if (key1.equals("") || key1.equals("חודש")) {

					continue;

				}


				//*/ INSERT TO 101 TASHLUMIM
				aaa = "INSERT INTO `" + name_schema + "`.`" + name_table_101 + "` "
						+ "(cid,dyear,id,FullName,Symbol,SymbolName"
						+ ",m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,num_worker) "
						+ " VALUES ('" 
						+ cid + "'," 
						+ sqlNumericEscape_for_numbers(al3.get(12)) + "," 
						+ id +  ",'" 
						+ name.replace('\'', ' ') + "'," 
						+ "0" + ",'"
						+ escapeSQL(key1) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(0)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(1)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(2)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(3)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(4)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(5)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(6)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(7)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(8)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(9)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(10)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(11)) + "',"
						+ numOved  + " );";

				tr.Insertintodb1(aaa);
				//*/

				/*/
				aaa = "INSERT INTO `" + name_schema + "`.`" + name_table_101 + "`"
						+ "(cid,dyear,id,FullName,Symbol,SymbolName"
						+ ",m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,total)"

						+ "VALUES('" 
						+ cid + "'," 
						+ sqlNumericEscape_for_numbers(al3.get(12))  + ","
						+ "'0','" 
						+ escapeSQL(key) + "'," 
						+ "0" + ",'"
						+ escapeSQL(key1) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(0)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(1)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(2)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(3)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(4)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(5)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(6)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(7)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(8)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(9)) + "','"
						+ sqlNumericEscape_for_numbers(al3.get(10)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(11)) + "','" 
						+ sqlNumericEscape_for_numbers(al3.get(12)) + "');";

				tr.Insertintodb1(aaa);

				//				String f = "select MAX(in_id )from " + name_schema + "." + name_table_101;
				//				int l = tr.Readfromdb(f);
				//				String sss = "  update " + name_schema + "." + name_table_101 + " as t1,(select \n"
				//						+ "SUBSTRING_INDEX(SUBSTRING_INDEX(FullName,' - ',1),' ',-1) as num_worker,\n"
				//						+ "SUBSTRING_INDEX(SUBSTRING_INDEX(FullName,' - ',2),' ',-1)as id ,\n"
				//						+ "SUBSTRING_INDEX(SUBSTRING_INDEX(FullName,'ת.ז.',1),' - ',-1)as FullName,\n"
				//						+ "substr(FullName,locate('מחלקה ',FullName)+6) as division from  " + name_schema + "."
				//						+ name_table_101 + " where in_id=" + l + ")as t2"
				//						+ "  set  t1.num_worker=t2.num_worker,t1.FullName=t2.FullName,t1.id=t2.id,t1.division=t2.division where t1.in_id="
				//						+ l + ";";
				//
				//				tr.Insertintodb1(sss);


				//				ResultSet rs1 = tr.getConnectionMySql().getStmt().executeQuery("SELECT fullname FROM " + name_schema + "." + name_table_101 + " where in_id = " + l + " ; ");
				//				rs1.next();
				//				
				//				String[] split2 = rs1.getString(1).split(" ");
				//
				//				Vector<String> v2 = new Vector<>();
				//				for (int i = 0; i < split2.length; i++) {
				//					v2.add(split[i]);
				//				}
				//
				//				//id
				//				id = v2.get(v2.indexOf("ת.ז.") + 1 );
				//				id.replace('\'', ' ');
				//
				//
				//				//name
				//				name = "";
				//
				//				for (int i = v2.indexOf("עובד") + 3; i < v2.indexOf("ת.ז."); i++) {
				//					name += v2.get(i) + " ";
				//				}
				//
				//				//numOved
				//				numOved = v2.get(v2.indexOf("עובד") + 1);

				String f = "UPDATE " + name_schema + "." + name_table_101 + " SET fullname = '" + name + "' , id = '" + id + "' , num_worker = '" + numOved + "' where fullname = '" + key + "' ; ";
				tr.Insertintodb1(f);
				//*/
			}
		}



		//*/ 2 הגדרות
		String update = "update " + name_schema + "." + name_table_101 + " set type = ';tlush_tashlum;' where symbolname IN ('";
		for (int i = 0; i < tashlumim.size() - 1; i++){
			update += tashlumim.get(i) + "', '";
		}

		update += tashlumim.get(tashlumim.size() - 1) + "');";


		tr.Insertintodb(update);


		update = "update " + name_schema + "." + name_table_101 + " set type = ';tlush_reshut;' where symbolname IN ('";
		for (int i = 0; i < reshut.size() - 1; i++){
			update += reshut.get(i) + "', '";
		}

		update += reshut.get(reshut.size() - 1) + "');";
		tr.Insertintodb(update);

		update = "update " + name_schema + "." + name_table_101 + " set type = ';tlush_tashlum;;tlush_shovy;' where symbolname IN ('";
		for (int i = 0; i < shovy.size() - 1; i++){
			update += shovy.get(i) + "', '";
		}

		update += shovy.get(shovy.size() - 1) + "');";
		tr.Insertintodb(update);



		update = "update " + name_schema + "." + name_table_101 + " set type = ';semelbilo;;nikuy_chova;' where symbolname = 'ביטוח לאומי' ; ";
		tr.Insertintodb(update);

		update = "update " + name_schema + "." + name_table_101 + " set type = ';semelbb;;nikuy_chova;' where symbolname = 'דמי בריאות' ; ";
		tr.Insertintodb(update);

		chova.remove("ביטוח לאומי"); chova.remove("דמי בריאות");

		update = "update " + name_schema + "." + name_table_101 + " set type = ';nikuy_chova;' where symbolname IN ('";
		for (int i = 0; i < chova.size() - 1; i++){
			update += chova.get(i) + "', '";
		}

		update += chova.get(chova.size() - 1) + "');";
		tr.Insertintodb(update);

		update = "update " + name_schema + "." + name_table_101 + " set type = ';tlush_neto;' where symbolname = 'נטו לתשלום' ; ";
		tr.Insertintodb(update);

		update = "update " + name_schema + "." + name_table_101 + " set type = ';b_tlush;' where symbolname = 'סה\"כ' ; ";
		tr.Insertintodb(update);
		//*/





		System.out.println("end tashlumim to 101.");


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
				+ "(cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) "
				+ "select cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value,  'original_shiklulit', type, num_worker, permission, type_for_gemel "
				+ "from " + name_schema + "." + table_tashlum;
		tr.Insertintodb(s);


		s = "insert ignore into " + name_schema + "." + table_101_sofi
				+ "(cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) "
				+ "select cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, 'original_shiklulit', type, num_worker, permission, type_for_gemel "
				+ "from " + name_schema + "." + table_tamhir;

		tr.Insertintodb(s);

		System.out.println("finish 101 sofi !");

	}



	public void loadDetails(String pathFile, String name_schema, String name_table_alfon, String nikod, int year, int cid) throws SQLException {
		
		System.out.println("insert alfon year : " + year);
		String s = "load data local infile '" + pathFile + "/" + year + "/alfon.csv'\n" +
				"\n" +
				"into table " + name_schema + "." + name_table_alfon +" \n" +
				"\n" +
				"FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\\n' IGNORE 7 LINES\n" +
				"\n" +
				"(id, t_z, l_name, f_name, mahlaka, leda, guil, min, marital_status, yeshuv, street, numStreet, mikud, td, telephone, bank, cid, dyear)"
				+ "SET cid = " + cid + ", dyear = " + year + " "
				+ ";";
		
		tr.Insertintodb(s);
		

	}



	public void createTableAlfon(String name_schema, String name_table_alfon) throws SQLException {
		String s = "CREATE TABLE if not exists " + name_schema + "." + name_table_alfon + " (\n" +
				"    `in_id` INT(11) NOT NULL AUTO_INCREMENT,\n" +
				"    `id` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `t_z` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `l_name` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `f_name` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `mahlaka` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `guil` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `min` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `marital_status` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `yeshuv` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `street` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    \n" +
				"    `numStreet` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `mikud` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `td` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `telephone` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    \n" +
				"    `bank` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `cid` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    `dyear` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"`leda` VARCHAR(200) COLLATE UTF8_UNICODE_CI DEFAULT NULL,\n" +
				"    PRIMARY KEY (`in_id`)\n" +
				")  ENGINE=INNODB AUTO_INCREMENT=327673 DEFAULT CHARSET=UTF8 COLLATE = UTF8_UNICODE_CI;";

		tr.Insertintodb(s);
		truncate(name_schema, name_table_alfon);

	}



	public void createTableAlfon101(String name_schema, String name_table_alfon_101) throws SQLException {
		String s = "CREATE TABLE if not exists " + name_schema + "." + name_table_alfon_101 + " (\n" +
				"  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `cid` int(10) NOT NULL,\n" +
				"  `dyear` int(6) NOT NULL,\n" +
				"  `id` int(11) NOT NULL,\n" +
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
				"  `intra_division` varchar(45) NOT NULL DEFAULT '-1',\n" +
				"  `intra_division_exp` varchar(45) NOT NULL DEFAULT '-1' ,\n" +
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
				") ENGINE=InnoDB AUTO_INCREMENT=10447270 DEFAULT CHARSET=utf8 COMMENT='InnoDB free: 1508352 kB; InnoDB free: 2484224 kB; InnoDB fre';";

		tr.Insertintodb(s);
		truncate(name_schema,
				name_table_alfon_101);

	}

	public void changeMin(String name_schema, String name_table_alfon) throws SQLException{
		String s = "UPDATE " + name_schema + "." + name_table_alfon + " SET min = replace(min , 'ז', '1'), min = replace(min, 'נ', '0')";
		
		
		tr.Insertintodb(s);
		
		s = "delete from " + name_schema + "." + name_table_alfon + " where t_z like '';";
		
		tr.Insertintodb(s);
	}


	void insert101Details(String name_schema, String name_table_alfon, String name_table_alfon_101) throws SQLException {

		System.out.println("insert into 101Details . . ." );
		
		String s = "INSERT INTO " + name_schema + "." + name_table_alfon_101 + "\n" +
				"(\n" +
				"`cid`,\n" +
				"`id`,`last_name`,\n" +
				"`first_name`,`is_male`,\n" +
				"marital_status,\n" +
				"`birthday`, city, street, street_num, zip_code, cell_phone, dyear\n" +
				")\n" +
				"select cid, id,  f_name, l_name, min, marital_status, leda, yeshuv, street, numStreet, mikud, telephone, dyear "
				+ "FROM " + name_schema + "." + name_table_alfon + " "
				+ "";

		tr.Insertintodb(s);


	}

}
