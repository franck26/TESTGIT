package netoMicpal;

import java.io.File;
import java.sql.SQLException;

import mySQL.Trysql;

public class NetoMicpal {


	private Trysql tr;
	
	static NetoMicpal instance = null;
	
	public NetoMicpal(){
		tr = Trysql.getInstance();
	}
	
	static NetoMicpal getInstance(){
		if(instance == null){
			instance = new NetoMicpal();
		}
		
		return instance;
			
	}

	public void create_table_neto(String name_schema,String name_table) throws SQLException{


		String a="CREATE TABLE  if not exists " + name_schema + ".`"+name_table+"` (\n" +
				"  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n" +
				"  `name_tz` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `mahlaka` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `shem` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `b` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `irgoun` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `briout` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `a` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `bruto` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `mas_hachnassa` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `leumi` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `tagmoulim` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `keren` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `istadrout` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `nikouy_aherim` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `schar_neto` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `nikouy_reshut` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `neto_letashlum` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `m` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `dyear` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `num_oved` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `full_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  PRIMARY KEY (`in_id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		
		tr.Insertintodb1(a);
		
		truncate(name_schema, name_table);

	}


	public void load_data_neto(String name_schema,String name_table, int year, String pathfile) throws SQLException {
			
		System.out.println("load neto year : " + year);
		
		File f = new File(pathfile+ "/" +year+"/neto");

		File[] listefichier = f.listFiles();

		int j = 0;

		for (int a = 0; a < listefichier.length; a++){
			if (listefichier[a].getAbsolutePath().endsWith(".csv")) {
				j++;
			}
		}
		
		for (int i = 1; i <= j; i++) {
				
				String a = "LOAD DATA  LOCAL INFILE"
						+ " '" + pathfile + "/"+year+"/neto/" + i + ".csv'"
						+ " INTO TABLE  `"+name_schema+"`." + name_table
						+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
						+ "  (  name_tz, mahlaka, a, bruto, mas_hachnassa, leumi, tagmoulim, keren, istadrout, nikouy_aherim, schar_neto,  nikouy_reshut, neto_letashlum)"
						+ "set dyear="+year+",m=" + i + ";";

				tr.Insertintodb1(a);
			}
		String z="update  " + name_schema + "."+name_table+"\n" +
				"set neto_letashlum = replace(neto_letashlum,',','');";
		tr.Insertintodb1(z);

		String x="update " + name_schema + ". "+name_table+"  as t2 set num_oved = substring_index(name_tz, ' - ', 1);";
		tr.Insertintodb1(x);
		String h="update " + name_schema + ". "+name_table+"  as t2 set full_name = substring_index(name_tz, ' - ', -1) ;";
		tr.Insertintodb1(h);

		h="delete from " + name_schema + ". "+name_table+" where name_tz LIKE '%סך %'  ;";
		tr.Insertintodb1(h);
		
	}
	
	public void load_data_neto(String name_schema,String name_table, String pathfile) throws SQLException {
		
		System.out.println("load neto ");
				
				String a = "LOAD DATA  LOCAL INFILE"
						+ " '" + pathfile + "/neto/neto.csv'"
						+ " INTO TABLE  `"+name_schema+"`." + name_table
						+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
						+ "  (  shem, dyear, num_oved, full_name, mahlaka, name_tz, a, b, m, bruto, mas_hachnassa, leumi, briout, tagmoulim, irgoun, schar_neto, nikouy_aherim, neto_letashlum)"
						;

				tr.Insertintodb1(a);
		String z="update  " + name_schema + "."+name_table+"\n" +
				"set schar_neto = replace(neto_letashlum,',','');";
		tr.Insertintodb1(z);

		String h="delete from " + name_schema + ". "+name_table+" where name_tz LIKE '%סך %'  ;";
		tr.Insertintodb1(h);
		
	}


	public void create_table_101(String name_schema, String name_table_101) throws SQLException {

		String a = "CREATE TABLE if not exists " + name_schema + ".`" + name_table_101 + "` (\n"
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
		
		truncate(name_schema, name_table_101);
		
	}
	

	public void convert_to_101_neto(String name_schema, String name_table_101,String name_table, int cid) throws SQLException {
		
		System.out.println("convert to 101");
		for (int i = 1; i < 13; i++) {
			String load = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ", num_worker) \n"
					+ "            SELECT   full_name," + cid + ",`dyear`,`name_tz`, 3000,'נטו',neto_letashlum, num_oved \n"
					+ "            FROM  "+name_schema+"."+name_table +"  where m=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load);
		}

	}
	
	public void update_total(String name_schema, String name_table_neto_101, String name_table_101_sofy, int year1, int year2, String name_hevra) throws SQLException {

		String a = "update " + name_schema + "." + name_table_neto_101 + " set total=m1+m2+m3+m4+m5+m6+m7+m8+m9+m10+m11+m12";
		tr.Insertintodb1(a);
		
		String s = "update " + name_schema + "." + name_table_neto_101 + " set type = ';tlush_neto;' where SymbolName = 'נטו' and symbol = 3000;";
		tr.Insertintodb(s);
		
		 a = "INSERT INTO "  + name_schema + "." + name_table_101_sofy + 
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
				"select cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel "
				+ "from " + name_schema + "." + name_table_neto_101;
		 tr.Insertintodb(a);
		 
		 tr.Insertintodb("UPDATE " + name_schema + "." + name_table_101_sofy + " as t1 inner join " + name_schema + "." + (name_hevra + "_" + year1 +"_" + year2 + "_alphon_101").toUpperCase() + " as t2 "
					+ "on t1.id = t2.num_worker and t1.dyear = t2.dyear    "
					+ "set t1.id = t2.id" );


	}
	
	public void truncate(String name_schema, String name_table) throws SQLException{

		String x = "TRUNCATE `" + name_schema + "`.`" + name_table + "`;";
		System.out.println(x);
		tr.Insertintodb1(x);
	}
	
	


}
