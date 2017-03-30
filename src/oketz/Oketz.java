/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oketz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import mySQL.Trysql;
import org.apache.commons.lang3.StringEscapeUtils;

/**
 *
 * @author user1
 */
public class Oketz {

	private Trysql tr;

	private Oketz(){
		tr = Trysql.getInstance();
	}

	private static Oketz instance = null;

	public static synchronized Oketz getInstance(){			
		if (instance == null)
		{ 	
			instance = new Oketz();	
		}
		return instance;
	}

	public void create_table_oketz(String name_schema, String name_table) throws SQLException {

		String b = "CREATE TABLE  if not exists`" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n"
				+ "`id_Worker` VARCHAR(200) NULL,"
				+ " `fullname` VARCHAR(200) NULL,"
				+ " `id` VARCHAR(200) NULL,"
				+ "`symbolName` VARCHAR(200) NULL,"
				+ "`value` VARCHAR(200) NULL,"
				+ "`m1` VARCHAR(200) NULL,"
				+ "`m2` VARCHAR(200) NULL,"
				+ "`m3` VARCHAR(200) NULL,"
				+ "`m4` VARCHAR(200) NULL,"
				+ "`m5` VARCHAR(200) NULL,"
				+ "`m6` VARCHAR(200) NULL,"
				+ "`m7` VARCHAR(200) NULL,"
				+ "`m8` VARCHAR(200) NULL,"
				+ "`m9` VARCHAR(200) NULL,"
				+ "`m10` VARCHAR(200) NULL,"
				+ "`m11` VARCHAR(200) NULL,"
				+ "`m12` VARCHAR(200) NULL,"
				+ "`dyear` VARCHAR(200) NULL,"
				+ " `cid` VARCHAR(200) NULL,"
				+ "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(b);

		truncate(name_schema, name_table);
                
                
                
	}

	void truncate(String name_scema, String name_table) throws SQLException {
		String a="TRUNCATE `"+name_scema+"`.`"+name_table+"`";

		System.out.println(a);
		tr.Insertintodb1(a);
	}


	public void create_insert_statment_from_array_list_zol_begadol(ArrayList<ArrayList<ArrayList<String>>> al, String name_schema, String name_table) throws SQLException {

		ArrayList<String> arrayList1 = new ArrayList<String>();
		String check2 = "SHOW COLUMNS FROM " + name_schema + "." + name_table;
		ResultSet gettabels = tr.gettabledb(check2);
		while (gettabels.next()) {
			arrayList1.add(gettabels.getString(1));
		}

		ArrayList<  ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < al.size(); i++) {

			list = al.get(i);
			for (int j = 0; j < list.size(); j++) {
				String a = list.get(j).get(1).replace("[", " ");
				// list.set(j, a);
				a = list.get(j).get(1).replace("]", " ");

				// list.set(j, a);
				System.out.println(a);
			}

			//INSERT INTO table_name (column1,column2,column3,...)
			//VALUES (value1,value2,value3,...);
			for (int k = 1; k < 100; k++) {

				String b = list.get(i++).get(1);

				String n = "insert into " + name_schema + "." + name_table + "(" + arrayList1.get(k) + ") VALUES (" + b + ")";
				System.out.println(n);
				tr.Insertintodb1(n);
			}
		}
	}

	public void create_insert_statment_from_array_list(ArrayList<ArrayList<String>> al, int month, int year, String name_schema, String name_table) throws SQLException {
		ArrayList<String> list = new ArrayList<String>();
		for (int i = 0; i < al.size(); i++) {

			list = al.get(i);
			for (int j = 0; j < list.size(); j++) {
				String a = list.get(j).replace("[", " ");
				list.set(j, a);
				a = list.get(j).replace("]", " ");

				list.set(j, a);
				System.out.println(a);
			}

			if (list.size() == 71) {
				String a = "INSERT INTO `" + name_schema + "`.`" + name_table + "`\n"
						+ "(`num_oved`,`name_oved`,`klitat_mascoret`,`sog_misra`,`dayes`,`hours`,`chishuv_mh`,\n"
						+ "`tarif_lyom`,\n"
						+ "`tarif_lshaha`,\n"
						+ "`sacar_yesod1`,\n"
						+ "`nesiot`,\n"
						+ "`havraha1`,\n"
						+ "`telephone`,`shaot_nosafot`,`chufsha`,`machala`,`pidyon_chufsha`,`sacar_meshulav`,`hescem_misgeret`,`tibor_horaha`,`tosefet_2001`,`tosefet_2008`,`tosefet_2011`,\n"
						+ "`gmol_hishtalmot`,\n"
						+ "`gmol_cefel_toar`,\n"
						+ "`hashlama_leminimom`,\n"
						+ "`bigod`,\n"
						+ "`tosefet_meonot`,\n"
						+ "`gmol_pensia`,\n"
						+ "`gmol_dafna`,\n"
						+ "`gmol_glarshaf`,\n"
						+ "`gmol_chinoch_cita`,\n"
						+ "`ymay_machala`,\n"
						+ "`tosefet_ezon`,\n"
						+ "`tosefet_1999`,\n"
						+ "`havraha`,\n"
						+ "`sacar_yesod`,\n"
						+ "`yom_bechirot`,\n"
						+ "`chagim`,\n"
						+ "`hafchatat_0.9324`,\n"
						+ "`headroyot`,\n"
						+ "`yemay_machala_2`,\n"
						+ "`yom_hatzmaot`,\n"
						+ "`hefresh_mascoret_mchodashim_kodmim`,\n"
						+ "`kizoz_machala`,\n"
						+ "`hechzer_scal`,\n"
						+ "`nesiot_2`,\n"
						+ "`kizoz_chofsha`,\n"
						+ "`hefreshei_horat_shaha`,\n"
						+ "`shot_shilov`,\n"
						+ "`shaot_nosafot2`,\n"
						+ "`machala_tashlum`,\n"
						+ "`shovi_telephone_nayad`,\n"
						+ "`broto`,\n"
						+ "`mascoret_chayevet_mh`,\n"
						+ "`mascoret_chayevet_bl`,\n"
						+ "`mh`,\n"
						+ "`bl`,\n"
						+ "`briot`,\n"
						+ "`hafkadot_oved`,\n"
						+ "`sach_nicoyim`,\n"
						+ "`neto`,\n"
						+ "`gemel_bleda`,\n"
						+ "`nicoyey_reshut`,\n"
						+ "`neto_letashlum`,\n"
						+ "`hetel_zarim`,\n"
						+ "`sach_alot`,\n"
						+ "`chufsha_nitzol`,\n"
						+ "`machala_nitzol`,\n"
						+ "`havraha_nitzul`,\n"
						+ "`miloim_nitzol`,m,dyear)\n"
						+ "VALUES(" + list.get(0) + ",'" + list.get(1) + "','" + list.get(2) + "','" + list.get(3) + "'," + list.get(4) + "," + list.get(5) + ",'"
						+ list.get(6) + "'," + list.get(7) + "," + list.get(8) + "," + list.get(9) + "," + list.get(10) + "," + list.get(11) + "," + list.get(12)
						+ "," + list.get(13) + "," + list.get(14) + "," + list.get(15) + "," + list.get(16) + "," + list.get(17)
						+ "," + list.get(18) + "," + list.get(19) + "," + list.get(20) + "," + list.get(21)
						+ "," + list.get(22) + ","
						+ list.get(23) + ","
						+ list.get(24) + ","
						+ list.get(25) + ","
						+ list.get(26) + ","
						+ list.get(27) + ","
						+ list.get(28) + ","
						+ list.get(29)
						+ "," + list.get(30)
						+ ","
						+ list.get(31) + ","
						+ list.get(32) + ","
						+ list.get(33) + ","
						+ list.get(34) + ","
						+ list.get(35) + "," + list.get(36) + ","
						+ list.get(37) + "," + list.get(38) + "," + list.get(39) + "," + list.get(40)
						+ "," + list.get(41) + ","
						+ list.get(42) + "," + list.get(43) + "," + list.get(44) + "," + list.get(45) + ","
						+ list.get(46) + "," + list.get(47) + "," + list.get(48) + "," + list.get(49)
						+ "," + list.get(50) + "," + list.get(51) + "," + list.get(52) + "," + list.get(53) + "," + list.get(54) + ","
						+ list.get(55)
						+ "," + list.get(56) + "," + list.get(57) + "," + list.get(58) + "," + list.get(59) + "," + list.get(60) + ","
						+ list.get(61) + "," + list.get(62)
						+ "," + list.get(63) + ","
						+ list.get(64) + "," + list.get(65) + "," + list.get(66) + ","
						+ list.get(67) + "," + list.get(68) + "," + list.get(69) + "," + list.get(70) + "," + month + "," + year + ")";
				tr.Insertintodb1(a);

			}
		}
	}

	public void create_table_101_oketz(String name_schema, String name_table_101) throws SQLException {

		String a = "CREATE TABLE if not exists " + name_schema + ".`" + name_table_101 + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n"
				+ "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n"
				+ "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n"
				+ "  `FullName` varchar(51) NOT NULL,\n"
				+ "  `Symbol` int(11) NOT NULL DEFAULT '0',\n"
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

	public void insert_Oketz(String name_schema, String name_table, String name_table1, int cid) throws SQLException {

		//  for (int i = 1; i < 13; i++) {
		String aa = "insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + " as cid,`dyear`,`num_oved`, '2000' as Symbol,'שכר יסוד'  as SymbolName ,num_oved,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";

		tr.Insertintodb1(aa);

		String a = "insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '2100' as Symbol,'נסיעות'  as SymbolName ,nesiot,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(a);
		a = " insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '2200' as Symbol,'הבראה'  as SymbolName ,havraha1,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);   ";
		tr.Insertintodb1(a);
		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '2300' as Symbol,'טלפון'  as SymbolName ,telephone,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";

		tr.Insertintodb1(a);

		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '2400' as Symbol,'שעות נוספות'  as SymbolName ,shaot_nosafot,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(a);
		a = "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '2500' as Symbol,'חופשה'  as SymbolName ,chufsha,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);\n";

		tr.Insertintodb1(a);

		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '2600' as Symbol,'מחלה'  as SymbolName ,machala,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);\n";

		tr.Insertintodb1(a);

		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '2700' as Symbol,'פדיון חופשה'  as SymbolName ,pidyon_chufsha,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(a);

		a = "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '2800' as Symbol,'שכר משולב'  as SymbolName ,sacar_meshulav,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";

		tr.Insertintodb1(a);

		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '2900' as Symbol,'הסכם מסגרת'  as SymbolName ,hescem_misgeret,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";

		tr.Insertintodb1(a);
		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '3000' as Symbol,'תגבור הוראה'  as SymbolName ,tibor_horaha,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";

		tr.Insertintodb1(a);

		a = "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '3100' as Symbol,'תוספת 2001'  as SymbolName ,tosefet_2001,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);\n";

		tr.Insertintodb1(a);
		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '3200' as Symbol,'תוספת 2008'  as SymbolName ,tosefet_2008,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);\n";

		tr.Insertintodb1(a);

		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '3300' as Symbol,'תוספת 2011'  as SymbolName ,tosefet_2011,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);\n";

		tr.Insertintodb1(a);

		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '3400' as Symbol,'גמול השתלמות '  as SymbolName ,gmol_hishtalmot,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";

		tr.Insertintodb1(a);
		a = "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '3500' as Symbol,'גמול כפל תואר'  as SymbolName ,gmol_cefel_toar,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(a);
		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '3600' as Symbol,'השלמה למינימום'  as SymbolName ,hashlama_leminimom,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";

		tr.Insertintodb1(a);
		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '3700' as Symbol,'ביגוד'  as SymbolName ,bigod,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(a);
		a = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '3800' as Symbol,'תוספת למעונות'  as SymbolName ,tosefet_meonot,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";

		tr.Insertintodb1(a);

		String s = "insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '3900' as Symbol,'גמול פנסיה'  as SymbolName ,gmol_pensia,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '4000' as Symbol,'גמול דפנה'  as SymbolName ,gmol_dafna,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";

		tr.Insertintodb1(s);
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '4100' as Symbol,'גמול גלרשף'  as SymbolName ,gmol_glarshaf,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '4200' as Symbol,'גמול חינוך כיתה'  as SymbolName ,gmol_chinoch_cita,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '4300' as Symbol,'ימי מחלה'  as SymbolName ,ymay_machala,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '4400' as Symbol,'תוספת איזון'  as SymbolName ,tosefet_ezon,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '4500' as Symbol,'תוספת 1999'  as SymbolName ,tosefet_1999,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '4600' as Symbol,'הבראה'  as SymbolName ,havraha,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '4700' as Symbol,'שכר יסוד'  as SymbolName ,sacar_yesod,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '4800' as Symbol,'יום בחירות'  as SymbolName ,yom_bechirot,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "  insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '4900' as Symbol,'חגים'  as SymbolName ,chagim,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		s
		= "           insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '5000' as Symbol,'הפחתת 0.9324'  as SymbolName ,`hafchatat_0.9324`,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '5100' as Symbol,'העדרויות'  as SymbolName ,headroyot,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '5200' as Symbol,'ימי מחלה'  as SymbolName ,yemay_machala_2,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s
		= "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '5300' as Symbol,'יום העצמאות'  as SymbolName ,yom_hatzmaot,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s
		= "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '5400' as Symbol,'הפרש משכורת מחודשים קודמים'  as SymbolName ,hefresh_mascoret_mchodashim_kodmim,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '5500' as Symbol,'קיזוז מחלה'  as SymbolName ,kizoz_machala,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '5600' as Symbol,'החזר שכר לימוד'  as SymbolName ,hechzer_scal,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '5700' as Symbol,'נסיעות'  as SymbolName ,nesiot_2,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '5800' as Symbol,'קיזוז חופשה'  as SymbolName ,kizoz_chofsha,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '6000' as Symbol,'הפרשי הוראת שעה'  as SymbolName ,hefreshei_horat_shaha,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '6100' as Symbol,'שעות שילוב'  as SymbolName ,shot_shilov,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '6100' as Symbol,'שעות נוספות '  as SymbolName ,shaot_nosafot2,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '6200' as Symbol,'מחלה תשלום'  as SymbolName ,machala_tashlum,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "        insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '6300' as Symbol,'שווי טלפון ניד'  as SymbolName ,shovi_telephone_nayad,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '6400' as Symbol,'ברוטו'  as SymbolName ,broto,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '6500' as Symbol,'משכרת חיבת מס הכנסה '  as SymbolName ,mascoret_chayevet_mh,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '6600' as Symbol,'משכרת חיבת ביטוח לאומי'  as SymbolName ,mascoret_chayevet_bl,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s
		= "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '6700' as Symbol,'מס הכנסה '  as SymbolName ,mh,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '6800' as Symbol,'ביטוח לאומי'  as SymbolName ,bl,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s
		= "      insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '6900' as Symbol,'בריאות'  as SymbolName ,briot,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '7000' as Symbol,'הפקדות עובד'  as SymbolName ,hafkadot_oved,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '7100' as Symbol,'סך ניכוים'  as SymbolName ,sach_nicoyim,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s
		= "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '7200' as Symbol,'נטו'  as SymbolName ,neto,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '7300' as Symbol,'גמל בלידה'  as SymbolName ,gemel_bleda,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '7400' as Symbol,'ניכוי רשות'  as SymbolName ,nicoyey_reshut,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);

		s = "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '7500' as Symbol,'נטו לתשלום'  as SymbolName ,neto_letashlum,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '7600' as Symbol,'היטל זרים'  as SymbolName ,hetel_zarim,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '7700' as Symbol,'סך עלות'  as SymbolName ,sach_alot,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "       insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '7800' as Symbol,'חופשה ניצול'  as SymbolName ,chufsha_nitzol,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "      insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '7900' as Symbol,'מחלה ניצול'  as SymbolName ,machala_nitzol,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s
		= "    insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '8000' as Symbol,'הבראה ניצול'  as SymbolName ,havraha_nitzul,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s
		= "     insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid + ",`dyear`,`num_oved`, '8100' as Symbol,'מילואים ניצול'  as SymbolName ,miloim_nitzol,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";

		tr.Insertintodb1(s);
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


	public void update_total(String name_schema, String name_table_101) throws SQLException {

		String a = "update " + name_schema + "." + name_table_101 + " set total=m1+m2+m3+m4+m5+m6+m7+m8+m9+m10+m11+m12";
		System.out.println(a);
		tr.Insertintodb1(a);

	}

	public void Final_Table(String name_schema, String name_table) throws SQLException {

		String a = "insert into diff_taxes_reports.tbl_101\n"
				+ "("
				+ "cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel"
				+ ")\n"
				+ "SELECT "
				+ "cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel"
				+ "\n"
				+ "FROM `" + name_schema + "`.`" + name_table + "`\n";
		tr.Insertintodb1(a);

		//  
	}

	void insert(ArrayList<ArrayList<ArrayList<String>>> al, String name_schema, String name_table, int cid, int year, int month) throws SQLException {
		
		System.out.println("insert year : " + year + "; month : " + month);
		
		
		ArrayList<String>sy=new ArrayList<>();
		String SymbolName=null;
		int jj=1;
		ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		//        int symbol = 1000;
		String FullName = null, id = null;
		int counter = 1;
		for (int i = 0; i < al.size(); i++) {
			sy.clear();
			jj=1;
			list = al.get(i);
			for (int j = 2; j < list.size(); j++) {
				FullName =escapeSQL( list.get(1).get(1));
				id = list.get(0).get(1);
				String z=StringEscapeUtils.unescapeCsv(list.get(j).get(1));
				z=   z.replace(",", "");
				SymbolName=escapeSQL(StringEscapeUtils.unescapeCsv(list.get(j).get(0)));

				if (sy.contains(SymbolName))
				{


					SymbolName=SymbolName+""+jj;
					jj++;
				}


				sy.add(SymbolName);



				//            if  (SymbolName.contains("נסיעות"))
				//            {
				//            
				//                System.out.println("gfjhgj");
				//            
				//            }
				String a = "insert into " + name_schema + "." + name_table + " (FullName,id,cid,dyear,SymbolName,m" + month + ")values('" + FullName
						+ "'," + id
						+ "," + cid
						+ ","
						+ year + ",'"
						+ SymbolName + "','" + z + "')"
						+ "on duplicate key update m" + month + "=values(m" + month + ");";
				System.out.println(a);
				tr.Insertintodb1(a);
			}

		}
	}

	String[][] csv2dar(String fileName, int maxRows, int maxCols) {
		BufferedReader bufRdr = null;
		String[][] dar;
		int lastRow;

		dar = null;
		try {

			dar = new String[maxRows][maxCols];

			File file = new File(fileName);
			bufRdr = new BufferedReader(new FileReader(file));
			String line = null;
			int row = 0;
			int col = 0;
			while ((line = bufRdr.readLine()) != null && row < maxRows) {
				String[] cl = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				for (String ccl : cl) {
					dar[row][col] = ccl;
					col++;
				}
				col = 0;
				row++;
			}
			lastRow = row - 1;
			return dar;

		} catch (Exception ex) {

		} finally {
			try {
				bufRdr.close();
			} catch (IOException ex) {

			}
		}

		return dar;
	}

	ArrayList<ArrayList<ArrayList<String>>> get_the_right_coulms(String[][] mat, int num_rows) {
		int mikom = 0;
		int savei = 0, savelength = 0;
		int length = 0;
		int l = 0;
		String save = null, save1 = null;
		String regex = "[0-9.,-]+";
		ArrayList<String> list_of_symbels = new ArrayList<String>();
		ArrayList<ArrayList<ArrayList<String>>> mat1 = new ArrayList<ArrayList<ArrayList<String>>>();
		//   ArrayList<ArrayList<String>> mat1 = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		ArrayList<String> list1 = new ArrayList<String>();
		for (int i = 0; i < num_rows; i++) {

			if (mat[i][0] == null) {
				continue;
			}
			if (mat[i][0].contains("מספר עובד")) {

				savei = i;
				i++;

				if (mat[i][0] != null) {
					while (mat[i][0] != null&&!mat[i][0].contains("מספר עובד")  ) {
						if (list_of_symbels.contains("מספר עובד")) {
							list_of_symbels.add(mat[i][0]);
						}
						mikom = 0;
						length++;
						i++;
						if (i == num_rows) {
							break;
						}
					}

				}

				savelength = length;
				length = 0;
				i--;
				///////////////////////////
				for (int k = 1; k < 6; k++) {
					mikom++;
					for (l = savei; l < savelength + savei + 1; l++) {

						//                       save=   mat[l][k].replace(",", "");
						//                   save1=    StringEscapeUtils.unescapeCsv(save);
						if (mat[l][k] != null) {
							if (!mat[l][k].contains("מספר עובד")) {
								if (mat[l][k - mikom].equals("עובד")) {
									list1.add(mat[l][k - mikom]);
									list1.add(mat[l][k]);
									list.add(list1);
									list1 = new ArrayList<String>();
								}
								if (mat[l][k].length() > 2 && mat[l][k].substring(0, 1).equals("\"") && mat[l][k].substring(mat[l][k].length() - 1).equals("\"")) {
									mat[l][k] = mat[l][k].substring(1, mat[l][k].length() - 1);
								}

								if (mat[l][k].contains("[")|| mat[l][k].contains("]"))
								{
									String a=mat[l][k].replace("[","").replace("]", "");
									list1.add(mat[l][k - mikom]);
									list1.add(a);
									list.add(list1);
									list1 = new ArrayList<String>();

								}

								if (mat[l][k].matches(regex)) {
									list1.add(mat[l][k - mikom]);
									list1.add(mat[l][k]);
									list.add(list1);
									list1 = new ArrayList<String>();
								}

							}
						}

					}

					mat1.add(list);
					list = new ArrayList<ArrayList<String>>();
					//   System.out.println(i);

				}

			}

		}

		return mat1;
	}

	public void create_symbels_numbers_and_insert_into_main_table1(String name_scema, String name_tabletemp, String name_table_101) throws SQLException {
		String a = "CREATE TABLE  if not exists `" + name_scema + "`.`" + name_tabletemp + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n"
				+ "  `symble` VARCHAR(200) NULL,\n"
				+ "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);
		String x = "TRUNCATE `" + name_scema + "`.`" + name_tabletemp + "`;";
		tr.Insertintodb1(x);
		String s = "ALTER TABLE `" + name_scema + "`.`" + name_tabletemp + "`  AUTO_INCREMENT = 2001";
		tr.Insertintodb1(s);

		String j = "insert into " + name_scema + "." + name_tabletemp + "(symble)  SELECT SymbolName FROM " + name_scema + "." + name_table_101 + " group by SymbolName;";
		tr.Insertintodb1(j);

		String insert = "UPDATE  " + name_scema + "." + name_table_101 + "  SET Symbol=(\n"
				+ "   SELECT in_id FROM " + name_scema + "." + name_tabletemp + " WHERE  " + name_tabletemp + ".symble = " + name_table_101 + ".SymbolName  COLLATE utf8_general_ci);";
		tr.Insertintodb1(insert);

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
}
