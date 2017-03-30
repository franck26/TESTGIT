/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package function;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import org.apache.commons.lang3.StringEscapeUtils;

import mySQL.Trysql;

/**
 *
 * @author shoshana
 */
public class Up_load2 {
	Trysql tr = new Trysql();
	Trysql tr1 = new Trysql();
	ResultSet gettabels = null;
	String name_tabel;
	String[][] dar;
	int lastRow;
	ArrayList<String> arrayList1;

	public void create_tabel_chelan(String name_schema, String name_tabel) throws SQLException {

		String create = "CREATE TABLE  if not exists  `" + name_schema + "`." + name_tabel + " ("
				+ "`in_id` INT NOT NULL AUTO_INCREMENT,"

				+ "`id_Worker` VARCHAR(200) NULL," + " `l_name` VARCHAR(200) NULL," + " `f_name` VARCHAR(200) NULL,"
				+ " `id` VARCHAR(200) NULL," + " `symbol` VARCHAR(200) NULL," + "`symbolName` VARCHAR(200) NULL,"
				+ "`value` VARCHAR(200) NULL," + "`m` VARCHAR(200) NULL," + "`dyear` VARCHAR(200) NULL,"
				+ "PRIMARY KEY (`in_id`))";

		tr.Insertintodb1(create);

	}

	public void create_tabel_micpal_tashlumim(String name_schema, String name_table) throws SQLException {

		String create = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "                `in_id` INT NOT NULL AUTO_INCREMENT,\n"
				+ "                `name_company` VARCHAR(200) NULL,\n"
				+ "                `tax_year` VARCHAR(200) NULL,\n" + "                `id_Worker` VARCHAR(200) NULL,\n"
				+ "                `full_name` VARCHAR(200) NULL,\n" + "               `m` VARCHAR(200) NULL,\n"
				+ "               `symbol` VARCHAR(200) NULL,\n" + "                `symbolName` VARCHAR(200) NULL,\n"
				+ "               `value` VARCHAR(200) NULL,\n" + "                 `sicom_camot` VARCHAR(200) NULL,\n"
				+ "                `dyear` VARCHAR(200) NULL,\n"
				+ "                 `name_machlaka` VARCHAR(200) NULL,\n"

				+ "               PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(create);

	}

	public void load_data(String name_schema, String name_tabel) throws SQLException {
		// name_tabel = name_tabel;
		for (int l = 2013; l < 2014; l++) {

			for (int i = 10; i < 11; i++) {

				String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/male_yosef/" + l + "/" + i + ".csv'"
						+ " INTO TABLE  `" + name_schema + "`." + name_tabel
						+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 2 LINES "
						+ "  ( mifal, tkufa, hanalach, semel, tear, machlaka, id, l_name, f_name, scum_nicoy)"
						+ "set dyear=" + l + ",month=" + i + ";";

				tr.Insertintodb1(a);
			}
		}
	}

	public void load_data_b(String name_schema, String name_tabel, int name) throws SQLException {
		// name_tabel = name_tabel;

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/onot/next_years/bituch_lemi/" + name
				+ ".csv'" + " INTO TABLE  `" + name_schema + "`." + name_tabel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 3 LINES "
				+ "  (  mis_oved, l_name, min, f_name, full_name, id,tarich_leda, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12)"
				+ "set dyear=" + name;

		tr.Insertintodb1(a);
	}

	public void load_data_micpal_tashlumim(String name_schema, String name_tabel, String name_file, int year)
			throws SQLException {

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/onot/" + year + "/" + name_file + ".csv'"
				+ " INTO TABLE `" + name_schema + "`." + name_tabel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  "
				+ " (name_company ,  tax_year , id_Worker , full_name, m ,symbol, symbolName ,value, sicom_camot,   name_machlaka,dyear)"
				+ "set dyear=" + year;
		// in_id, name_company, tax_year, id_Worker, full_name, m, symbol,
		// symbolName, value, sicom_camot, dyear, name_machlaka
		tr.Insertintodb1(a);

	}

	public void load_data_micpal_kupot_gemel(String name_schema, String name_tabel, String name_file, int year)
			throws SQLException {

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/onot/" + year + "/" + name_file + ".csv'"
				+ " INTO TABLE `" + name_schema + "`." + name_tabel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
				+ "  ( shem_hevra, shnat_mas, TZ, mis_oved, shem_oved, shem_mahlaka, mis_hodesh, zihoy_1, zihoy_2, mis_kupa, shem_kupa, mis_haver, tigmulim_oved, tigmulim_maavid, pitzuim, shonot,"
				+ " zkifa, sachar_mafkidim_lakupa, min, taarih_leida, matzav_mishpaha, yeshuv, ktovet, mis_bait, mikud, telephon, symbol)"
				+ "set dyear=" + year;

		tr.Insertintodb1(a);

	}

	public void load_data_micpal_nikouy_hova(String name_schema, String name_tabel, String name_file, int year)
			throws SQLException {

		// for (int i = 1; i < 8; i++) {

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/onot/" + year + "/" + name_file + ".csv'"
				+ " INTO TABLE `" + name_schema + "`." + name_tabel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  "
				+ " (shem_hevra, shnat_mas, mis_oved, shem_oved, shem_mahlaka, mis_hodesh, bituah_leumi, mas_irgun, sah_bituah_leumi, mas_briut, mas_hahnasa)";

		tr.Insertintodb1(a);
		// }
	}

	public void load_data_micpal_tamhir_ovdim(String name_schema, String name_table_micpal, String name_file, int year)
			throws SQLException {
		//
		for (int i = 1; i < 13; i++) {

			String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/onot/" + year + "/tamcir_ovdim/" + i
					+ ".csv'" + " INTO TABLE `" + name_schema + "`." + name_table_micpal
					+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
					+ "  (shem_hevra, shnat_mas, mis_oved, shem_oved, alut, sikum_bruto, sikum_tigmulim, sikum_pitzuim, sikum_shonot, sikum_bituah_leumi_maavid, sikum_mas_maasikim, sikum_mas_sachar, sikum_hetel_oved_zar, symbol)";

			tr.Insertintodb1(a);

			String s = "update `" + name_schema + "`.`" + name_table_micpal + "` set mis_hodesh =" + i
					+ " where mis_hodesh is null";
			tr.Insertintodb1(s);
		}

	}

	public void load_data_micpal_nikuy_reshut(String name_schema, String name_tabel, String name_file, int year)
			throws SQLException {
		// name_tabel = name_tabel;

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/onot/" + year + "/" + name_file + ".csv'"
				+ " INTO TABLE `" + name_schema + "`." + name_tabel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
				+ "  ( shem_hevra, shnat_mas, mis_oved, shem_oved, shem_mahlaka, mis_hodesh, mis_shura, mis_nikuy, kod_sug_nikuy, shem_nikuy, schum_nikuy, symbol)"
		// + "set dyear=2015";
		;
		// in_id, name_company, tax_year, id_Worker, full_name, m, symbol,
		// symbolName, value, sicom_camot, dyear, name_machlaka
		tr.Insertintodb1(a);

	}

	public void create_tabel_101(String name_schema, String name) throws SQLException {

		String a = "CREATE TABLE  if not exists " + name_schema + "." + name + "(\n"
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
	}

	public void convert_to_101(String name_schema, String name, String name_table, int cid) throws SQLException {
		for (int i = 1; i < 13; i++) {
			String load = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT   concat(`f_name`,' ',`l_name`)," + cid
					+ ",`dyear`,`id`, `symbol`,`symbolName`,`value`\n" + "            FROM  " + name_schema + "."
					+ name_table + "  where m=" + i + " \n" + "            on duplicate key update m" + i + "=values(m"
					+ i + ");";
			tr.Insertintodb1(load);
		}

	}

	public void create_tabel_micpal_koupot_gemel(String name_schema, String name_table) throws SQLException {

		String kupot_gemel = "CREATE TABLE if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n"
				+ "  `shem_hevra` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shnat_mas` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `TZ` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_oved` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shem_oved` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shem_mahlaka` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_hodesh` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `zihoy_1` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `zihoy_2` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_kupa` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shem_kupa` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_haver` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `tigmulim_oved` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `tigmulim_maavid` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `pitzuim` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shonot` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `zkifa` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `sachar_mafkidim_lakupa` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `min` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `taarih_leida` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `matzav_mishpaha` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `yeshuv` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `ktovet` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_bait` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mikud` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `telephon` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `symbol` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `dyear` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n" + "  PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		tr.Insertintodb1(kupot_gemel);

	}

	public void create_tabel_micpal_nikuy_hova(String name_schema, String name_table) throws SQLException {

		String nikuy = "CREATE TABLE if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n"
				+ "  `shem_hevra` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shnat_mas` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_oved` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shem_oved` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shem_mahlaka` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_hodesh` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `bituah_leumi` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mas_irgun` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `sah_bituah_leumi` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mas_briut` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mas_hahnasa` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n" + "  PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		tr.Insertintodb1(nikuy);
	}

	public void create_tabel_micpal_nikuy_reshut(String name_schema, String name_table) throws SQLException {
		String reshut = "CREATE TABLE if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n"
				+ "  `shem_hevra` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shnat_mas` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_oved` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shem_oved` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shem_mahlaka` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_hodesh` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_shura` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_nikuy` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `kod_sug_nikuy` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shem_nikuy` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `schum_nikuy` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `symbol` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" + "  PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		tr.Insertintodb1(reshut);
	}

	public void create_tabel_micpal_tamhir_ovdim(String name_schema, String name_table) throws SQLException {

		String tamchir = "CREATE TABLE if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n"
				+ "  `shem_hevra` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shnat_mas` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_oved` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `shem_oved` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `alut` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `sikum_bruto` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `sikum_tigmulim` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `sikum_pitzuim` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `sikum_shonot` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `sikum_bituah_leumi_maavid` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `sikum_mas_maasikim` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `sikum_mas_sachar` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `sikum_hetel_oved_zar` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `symbol` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"

				+ "  `mis_hodesh` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" + "  PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		tr.Insertintodb1(tamchir);
	}

	//
	public void create_101_micpal_koupot_gemel(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists  " + name_schema + ".`" + name_table + "` (\n"
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
	}

	public void create_101_micpal_nikuy_hova(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists " + name_schema + ".`" + name_table + "` (\n"
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

	}

	public void create_101_micpal_nikuy_reshut(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists " + name_schema + ".`" + name_table + "` (\n"
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
	}

	public void create_101_micpal_tamhir_ovdim(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists " + name_schema + ".`" + name_table + "` (\n"
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

	}

	public void create_101_micpal_tamhir_tashlumim(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists " + name_schema + ".`" + name_table + "` (\n"
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

	}

	public void Micpal_to_101_tashlumim(String name_schema, int cid, String name_table, String name_table1)
			throws SQLException {

		for (int i = 1; i < 13; i++) {
			String load = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "            SELECT   full_name," + cid
					+ ",`tax_year`,`id_Worker`, `symbol`,`symbolName`,`value`,'תשלומים','נתוני שכר'\n"
					+ "            FROM " + name_schema + "." + name_table1 + " where m=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load);

		}

	}

	public void Micpal_to_101_nikuy_hova(String name_schema, int cid, String name_table, String name_table1)
			throws SQLException {

		for (int i = 1; i < 13; i++) {
			// String load = "insert into Upload_file." + name_table + "\n"
			// + " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i +
			// ") \n"
			// + " SELECT shem_oved," + cid + ",`dyear`,`mis_oved`,
			// `2000`,'bituahleumi' as SymbolName ,`bituah_leumi`\n"
			// + " FROM Upload_file." + name_table1 + "where mis_hodesh=" + i +
			// " \n"
			// + " on duplicate key update m" + i + "=values(m" + i + ");";

			String a = " insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n  "
					+ " SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '2000' as Symbol,'ביטוח לאומי'  as SymbolName ,sum(bituah_leumi),'ניכוי חובה','נתוני שכר'"
					+ " FROM " + name_schema + "." + name_table1 + " where mis_hodesh=" + i
					+ " \n group by mis_oved,SymbolName,Symbol   on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(a);
			String b = " insert ignore into Upload_file." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n  "
					+ " SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '3000' as Symbol,'מס אירגון '  as SymbolName ,sum(mas_irgun),'ניכוי חובה','נתוני שכר'"
					+ " FROM " + name_schema + "." + name_table1 + " where mis_hodesh=" + i
					+ " \n  group by mis_oved,SymbolName,Symbol  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(b);
			String c = "  insert ignore into Upload_file." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n  "
					+ " SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '4000' as Symbol,'סך ביטוח לאומי '  as SymbolName ,sum(sah_bituah_leumi),'ניכוי חובה','נתוני שכר'"
					+ " FROM " + name_schema + "." + name_table1 + " where mis_hodesh=" + i
					+ " \n  group by mis_oved,SymbolName,Symbol  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(c);
			String f = "insert ignore into Upload_file." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n  "
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '5000' as Symbol,'מס בריאות '  as SymbolName ,sum(mas_briut),'ניכוי חובה','נתוני שכר'"
					+ " FROM " + name_schema + "." + name_table1 + " where mis_hodesh=" + i
					+ " \n group by mis_oved,SymbolName,Symbol   on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(f);
			String h = "insert ignore into Upload_file." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n  "
					+ " SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '6000' as Symbol,'מס הכנסה '  as SymbolName ,sum(mas_hahnasa),'ניכוי חובה','נתוני שכר'"
					+ " FROM " + name_schema + "." + name_table1 + " where mis_hodesh=" + i
					+ " \n group by mis_oved,SymbolName,Symbol   on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(h);
			// tr.Insertintodb1(load);
			// shem_hevra, shnat_mas, mis_oved, shem_oved, shem_mahlaka,
			// mis_hodesh, bituah_leumi, mas_irgun, sah_bituah_leumi, mas_briut,
			// mas_hahnasa, dyear
		}

	}

	public void update_total(String name_schema, String name_table) throws SQLException {

		String a = "update " + name_schema + "." + name_table + " set total=m1+m2+m3+m4+m5+m6+m7+m8+m9+m10+m11+m12";
		tr.Insertintodb1(a);

	}

	public void create_symbels() throws SQLException {

		String a = "  CREATE TABLE Upload_file.`symbols` (`kod` int(11) NOT NULL AUTO_INCREMENT,`name` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,PRIMARY KEY (kod))";
		tr.Insertintodb1(a);
		String v = "ALTER TABLE Upload_file.symbols AUTO_INCREMENT = 1500";
		tr.Insertintodb1(a);
	}

	public void insert_symbels() throws SQLException {

		String a = " INSERT INTO `Upload_file`.`symbols`\n"
				+ "(`name`)SELECT shem_nikuy FROM Upload_file.micpal_nikuy_reshut group by shem_nikuy";
		tr.Insertintodb1(a);
	}

	public void insert_cod_symbels_into_main_table(String table_name) throws SQLException {

		String a = "update Upload_file." + table_name + " as t1,Upload_file.symbols as t2" + "set t1.Symbol= t2.kod"
				+ "where shem_nikuy=  t2.name";
		tr.Insertintodb1(a);
	}

	public void update_source(String name_table, String name_source) throws SQLException {

		String a = "  update Upload_file.name_table" + "set source='" + name_source + "'";
		tr.Insertintodb1(a);
	}

	public void Micpal_to_101_nicoy_reshut(String name_schema, int cid, String name_table, String name_table1)
			throws SQLException {

		for (int i = 1; i < 13; i++) {
			String a = " insert  into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, symbol as Symbol,shem_nikuy as SymbolName \n"
					+ "   ,sum(schum_nikuy),'ניכוי רשות','נתוני שכר' FROM " + name_schema + "." + name_table1
					+ " where mis_hodesh=" + i + "\n"
					+ " group by mis_oved,SymbolName,symbol   on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

	}

	public void Micpal_to_101_tamchir_ovdim_per_year(String name_schema, int cid, String name_table, String name_table1)
			throws SQLException {
		for (int i = 1; i < 13; i++) {

			String a = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ "  as cid,`shnat_mas`,`mis_oved`, '1500' as Symbol,'עלות'  as SymbolName \n"
					+ "   ,alut/12,'תמחיר עובדים ' ,'נתוני שכר'  FROM " + name_schema + ". " + name_table1 + "  \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
			String b = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '1600' as Symbol,'סיכום ברוטו'  as SymbolName \n"
					+ "   ,sikum_bruto/12,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + ". " + name_table1
					+ "  \n" + "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(b);
			String c = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '1700' as Symbol,'סיכום תגמולים'  as SymbolName \n"
					+ "   ,sikum_tigmulim/12,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + "." + name_table1
					+ "  \n" + "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(c);
			String d = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '1800' as Symbol,'סיכום פיצוים'  as SymbolName \n"
					+ "   ,sikum_pitzuim/12,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + ". " + name_table1
					+ "  \n" + "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(d);
			String e = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '1900' as Symbol,'סיכום שונות'  as SymbolName \n"
					+ "   ,sikum_shonot/12,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + ". " + name_table1
					+ "  \n" + "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(e);
			String f = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '2000' as Symbol,'סיכום ביטוח לאומי מעביד'  as SymbolName \n"
					+ "   ,sikum_bituah_leumi_maavid/12,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + ". "
					+ name_table1 + "  \n" + "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(f);
		}

	}

	public void Micpal_to_101_tamchir_ovdim_per_month(String name_schema, int cid, String name_table,
			String name_table1) throws SQLException {
		for (int i = 1; i < 13; i++) {

			String a = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ "  as cid,`shnat_mas`,`mis_oved`, '1500' as Symbol,'עלות'  as SymbolName \n"
					+ "   ,alut,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + "." + name_table1
					+ " where mis_hodesh=" + i + " \n" + "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
			String b = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '1600' as Symbol,'סיכום ברוטו'  as SymbolName \n"
					+ "   ,sikum_bruto,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + "." + name_table1
					+ " where mis_hodesh=" + i + " \n" + "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(b);
			String c = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '1700' as Symbol,'סיכום תגמולים'  as SymbolName \n"
					+ "   ,sikum_tigmulim,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + "." + name_table1
					+ " where mis_hodesh=" + i + " \n" + "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(c);
			String d = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '1800' as Symbol,'סיכום פיצוים'  as SymbolName \n"
					+ "   ,sikum_pitzuim,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + "." + name_table1
					+ " where mis_hodesh=" + i + " \n" + "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(d);
			String e = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '1900' as Symbol,'סיכום שונות'  as SymbolName \n"
					+ "   ,sikum_shonot,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + "." + name_table1
					+ " where mis_hodesh=" + i + " \n" + "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(e);
			String f = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '2000' as Symbol,'סיכום ביטוח לאומי מעביד'  as SymbolName \n"
					+ "   ,sikum_bituah_leumi_maavid,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + "."
					+ name_table1 + " where mis_hodesh=" + i + " \n" + "    on duplicate key update m" + i + "=values(m"
					+ i + ");";
			tr.Insertintodb1(f);

			String g = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid
					+ " as cid,`shnat_mas`,`mis_oved`, '2100' as Symbol,'סיכום מס שכר '  as SymbolName \n"
					+ "   ,sikum_mas_sachar,'תמחיר עובדים ','נתוני שכר'  FROM " + name_schema + "." + name_table1
					+ " where mis_hodesh=" + i + " \n" + "    on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(g);

		}
	}

	public void create_101_micpal_sofi(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE   if not exists " + name_schema + ".`" + name_table + "` (\n"
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

	}

	public void insert_total_101_micpal(String name_scema, String name_table_101_sofi, String name_table_101_tashlumim,
			String name_table_101_nikuy_hova, String name_table_101_nikuy_reshut, String name_table_101_tamhir_ovdim)
			throws SQLException {

		String a = "insert ignore into Upload_file." + name_table_101_sofi + " \n"
				+ "            (cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) \n"
				+ "   SELECT   \n"
				+ "   cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel\n"
				+ "   \n" + "   \n" + "   FROM " + name_scema + "." + name_table_101_nikuy_hova;
		tr.Insertintodb1(a);
		String b = "insert ignore into Upload_file." + name_table_101_sofi + " \n"
				+ "            (cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) \n"
				+ "   SELECT   \n"
				+ "   cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel\n"
				+ "   \n" + "   \n" + "   FROM " + name_scema + "." + name_table_101_nikuy_reshut;
		tr.Insertintodb1(b);
		String c = "insert ignore into Upload_file." + name_table_101_sofi + " \n"
				+ "            (cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) \n"
				+ "   SELECT   \n"
				+ "   cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel\n"
				+ "   \n" + "   \n" + "   FROM " + name_scema + "." + name_table_101_tamhir_ovdim;
		tr.Insertintodb1(c);
		String d = "insert ignore into Upload_file." + name_table_101_sofi + " \n"
				+ "            (cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) \n"
				+ "   SELECT   \n"
				+ "   cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel\n"
				+ "   \n" + "   \n" + "   FROM " + name_scema + "." + name_table_101_tashlumim;
		tr.Insertintodb1(d);

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

	}

	public void Malam_load_data(String name_schema, int year, String name_table, String name_file, String kidod)
			throws SQLException {

		String a = "LOAD DATA  LOCAL INFILE "
				+ " '/home/user/NetBeansProjects/JavaApplication1/src/javaapplication1/Up_load_documents/" + name_file
				+ ".csv'" + "    INTO TABLE " + name_schema + ".`" + name_table + "` \n" + kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n" + "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE 1 LINES\n"
				+ "     ( office_num, office_name, `name`, numWorker, idWorker, m_n, derug, darga, start_date, vetek, department, unit, symbol_type, symbolName, m1, m1_amount, m2, m2_amount, m3, m3_amount, m4, m4_amount, m5, m5_amount, m6, m6_amount, m7, m7_amount, m8, m8_amount, m9, m9_amount, m10, m10_amount, m11, m11_amount, m12, m12_amount, all_sum, all_amount)\n"
				+ "     set dyear=" + year;
		tr.Insertintodb1(a);

	}

	public void Malam_load_data_1(String name_schema, int year, String name_table, String name_file, String kidod)
			throws SQLException {

		String a = "LOAD DATA  LOCAL INFILE " + " '/home/shoshana/Downloads/dan/" + name_file + ".csv'"
				+ "    INTO TABLE " + name_schema + ".`" + name_table + "` \n" + kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n" + "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE 6 LINES\n"
				+ "     ( office_num, office_name, `name`, numWorker, idWorker, m_n, derug, darga, start_date, vetek, department, unit, symbol_type, symbolName, m1, m1_amount, m2, m2_amount, m3, m3_amount, m4, m4_amount, m5, m5_amount, m6, m6_amount, m7, m7_amount, m8, m8_amount, m9, m9_amount, m10, m10_amount, m11, m11_amount, m12, m12_amount, all_sum, all_amount)\n"
				+ "     set dyear=" + year;
		tr.Insertintodb1(a);

	}

	public void Malam_load_data_fatal(String name_schema, String year, String name_table, int name_file, String kidod)
			throws SQLException {

		String a = "LOAD DATA  LOCAL INFILE " + " '/home/shoshana/Downloads/reaot/sacar/" + name_file + ".csv'"
				+ "    INTO TABLE " + name_schema + ".`" + name_table + "` \n" + kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n" + "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE 6 LINES\n" + "     ( office_num, office_name, `name`, numWorker, idWorker, m_n, "
				+ "derug, darga, start_date, vetek, department, unit, symbol_type, symbolName, m1, m1_amount, m2, m2_amount, m3, m3_amount, m4, m4_amount, m5, m5_amount, "
				+ "m6, m6_amount, m7, m7_amount, m8, m8_amount, m9, m9_amount, m10, m10_amount, m11, m11_amount, m12, m12_amount, all_sum, all_amount)\n"
				+ "     set dyear=" + year;
		tr.Insertintodb1(a);

	}

	public void Malam_load_data_isrotel(String name_schema, int year, String name_table, String name_file, String kidod)
			throws SQLException {
		/// home/shoshana/NetBeansProjects/JavaAppli
		String a = "LOAD DATA  LOCAL INFILE "
				+ " '/home/shoshana/NetBeansProjects/JavaApplication1/src/javaapplication1/Up_load_documents/"
				+ name_file + ".csv'" + "    INTO TABLE " + name_schema + ".`" + name_table + "` \n" + kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n" + "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE 1 LINES\n"
				+ "( F_name, L_name, num_oved, id, seif_taktzivi, Symbol, Symbol_name, m1_s, m1_c, m2_s, m2_c, m3_s, m3_c, m4_s, m4_c, m5_s, m5_c, m6_s, m6_c, m7_s, m7_c, m8_s, m8_c, m9_s, m9_c, m10_s, m10_c, m11_s, m11_c, m12_s, m12_c,dyear)"
				// + " ( office_num, office_name, `name`, numWorker, idWorker,
				// m_n, derug, darga, start_date, vetek, department, unit,
				// symbol_type, symbolName, m1, m1_amount, m2, m2_amount, m3,
				// m3_amount, m4, m4_amount, m5, m5_amount, m6, m6_amount, m7,
				// m7_amount, m8, m8_amount, m9, m9_amount, m10, m10_amount,
				// m11, m11_amount, m12, m12_amount, all_sum, all_amount)\n"
				+ "     set dyear=" + year;
		tr.Insertintodb1(a);

	}

	public void Malam_load_data_fanitzia(String name_schema, int year, String name_table, int name_file, String kidod,
			int cid) throws SQLException {
		/// home/shoshana/NetBeansProjects/JavaAppli
		String a = "LOAD DATA  LOCAL INFILE " + " '/home/shoshana/Downloads/isrotel_bechirim/" + name_file + ".csv'"
				+ "    INTO TABLE " + name_schema + ".`" + name_table + "` \n" + kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n" + "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE 1 LINES\n"
				+ "( F_name, L_name, num_oved, id, seif_taktzivi, Symbol, Symbol_name, m1_s, m1_c, m2_s, m2_c, m3_s, m3_c, m4_s, m4_c, m5_s, m5_c, m6_s, m6_c, m7_s, m7_c, m8_s, m8_c, m9_s, m9_c, m10_s, m10_c, m11_s, m11_c, m12_s, m12_c,dyear)"
				// + " ( office_num, office_name, `name`, numWorker, idWorker,
				// m_n, derug, darga, start_date, vetek, department, unit,
				// symbol_type, symbolName, m1, m1_amount, m2, m2_amount, m3,
				// m3_amount, m4, m4_amount, m5, m5_amount, m6, m6_amount, m7,
				// m7_amount, m8, m8_amount, m9, m9_amount, m10, m10_amount,
				// m11, m11_amount, m12, m12_amount, all_sum, all_amount)\n"
				+ "     set dyear=" + year + " , cid=" + cid;
		tr.Insertintodb1(a);

	}

	public void Malam_replace_comma(String name_schema, String name_table) throws SQLException {

		String a = "update " + name_schema + "." + name_table + "\n" + "set m1=replace(m1,',',''),\n"
				+ "m2=replace(m2,',',''),\n" + "m3=replace(m3,',',''),\n" + "m3=replace(m3,',',''),\n"
				+ "m4=replace(m4,',',''),\n" + "m5=replace(m5,',',''),\n" + "m6=replace(m6,',',''),\n"
				+ "m7=replace(m7,',',''),\n" + "m8=replace(m8,',',''),\n" + "m9=replace(m9,',',''),\n"
				+ "m10=replace(m10,',',''),\n" + "m11=replace(m11,',',''),\n" + "m12 =replace(m12,',','');";
		tr.Insertintodb1(a);

	}

	public void Malam_create_table_get_the_right_coulms(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists `" + name_schema + "`.`" + name_table + "`\n" + "(\n"
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
				+ "`cid` varchar(45) COLLATE utf8_unicode_ci DEFAULT '0' ,\n" + "PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		tr.Insertintodb1(a);
	}

	public void Malam_insert_into_temp(String name_schema, String name_table, String name_table1) throws SQLException {

		String a = "INSERT  INTO `" + name_schema + "`.`" + name_table + "`\n" + "(\n" + "`dyear`,\n" + "`id`,\n"
				+ "`FullName`,\n" + "`Symbol`,\n" + "`SymbolName`,\n" + "`symbol_type`,\n" + "`m1`,\n" + "`m2`,\n"
				+ "`m3`,\n" + "`m4`,\n" + "`m5`,\n" + "`m6`,\n" + "`m7`,\n" + "`m8`,\n" + "`m9`,\n" + "`m10`,\n"
				+ "`m11`,\n" + "`m12`, \n" + "`cid`\n" + ")\n" + " " + ""
						+ "select\n" + "dyear,idWorker, `name`,\n"
				+ "SUBSTRING_INDEX(SymbolName,'-',1),\n"
				+ "SUBSTR(SymbolName,instr(SymbolName,'-')+1,LENGTH(SymbolName)),`symbol_type`,\n"
				+ "sum(m1) as m1, sum(m2) as m2, sum(m3) as m3, sum(m4) as m4, sum(m5) as m5, sum(m6) as m6, sum(m7) as m7, sum(m8) as m8,\n"
				+ "sum(m9) as m9, sum(m10) as m10, sum(m11) as m11, sum(m12) as m12,office_num\n" + "from "
				+ name_schema + "." + name_table1 + "\n" + ""
						+ "group by dyear,idWorker,SymbolName,`symbol_type` \n"
				+ " on duplicate key update m1=values(m1),m2=values(m2),m3=values(m3),m4=values(m4),m5=values(m5),m6=values(m6),m7=values(m7),m8=values(m8),m9=values(m9),m10=values(m10),m11=values(m11),m12=values(m12);";
		tr.Insertintodb1(a);

	}

	public void Malam_insert_into_temp_isrotel(String name_schema, String name_table, String name_table1, int year)
			throws SQLException {

		String a = "INSERT  INTO `" + name_schema + "`.`" + name_table + "`\n" + "(\n" + "`cid`,\n" + "`dyear`,\n"
				+ "`id`,\n" + "`FullName`,\n" + "`Symbol`,\n" + "`SymbolName`,\n"

				+ "`m1`,\n" + "`m2`,\n" + "`m3`,\n" + "`m4`,\n" + "`m5`,\n" + "`m6`,\n" + "`m7`,\n" + "`m8`,\n"
				+ "`m9`,\n" + "`m10`,\n" + "`m11`,\n" + "`m12`\n" + ")SELECT cid,  \n"

				+ year + ", `id`,  concat(`F_name`,'',`L_name`),\n"
				+ "   `Symbol`,`Symbol_name`,`m1_s`,`m2_s`,`m3_s`,`m4_s`,`m5_s`,`m6_s`,`m7_s`,`m8_s`,`m9_s`,`m10_s`,`m11_s`,`m12_s`\n"
				+ "  \n" + "FROM  `" + name_schema + "`.`" + name_table1 + "`;";
		// + "select\n"
		// + "dyear,idWorker, `name`,\n"
		// + "SUBSTRING_INDEX(SymbolName,'-',1),\n"
		// +
		// "SUBSTR(SymbolName,instr(SymbolName,'-')+1,LENGTH(SymbolName)),`symbol_type`,\n"
		// + "sum(m1) as m1, sum(m2) as m2, sum(m3) as m3, sum(m4) as m4,
		// sum(m5) as m5, sum(m6) as m6, sum(m7) as m7, sum(m8) as m8,\n"
		// + "sum(m9) as m9, sum(m10) as m10, sum(m11) as m11, sum(m12) as
		// m12\n"
		// + "from " + name_schema + "." + name_table1 + "\n"
		// + "group by dyear,idWorker,SymbolName,`symbol_type` \n"
		// + " on duplicate key update
		// m1=values(m1),m2=values(m2),m3=values(m3),m4=values(m4),m5=values(m5),m6=values(m6),m7=values(m7),m8=values(m8),m9=values(m9),m10=values(m10),m11=values(m11),m12=values(m12);";
		tr.Insertintodb1(a);

	}

	public void set_symble(String name_schema, String name_table) throws SQLException {

		String a = "UPDATE `" + name_schema + "`.`" + name_table + "`\n" + "SET Symbol= '6565'\n"
				+ "WHERE Symbol='עלות מעביד';";
		tr.Insertintodb1(a);

	}

	public void create_101_malam_isrotel(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists  " + name_schema + ".`" + name_table + "` (\n"
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

	}

	public void Malam_insert_into_101(String name_schema, String name_table, String name_table1) throws SQLException {
		String a = "INSERT INTO `" + name_schema + "`.`" + name_table + "`\n" + "(\n" + "`cid`,\n" + "`dyear`,\n"
				+ "`id`,\n" + "`FullName`,\n" + "`Symbol`,\n" + "`SymbolName`,\n" + "`m1`,\n" + "`m2`,\n" + "`m3`,\n"
				+ "`m4`,\n" + "`m5`,\n" + "`m6`,\n" + "`m7`,\n" + "`m8`,\n" + "`m9`,\n" + "`m10`,\n" + "`m11`,\n"
				+ "`m12`,\n" + "`type`\n" + ")\n"
				+ "  select  cid  , dyear, id, FullName, Symbol,concat( SymbolName,'  ',`symbol_type`), ifnull(m1,'0'), ifnull(m2,'0'), ifnull(m3,'0'), ifnull(m4,'0'), ifnull(m5,'0'), ifnull(m6,'0'), ifnull(m7,'0'), ifnull(m8,'0') ,ifnull(m9,'0'),ifnull(m10,'0'),ifnull(m11,'0'),ifnull(m12,'0'),`symbol_type`\n"
				+ "from `" + name_schema + "`." + name_table1;

		tr.Insertintodb1(a);

	}

	public void Malam_insert_into_101_r(String name_schema, String name_table, String name_table1) throws SQLException {
		String a = "INSERT INTO `" + name_schema + "`.`" + name_table + "`\n" + "(\n" + "`cid`,\n" + "`dyear`,\n"
				+ "`id`,\n" + "`FullName`,\n" + "`Symbol`,\n" + "`SymbolName`,\n" + "`m1`,\n" + "`m2`,\n" + "`m3`,\n"
				+ "`m4`,\n" + "`m5`,\n" + "`m6`,\n" + "`m7`,\n" + "`m8`,\n" + "`m9`,\n" + "`m10`,\n" + "`m11`,\n"
				+ "`m12`,\n" + "`type`\n" + ")\n"
				+ "  select  cid  , dyear, id, FullName, Symbol, SymbolName, ifnull(m1,'0'), ifnull(m2,'0'), ifnull(m3,'0'), ifnull(m4,'0'), ifnull(m5,'0'), ifnull(m6,'0'), ifnull(m7,'0'), ifnull(m8,'0') ,ifnull(m9,'0'),ifnull(m10,'0'),ifnull(m11,'0'),ifnull(m12,'0'),`symbol_type`\n"
				+ "from `" + name_schema + "`." + name_table1;

		tr.Insertintodb1(a);

	}

	public void Malam_insert_into_fenitzia(String name_schema, String name_table, String name_table1)
			throws SQLException {
		String a = "INSERT INTO `" + name_schema + "`.`" + name_table + "`\n" + "(\n" + "`cid`,\n" + "`dyear`,\n"
				+ "`id`,\n" + "`FullName`,\n" + "`Symbol`,\n" + "`SymbolName`,\n" + "`m1`,\n" + "`m2`,\n" + "`m3`,\n"
				+ "`m4`,\n" + "`m5`,\n" + "`m6`,\n" + "`m7`,\n" + "`m8`,\n" + "`m9`,\n" + "`m10`,\n" + "`m11`,\n"
				+ "`m12`\n"
				// + "`type`\n"
				+ " )\n"
				+ "  select  cid  , dyear, id, FullName, Symbol, SymbolName, ifnull(m1,'0'), ifnull(m2,'0'), ifnull(m3,'0'), ifnull(m4,'0'), ifnull(m5,'0'), ifnull(m6,'0'), ifnull(m7,'0'), ifnull(m8,'0') ,ifnull(m9,'0') ,ifnull(m10,'0') ,ifnull(m11,'0') ,ifnull(m12,'0') \n"
				+ "from `" + name_schema + "`." + name_table1;

		tr.Insertintodb1(a);

	}

	public void Malam_insert_into_101_isrotel(String name_schema, String name_table, int cid, String name_table1)
			throws SQLException {
		String a = "INSERT INTO `" + name_schema + "`.`" + name_table + "`\n" + "(\n" + "`cid`,\n" + "`dyear`,\n"
				+ "`id`,\n" + "`FullName`,\n" + "`Symbol`,\n" + "`SymbolName`,\n" + "`m1`,\n" + "`m2`,\n" + "`m3`,\n"
				+ "`m4`,\n" + "`m5`,\n" + "`m6`,\n" + "`m7`,\n" + "`m8`,\n" + "`m9`,\n" + "`m10`,\n" + "`m11`,\n"
				+ "`m12`,\n" + "`type`\n" + ")\n" + "select\n" + "" + cid
				+ ", dyear, id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12,`symbol_type`\n"
				+ "from `" + name_schema + "`." + name_table1;

		tr.Insertintodb1(a);

	}

	public void Malam_insert_into_101_fanitzia(String name_schema, String name_table, String name_table1)
			throws SQLException {
		String a = "INSERT INTO `" + name_schema + "`.`" + name_table + "`\n" + "(\n" + "`cid`,\n" + "`dyear`,\n"
				+ "`id`,\n" + "`FullName`,\n" + "`Symbol`,\n" + "`SymbolName`,\n" + "`m1`,\n" + "`m2`,\n" + "`m3`,\n"
				+ "`m4`,\n" + "`m5`,\n" + "`m6`,\n" + "`m7`,\n" + "`m8`,\n" + "`m9`,\n" + "`m10`,\n" + "`m11`,\n"
				+ "`m12`\n"

				+ " )\n"
				+ "select  cid , dyear, id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12  \n"
				+ "from `" + name_schema + "`." + name_table1;

		tr.Insertintodb1(a);

	}

	void Final_Table(String name_schema, String name_table) throws SQLException {

		String a = "insert into diff_taxes_reports.tbl_101\n" + "("
				+ "cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel"
				+ ")\n" + "SELECT "
				+ "cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel"
				+ "\n" + "FROM `" + name_schema + "`.`" + name_table + "`\n";
		tr.Insertintodb1(a);

		//
	}

	public void create_table_Shiklolit_tamchir(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mispar_oved` VARCHAR(200) NULL,\n"
				+ "  `L_name` VARCHAR(200) NULL,\n" + "  `F_name` VARCHAR(200) NULL,\n" + "  `ID` VARCHAR(200) NULL,\n"
				+ "  `Machlaka` VARCHAR(200) NULL,\n" + "  `Mascoret_neto` VARCHAR(200) NULL,\n"
				+ "  `Mascoret_Broto` VARCHAR(200) NULL,\n" + " `Empty` VARCHAR(200) NULL,\n"
				+ "  `Bituch_leomi` VARCHAR(200) NULL,\n" + "  `Tagmolim_pitzum` VARCHAR(200) NULL,\n"
				+ "  `khl` VARCHAR(200) NULL,\n" + "  `Mas_sachar` VARCHAR(200) NULL,\n"
				+ "  `Hetal_oved_zar` VARCHAR(200) NULL,\n" + "  `Mas_masikim` VARCHAR(200) NULL,\n"
				+ "  `atoda_lapitzuim` VARCHAR(200) NULL,\n" + "  `atoda_lachufsha` VARCHAR(200) NULL,\n"
				+ "  `atoda_lhavraha` VARCHAR(200) NULL,\n" + "  `sach_alot` VARCHAR(200) NULL,\n"
				+ "  `alot` VARCHAR(200) NULL,\n" + "  `dyear` VARCHAR(200) NULL,\n"
				+ "  `mis_hodesh` VARCHAR(200) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(a);

	}

	public void Shiklolit_load_data_per_month(String name_schema, String name_table, String kidod) throws SQLException {
		/// home/shoshana/NetBeansProjects/JavaApplication1/src/javaapplication1/dor_group_tamchir/dor_group_ot_tamchir_per_month
		/// home/shoshana/NetBeansProjects/JavaApplication1/src/javaapplication1/dor_tv_tamchir/dor_tv_ot_tamchir_per_month
		// /home/shoshana/NetBeansProjects/JavaApplication1/src/javaapplication1/dor_media_spak_tamchir/dor_media_spak_tamchir_per_month

		for (int i = 2009; i < 2015; i++) {
			for (int j = 1; j < 13; j++) {

				String a = "LOAD DATA  LOCAL INFILE " + " '/home/shoshana/Downloads/aly_sich/" + i + "/" + j + ".csv'"
						+ "    INTO TABLE " + name_schema + ".`" + name_table + "` \n" + kidod
						+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n" + "	LINES TERMINATED BY '\\n' \n"
						+ "     IGNORE 8 LINES\n"
						+ "(mispar_oved, L_name, F_name, ID, Machlaka, Mascoret_neto, Mascoret_Broto,Empty, Bituch_leomi, Tagmolim_pitzum, khl, Mas_sachar, Hetal_oved_zar, Mas_masikim, atoda_lapitzuim, atoda_lachufsha, atoda_lhavraha, sach_alot, alot,dyear) "
						+ "     set dyear=" + i + " , mis_hodesh=" + j;
				tr.Insertintodb1(a);
			}
		}
		/// home/shoshana/NetBeansProjects/JavaApplication1/src/javaapplication1/dor_media_ot_tamchir/dor_media_ot_tamchir_per_month
	}

	public void Shiklolit_load_data_per_year(String name_schema, String name_table, int name_file, String kidod,
			int year) throws SQLException {
		// dor_tv_tamchir
		String a = "LOAD DATA  LOCAL INFILE " + " '/home/shoshana/Downloads/ohel_asher/tamchir/" + name_file + ".csv'"
				+ "    INTO TABLE " + name_schema + ".`" + name_table + "` \n" + kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n" + "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE 8 LINES\n"
				+ "(mispar_oved, L_name, F_name, ID, Machlaka, Mascoret_neto, Mascoret_Broto,Empty, Bituch_leomi, Tagmolim_pitzum, khl, Mas_sachar, Hetal_oved_zar, Mas_masikim, atoda_lapitzuim, atoda_lachufsha, atoda_lhavraha, sach_alot, alot,dyear) "
				+ "     set dyear=" + year;
		tr.Insertintodb1(a);

	}

	public void Shiklolit_before_101() {

		// String a = "CREATE TABLE `kiryatYam_fix_2015` (\n"
		// + " `factory` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `period` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `class` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `d_Class` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `idWorker` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `lastName` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `firstName` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `dateStart` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `symbol` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `d_Symbol` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `i` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `dateEnd` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `symbolStatus` varchar(45) COLLATE utf8_unicode_ci DEFAULT
		// NULL,\n"
		// + " `amount` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `precent` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `rate` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `rest` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `sum` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `cost` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `sumWithCost` varchar(45) COLLATE utf8_unicode_ci DEFAULT
		// NULL,\n"
		// + " `kodBl` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `bl` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `kodTax` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `taxKind` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
		// + " `realSection` varchar(45) COLLATE utf8_unicode_ci DEFAULT
		// NULL,\n"
		// + " \n"
		// + " `dyear` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL\n"
		// + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
	}

	void create_101_shiklolit(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n" + "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n" + "  `FullName` varchar(200) NOT NULL,\n"
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

	}

	void Shiklolit_insert_into_101_tamchir_per_year(String name_schema, String name_table, String name_table_101,
			int cid) throws SQLException {

		for (int i = 1; i < 13; i++) {

			String a = " insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n "
					+ "  SELECT    concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '2000' as Symbol,'משכרת נטו '  as Mascoret_neto ,Mascoret_neto/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + "where mis_hodesh=" + i + " \n "
					+ "  where `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(a);
			String b = " insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)" + " \n "
					+ " SELECT    concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '3000' as Symbol,'משכרת ברוטו '  as Mascoret_Broto ,Mascoret_Broto/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + " where mis_hodesh=" + i + " \n "
					+ "  where `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(b);
			String c = "  insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ "  SELECT    concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '4000' as Symbol,' ביטוח לאומי '  as Bituch_leomi ,Bituch_leomi/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + " where mis_hodesh=" + i + " \n "
					+ "  where `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(c);
			String f = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n     "
					+ "   SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '5000' as Symbol,'תגמולים פיצוים כושר שונות   '  as Tagmolim_pitzum ,Tagmolim_pitzum/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + "where mis_hodesh=" + i + " \n "
					+ "  where `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(f);
			String h = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT  concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '6000' as Symbol,'קהל'  as khl ,khl/12,'תמחיר עובדים'" + " FROM "
					+ name_schema + "." + name_table
					// + "where mis_hodesh=" + i + " \n "
					+ "    where `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(h);

			String n = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '7000' as Symbol,'מס שכר '  as Mas_sachar ,Mas_sachar/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + " where mis_hodesh=" + i + " \n "
					+ "  where `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(n);
			String z = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '8000' as Symbol,'היטל עובד זר '  as Hetal_oved_zar ,Hetal_oved_zar/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + "where mis_hodesh=" + i + " \n "
					+ "  where `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(z);

			String x = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '9000' as Symbol,'מס מעסיקים  '  as Mas_masikim ,Mas_masikim/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + "where mis_hodesh=" + i + " \n "
					+ "   where `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(x);

			String d = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT  concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '10000' as Symbol,'עתודה לפיצוים '  as atoda_lapitzuim ,atoda_lapitzuim/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + " where mis_hodesh=" + i + " \n"
					+ "   where `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(d);

			String m = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '11000' as Symbol,'עתודה לחופשה '  as atoda_lachufsha ,atoda_lachufsha/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + " where mis_hodesh=" + i + " \n "
					+ "     where `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(m);

			String k = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '12000' as Symbol,'עתודה להבראה '  as atoda_lhavraha ,atoda_lhavraha/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + " where mis_hodesh=" + i + " \n "
					+ "     where `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(k);

			String o = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '13000' as Symbol,'סך עלות '  as sach_alot ,sach_alot/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + "where mis_hodesh=" + i + " \n "
					+ "   where `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(o);

			String o1 = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '14000' as Symbol,'אחוז עלות  '  as sach_alot ,sach_alot/12,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table
					// + "where mis_hodesh=" + i + " \n "
					+ "    where `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(o1);

			// in_id, mispar_oved, L_name, F_name, ID, Machlaka, Mascoret_neto,
			// Mascoret_Broto, Bituch_leomi, Tagmolim_pitzum, khl, Mas_sachar,
			// Hetal_oved_zar, Mas_masikim, atoda_lapitzuim, atoda_lachufsha,
			// atoda_lhavraha, sach_alot, alot, dyear
		}

	}

	void Shiklolit_insert_into_101_tamchir_per_month(String name_schema, String name_table, String name_table_101,
			int cid) throws SQLException {

		for (int i = 1; i < 13; i++) {

			String a = " insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n "
					+ "  SELECT    concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '2000' as Symbol,'משכרת נטו '  as Mascoret_neto ,Mascoret_neto,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + " where mis_hodesh=" + i + " \n  "
					+ "  and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(a);
			String b = " insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)" + " \n "
					+ " SELECT    concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '3000' as Symbol,'משכרת ברוטו '  as Mascoret_Broto ,Mascoret_Broto,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " \n "
					+ "  and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(b);
			String c = "  insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ "  SELECT    concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '4000' as Symbol,' ביטוח לאומי '  as Bituch_leomi ,Bituch_leomi,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " \n "
					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(c);
			String f = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n     "
					+ "   SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '5000' as Symbol,'תגמולים פיצוים כושר שונות   '  as Tagmolim_pitzum ,Tagmolim_pitzum,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + " where mis_hodesh=" + i + " \n "
					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(f);
			String h = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT  concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '6000' as Symbol,'קהל'  as khl ,khl,'תמחיר עובדים'" + " FROM "
					+ name_schema + "." + name_table + " where mis_hodesh=" + i + " \n  "
					+ "    and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(h);

			String n = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '7000' as Symbol,'מס שכר '  as Mas_sachar ,Mas_sachar,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " \n "
					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(n);
			String z = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '8000' as Symbol,'היטל עובד זר '  as Hetal_oved_zar ,Hetal_oved_zar,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + " where mis_hodesh=" + i + " \n  "
					+ "  and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(z);

			String x = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '9000' as Symbol,'מס מעסיקים  '  as Mas_masikim ,Mas_masikim,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + " where mis_hodesh=" + i + " \n  "
					+ "   and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(x);

			String d = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT  concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '10000' as Symbol,'עתודה לפיצוים '  as atoda_lapitzuim ,atoda_lapitzuim,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " \n"
					+ "   and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(d);

			String m = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '11000' as Symbol,'עתודה לחופשה '  as atoda_lachufsha ,atoda_lachufsha,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " \n  "
					+ "     and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(m);

			String k = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '12000' as Symbol,'עתודה להבראה '  as atoda_lhavraha ,atoda_lhavraha,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " \n  "
					+ "     and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(k);

			String o = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '13000' as Symbol,'סך עלות '  as sach_alot ,sach_alot,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " \n "
					+ "   and `F_name` <>'' on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(o);

			String o1 = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n  "
					+ " SELECT   concat(`F_name`,' ',`L_name`)," + cid
					+ " as cid,`dyear`,`ID`, '14000' as Symbol,'אחוז עלות  '  as sach_alot ,sach_alot,'תמחיר עובדים'"
					+ " FROM " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " \n  "
					+ "    and `F_name` <>''  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(o1);

			// in_id, mispar_oved, L_name, F_name, ID, Machlaka, Mascoret_neto,
			// Mascoret_Broto, Bituch_leomi, Tagmolim_pitzum, khl, Mas_sachar,
			// Hetal_oved_zar, Mas_masikim, atoda_lapitzuim, atoda_lachufsha,
			// atoda_lhavraha, sach_alot, alot, dyear
		}

	}

	public void create_table_Shiklolit_tashlumim(String name_schema, String name_table)
			throws SQLException, SQLException {

		String a = "CREATE TABLE if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `name_tz` VARCHAR(200) NULL,\n"
				+ "  `m1` VARCHAR(200) NULL,\n" + "  `m2` VARCHAR(200) NULL,\n" + "  `m3` VARCHAR(200) NULL,\n"
				+ "  `m4` VARCHAR(200) NULL,\n" + "  `m5` VARCHAR(200) NULL,\n" + "  `m6` VARCHAR(200) NULL,\n"
				+ "  `m7` VARCHAR(200) NULL,\n" + "  `m8` VARCHAR(200) NULL,\n" + "  `m9` VARCHAR(200) NULL,\n"
				+ "  `m10` VARCHAR(200) NULL,\n" + "  `m11` VARCHAR(200) NULL,\n" + "  `m12` VARCHAR(200) NULL,\n"
				+ "  `total` VARCHAR(200) NULL,\n" + "  `dyear` VARCHAR(200) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(a);

	}

	public void Shiklolit_load_data_tashlumim(String name_schema, String name_table, int name_file, String kidod,
			int year) throws SQLException {

		String a = "LOAD DATA  LOCAL INFILE " + " '/home/shoshana/Downloads/mishcan/" + name_file + ".csv'"
				+ "    INTO TABLE " + name_schema + ".`" + name_table + "` \n" + kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n" + "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE 8 LINES\n" + "(name_tz, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total,dyear) "
				+ "     set dyear=" + year;
		tr.Insertintodb1(a);

	}

	public void create_table_101_Shiklolit_tashlumim(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n" + "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n" + "  `FullName` varchar(200) NOT NULL,\n"
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

	}

	// void Shiklolit_insert_into_101_tashlumim(String
	// tbl_Shiklolit_101_tashlumim, int i) throws SQLException {
	//
	// String a = "INSERT INTO Upload_file.Tbl_Shiklolit_101_tashlumim
	// (num_worker, FullName, id,division)\n"
	// + "SELECT substr(name_tz,LOCATE('עובד',name_tz)+5,3) as mis,\n"
	// + "substr(substr(name_tz,LOCATE('עובד',name_tz)+10),1,\n"
	// + "LOCATE('ת.ז',substr(name_tz,LOCATE('עובד',name_tz)+10))-1) as name
	// ,\n"
	// + "-- substr(semel,LOCATE('עובד',semel)+10), \n"
	// + "substr(name_tz,LOCATE('ת.ז',name_tz)+5,9) as id,\n"
	// + "substr(name_tz,LOCATE('מחלקה',name_tz)+6) as mahlaka \n"
	// + "FROM Upload_file.Tbl_Shiklolit_tashlumim\n"
	// + "WHERE name_tz like '%עובד%';";
	//
	// tr.Insertintodb1(a);
	//
	// }
	void shiklolit_get_the_right_rows(String name_schema, String name_table, String name_table_101, int year, int cid)
			throws SQLException {
		String s = "";
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

	public void create_symbels_numbers_and_insert_into_main_table(String name_scema, String name_tabletemp,
			String name_table_101) throws SQLException {
		String a = "CREATE TABLE  if not exists `" + name_scema + "`.`" + name_tabletemp + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `symble` VARCHAR(200) NULL,\n"

				+ "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);
		String x = "TRUNCATE `" + name_scema + "`.`" + name_tabletemp + "`;";
		tr.Insertintodb1(x);
		String s = "ALTER TABLE `" + name_scema + "`.`" + name_tabletemp + "`  AUTO_INCREMENT = 2001";
		tr.Insertintodb1(s);

		String j = "insert into " + name_scema + "." + name_tabletemp + "(symble)  SELECT SymbolName FROM " + name_scema
				+ "." + name_table_101 + " group by  SymbolName;";
		tr.Insertintodb1(j);

		String insert = "UPDATE  " + name_scema + "." + name_table_101 + "  SET Symbol=(\n" + "   SELECT in_id FROM "
				+ name_scema + "." + name_tabletemp + " WHERE  " + name_tabletemp + ".symble = " + name_table_101
				+ ".SymbolName  COLLATE utf8_general_ci);";
		tr.Insertintodb1(insert);

	}

	public void create_symbels_numbers_and_insert_into_main_table_pa(String name_scema, String name_tabletemp,
			String name_table_101) throws SQLException {
		String a = "CREATE TABLE  if not exists `" + name_scema + "`.`" + name_tabletemp + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `symble` VARCHAR(200) NULL,\n"

				+ "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);
		String x = "TRUNCATE `" + name_scema + "`.`" + name_tabletemp + "`;";
		tr.Insertintodb1(x);
		String s = "ALTER TABLE `" + name_scema + "`.`" + name_tabletemp + "`  AUTO_INCREMENT = 2001";
		tr.Insertintodb1(s);

		String j = "insert into " + name_scema + "." + name_tabletemp + "(symble)  SELECT shem_nikuy FROM " + name_scema
				+ "." + name_table_101 + " group by  shem_nikuy;";
		tr.Insertintodb1(j);

		String insert = "UPDATE  " + name_scema + "." + name_table_101 + "  SET Symbol=(\n" + "   SELECT in_id FROM "
				+ name_scema + "." + name_tabletemp + " WHERE  " + name_tabletemp + ".symble = " + name_table_101
				+ ".shem_nikuy  COLLATE utf8_general_ci);";
		tr.Insertintodb1(insert);

	}

	public void create_symbels_numbers_and_insert_into_main_table_ikea(String name_scema, String name_tabletemp,
			String name_table_101) throws SQLException {
		String a = "CREATE TABLE  if not exists `" + name_scema + "`.`" + name_tabletemp + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `symble` VARCHAR(200) NULL,\n"
				+ "  `symblenum` VARCHAR(200) NULL,\n" + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);
		String x = "TRUNCATE `" + name_scema + "`.`" + name_tabletemp + "`;";
		tr.Insertintodb1(x);
		String s = "ALTER TABLE `" + name_scema + "`.`" + name_tabletemp + "`  AUTO_INCREMENT = 2001";
		tr.Insertintodb1(s);

		String j = "insert into " + name_scema + "." + name_tabletemp
				+ "(symble,symblenum)  SELECT Symbol,  SymbolName FROM " + name_scema + "." + name_table_101
				+ " group by Symbol ,  SymbolName;";
		tr.Insertintodb1(j);

		// String insert = "UPDATE " + name_scema + "." + name_table_101 + " SET
		// Symbol=(\n"
		// + " SELECT in_id FROM " + name_scema + "." + name_tabletemp + " WHERE
		// " + name_tabletemp + ".symblenum = " + name_table_101 + ".SymbolName
		// COLLATE utf8_general_ci);";
		// tr.Insertintodb1(insert);

		// String insert = "update "+name_scema+"."+name_table_101+" as t1,
		// "+name_scema+"."+name_tabletemp +" as sy set t1.Symbol1=sy.in_id\n" +
		// " where \n" +
		// " ( t1.SymbolName COLLATE utf8_general_ci=sy.symble \n" +
		// " and t1.Symbol COLLATE utf8_general_ci=sy.symblenum )";
		// tr.Insertintodb1(insert);

		String aa = " update   Upload_file." + name_table_101 + "  as t1, Upload_file." + name_tabletemp + " as sy \n"
				+ "      set t1.Symbol=sy.in_id\n" + "    where  \n"
				+ "        (     t1.Symbol COLLATE utf8_general_ci=sy.symble  \n"
				+ "  and  t1.SymbolName COLLATE utf8_general_ci=sy.symblenum )";
		tr.Insertintodb1(aa);

	}

	public void create_symbels_numbers_and_insert_into_main_table1(String name_scema, String name_tabletemp,
			String name_table_101) throws SQLException {
		String a = "CREATE TABLE  if not exists `" + name_scema + "`.`" + name_tabletemp + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `symble` VARCHAR(200) NULL,\n"
				+ "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);
		String x = "TRUNCATE `" + name_scema + "`.`" + name_tabletemp + "`;";
		tr.Insertintodb1(x);
		String s = "ALTER TABLE `" + name_scema + "`.`" + name_tabletemp + "`  AUTO_INCREMENT = 2001";
		tr.Insertintodb1(s);

		String j = "insert into " + name_scema + "." + name_tabletemp + "(symble)  SELECT SymbolName FROM " + name_scema
				+ "." + name_table_101 + " group by SymbolName;";
		tr.Insertintodb1(j);

		String insert = "UPDATE  " + name_scema + "." + name_table_101 + "  SET Symbol=(\n" + "   SELECT in_id FROM "
				+ name_scema + "." + name_tabletemp + " WHERE  " + name_tabletemp + ".symble = " + name_table_101
				+ ".SymbolName  COLLATE utf8_general_ci);";
		tr.Insertintodb1(insert);

	}

	boolean saveString2file(String str, String path) {
		try (PrintWriter out = new PrintWriter(path)) {
			out.println(str);
		} catch (Exception ex) {
			return false;
		}
		return true;
	}

	boolean saveStringBuilder2file(StringBuilder sb1, String path) {
		try (PrintWriter out = new PrintWriter(path)) {
			out.println(sb1);
		} catch (Exception ex) {
			return false;
		}
		return true;
	}

	String[][] csv2dar(String fileName, int maxRows, int maxCols) {
		BufferedReader bufRdr = null;
		try {

			dar = null;
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

	ArrayList<ArrayList<String>> get_the_right_coulms1(String[][] mat) {
		int savei = 0, savelength = 0;
		int length = 0;
		int l = 0;
		ArrayList<String> list_of_symbels = new ArrayList<String>();
		ArrayList<ArrayList<String>> mat1 = new ArrayList<ArrayList<String>>();
		ArrayList<String> list = new ArrayList<String>();
		for (int i = 0; i < 370; i++) {

			if (mat[i][0] == null) {
				continue;
			}
			if (mat[i][0].contains("מספר עובד")) {

				savei = i;
				i++;

				if (mat[i][0] != null) {
					while (!mat[i][0].contains("מספר עובד") && mat[i][0] != null) {
						if (list_of_symbels.contains("מספר עובד")) {
							list_of_symbels.add(mat[i][0]);
						}
						length++;
						i++;
						// if (i == 358) {
						// break;
						// }
						if (i == 165) {
							break;
						}
					}

				}

				savelength = length;
				length = 0;
				i--;
				for (int k = 1; k < 6; k++) {

					for (l = savei; l < savelength + savei + 1; l++) {
						if (mat[l][k] != null) {
							if (!mat[l][k].contains("מספר עובד")) {
								list.add(mat[l][k]);
							}
						}

					}

					mat1.add(list);
					list = new ArrayList<String>();
					// System.out.println(i);

				}

			}

		}

		return mat1;
	}

	public void create_table_oketz(String name_schema, String name_table) throws SQLException {

		String b = "CREATE TABLE  if not exists`" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `num_oved` VARCHAR(200) NULL,\n"
				+ "  `name_oved` VARCHAR(200) NULL,\n" + "  `klitat_mascoret` VARCHAR(200) NULL,\n"
				+ "  `sog_misra` VARCHAR(200) NULL,\n" + "  `dayes` VARCHAR(200) NULL,\n"
				+ "  `hours` VARCHAR(200) NULL,\n" + "  `chishuv_mh` VARCHAR(200) NULL,\n"
				+ "  `tarif_lyom` VARCHAR(200) NULL,\n" + "  `tarif_lshaha` VARCHAR(200) NULL,\n"
				+ "  `sacar_yesod1` VARCHAR(45) NULL,\n" + "  `nesiot` VARCHAR(45) NULL,\n"
				+ "  `havraha1` VARCHAR(45) NULL,\n" + "  `telephone` VARCHAR(45) NULL,\n"
				+ "  `shaot_nosafot` VARCHAR(45) NULL,\n" + "  `chufsha` VARCHAR(45) NULL,\n"
				+ "  `machala` VARCHAR(45) NULL,\n" + "  `pidyon_chufsha` VARCHAR(45) NULL,\n"
				+ "  `sacar_meshulav` VARCHAR(45) NULL,\n" + "  `hescem_misgeret` VARCHAR(45) NULL,\n"
				+ "  `tibor_horaha` VARCHAR(45) NULL,\n" + "  `tosefet_2001` VARCHAR(45) NULL,\n"
				+ "  `tosefet_2008` VARCHAR(45) NULL,\n" + "  `tosefet_2011` VARCHAR(45) NULL,\n"
				+ "  `gmol_hishtalmot` VARCHAR(45) NULL,\n" + "  `gmol_cefel_toar` VARCHAR(45) NULL,\n"
				+ "  `hashlama_leminimom` VARCHAR(45) NULL,\n" + "  `bigod` VARCHAR(45) NULL,\n"
				+ "  `tosefet_meonot` VARCHAR(45) NULL,\n" + "  `gmol_pensia` VARCHAR(45) NULL,\n"
				+ "  `gmol_dafna` VARCHAR(45) NULL,\n" + "  `gmol_glarshaf` VARCHAR(45) NULL,\n"
				+ "  `gmol_chinoch_cita` VARCHAR(45) NULL,\n" + "  `ymay_machala` VARCHAR(45) NULL,\n"
				+ "  `tosefet_ezon` VARCHAR(45) NULL,\n" + "  `tosefet_1999` VARCHAR(45) NULL,\n"
				+ "  `havraha` VARCHAR(45) NULL,\n" + "  `sacar_yesod` VARCHAR(45) NULL,\n"
				+ "  `yom_bechirot` VARCHAR(45) NULL,\n" + "  `chagim` VARCHAR(45) NULL,\n"
				+ "  `hafchatat_0.9324` VARCHAR(45) NULL,\n" + "  `headroyot` VARCHAR(45) NULL,\n"
				+ "  `yemay_machala_2` VARCHAR(45) NULL,\n" + "  `yom_hatzmaot` VARCHAR(45) NULL,\n"
				+ "  `hefresh_mascoret_mchodashim_kodmim` VARCHAR(45) NULL COMMENT 'חח',\n"
				+ "  `kizoz_machala` VARCHAR(45) NULL,\n" + "  `hechzer_scal` VARCHAR(45) NULL,\n"
				+ "  `nesiot_2` VARCHAR(45) NULL,\n" + "  `kizoz_chofsha` VARCHAR(45) NULL,\n"
				+ "  `hefreshei_horat_shaha` VARCHAR(45) NULL,\n" + "  `shot_shilov` VARCHAR(45) NULL,\n"
				+ "  `shaot_nosafot2` VARCHAR(45) NULL,\n" + "  `machala_tashlum` VARCHAR(45) NULL,\n"
				+ "  `shovi_telephone_nayad` VARCHAR(45) NULL,\n" + "  `broto` VARCHAR(45) NULL,\n"
				+ "  `mascoret_chayevet_mh` VARCHAR(45) NULL,\n" + "  `mascoret_chayevet_bl` VARCHAR(45) NULL,\n"
				+ "  `mh` VARCHAR(45) NULL,\n" + "  `bl` VARCHAR(45) NULL,\n" + "  `briot` VARCHAR(45) NULL,\n"
				+ "  `hafkadot_oved` VARCHAR(45) NULL,\n" + "  `sach_nicoyim` VARCHAR(45) NULL,\n"
				+ "  `neto` VARCHAR(45) NULL,\n" + "  `gemel_bleda` VARCHAR(45) NULL,\n"
				+ "  `nicoyey_reshut` VARCHAR(45) NULL,\n" + "  `neto_letashlum` VARCHAR(45) NULL,\n"
				+ "  `hetel_zarim` VARCHAR(45) NULL,\n" + "  `sach_alot` VARCHAR(45) NULL,\n"
				+ "  `chufsha_nitzol` VARCHAR(45) NULL,\n" + "  `machala_nitzol` VARCHAR(45) NULL,\n"
				+ "  `havraha_nitzul` VARCHAR(45) NULL,\n" + "  `miloim_nitzol` VARCHAR(45) NULL,\n"
				+ "  `m` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(b);
	}

	void create_insert_statment_from_array_list(ArrayList<ArrayList<String>> al, int month, int year)
			throws SQLException {
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
				String a = "INSERT INTO `Upload_file`.`Tbl_Oketz_temp`\n"
						+ "(`num_oved`,`name_oved`,`klitat_mascoret`,`sog_misra`,`dayes`,`hours`,`chishuv_mh`,\n"
						+ "`tarif_lyom`,\n" + "`tarif_lshaha`,\n" + "`sacar_yesod1`,\n" + "`nesiot`,\n"
						+ "`havraha1`,\n"
						+ "`telephone`,`shaot_nosafot`,`chufsha`,`machala`,`pidyon_chufsha`,`sacar_meshulav`,`hescem_misgeret`,`tibor_horaha`,`tosefet_2001`,`tosefet_2008`,`tosefet_2011`,\n"
						+ "`gmol_hishtalmot`,\n" + "`gmol_cefel_toar`,\n" + "`hashlama_leminimom`,\n" + "`bigod`,\n"
						+ "`tosefet_meonot`,\n" + "`gmol_pensia`,\n" + "`gmol_dafna`,\n" + "`gmol_glarshaf`,\n"
						+ "`gmol_chinoch_cita`,\n" + "`ymay_machala`,\n" + "`tosefet_ezon`,\n" + "`tosefet_1999`,\n"
						+ "`havraha`,\n" + "`sacar_yesod`,\n" + "`yom_bechirot`,\n" + "`chagim`,\n"
						+ "`hafchatat_0.9324`,\n" + "`headroyot`,\n" + "`yemay_machala_2`,\n" + "`yom_hatzmaot`,\n"
						+ "`hefresh_mascoret_mchodashim_kodmim`,\n" + "`kizoz_machala`,\n" + "`hechzer_scal`,\n"
						+ "`nesiot_2`,\n" + "`kizoz_chofsha`,\n" + "`hefreshei_horat_shaha`,\n" + "`shot_shilov`,\n"
						+ "`shaot_nosafot2`,\n" + "`machala_tashlum`,\n" + "`shovi_telephone_nayad`,\n" + "`broto`,\n"
						+ "`mascoret_chayevet_mh`,\n" + "`mascoret_chayevet_bl`,\n" + "`mh`,\n" + "`bl`,\n"
						+ "`briot`,\n" + "`hafkadot_oved`,\n" + "`sach_nicoyim`,\n" + "`neto`,\n" + "`gemel_bleda`,\n"
						+ "`nicoyey_reshut`,\n" + "`neto_letashlum`,\n" + "`hetel_zarim`,\n" + "`sach_alot`,\n"
						+ "`chufsha_nitzol`,\n" + "`machala_nitzol`,\n" + "`havraha_nitzul`,\n"
						+ "`miloim_nitzol`,m,dyear)\n" + "VALUES(" + list.get(0) + ",'" + list.get(1) + "','"
						+ list.get(2) + "','" + list.get(3) + "'," + list.get(4) + "," + list.get(5) + ",'"
						+ list.get(6) + "'," + list.get(7) + "," + list.get(8) + "," + list.get(9) + "," + list.get(10)
						+ "," + list.get(11) + "," + list.get(12) + "," + list.get(13) + "," + list.get(14) + ","
						+ list.get(15) + "," + list.get(16) + "," + list.get(17) + "," + list.get(18) + ","
						+ list.get(19) + "," + list.get(20) + "," + list.get(21) + "," + list.get(22) + ","
						+ list.get(23) + "," + list.get(24) + "," + list.get(25) + "," + list.get(26) + ","
						+ list.get(27) + "," + list.get(28) + "," + list.get(29) + "," + list.get(30) + ","
						+ list.get(31) + "," + list.get(32) + "," + list.get(33) + "," + list.get(34) + ","
						+ list.get(35) + "," + list.get(36) + "," + list.get(37) + "," + list.get(38) + ","
						+ list.get(39) + "," + list.get(40) + "," + list.get(41) + "," + list.get(42) + ","
						+ list.get(43) + "," + list.get(44) + "," + list.get(45) + "," + list.get(46) + ","
						+ list.get(47) + "," + list.get(48) + "," + list.get(49) + "," + list.get(50) + ","
						+ list.get(51) + "," + list.get(52) + "," + list.get(53) + "," + list.get(54) + ","
						+ list.get(55) + "," + list.get(56) + "," + list.get(57) + "," + list.get(58) + ","
						+ list.get(59) + "," + list.get(60) + "," + list.get(61) + "," + list.get(62) + ","
						+ list.get(63) + "," + list.get(64) + "," + list.get(65) + "," + list.get(66) + ","
						+ list.get(67) + "," + list.get(68) + "," + list.get(69) + "," + list.get(70) + "," + month
						+ "," + year + ")";
				tr.Insertintodb1(a);

			}
		}
	}

	void create_insert_statment_from_array_list_zol_begadol(ArrayList<ArrayList<ArrayList<String>>> al, int month,
			int year, String name_schema, String name_table) throws SQLException {

		arrayList1 = new ArrayList<String>();
		String check2 = "SHOW COLUMNS FROM Upload_file." + name_table;
		gettabels = tr.gettabledb(check2);
		arrayList1 = new ArrayList<String>();
		while (gettabels.next()) {
			arrayList1.add(gettabels.getString(1));
		}

		ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < al.size(); i++) {

			list = al.get(i);
			for (int j = 0; j < list.size(); j++) {
				String a = list.get(j).get(1).replace("[", " ");
				// list.set(j, a);
				a = list.get(j).get(1).replace("]", " ");

				// list.set(j, a);
				System.out.println(a);
			}

			// INSERT INTO table_name (column1,column2,column3,...)
			// VALUES (value1,value2,value3,...);
			for (int k = 1; k < 100; k++) {

				String b = list.get(i++).get(1);

				String n = "insert into " + name_schema + "." + name_table + "(" + arrayList1.get(k) + ") VALUES (" + b
						+ ")";
				System.out.println(n);

				// if (list.size() == 71) {
				// String a = "INSERT INTO `Upload_file`.`Tbl_Oketz_temp`\n"
				// +
				// "(`num_oved`,`name_oved`,`klitat_mascoret`,`sog_misra`,`dayes`,`hours`,`chishuv_mh`,\n"
				// + "`tarif_lyom`,\n"
				// + "`tarif_lshaha`,\n"
				// + "`sacar_yesod1`,\n"
				// + "`nesiot`,\n"
				// + "`havraha1`,\n"
				// +
				// "`telephone`,`shaot_nosafot`,`chufsha`,`machala`,`pidyon_chufsha`,`sacar_meshulav`,`hescem_misgeret`,`tibor_horaha`,`tosefet_2001`,`tosefet_2008`,`tosefet_2011`,\n"
				// + "`gmol_hishtalmot`,\n"
				// + "`gmol_cefel_toar`,\n"
				// + "`hashlama_leminimom`,\n"
				// + "`bigod`,\n"
				// + "`tosefet_meonot`,\n"
				// + "`gmol_pensia`,\n"
				// + "`gmol_dafna`,\n"
				// + "`gmol_glarshaf`,\n"
				// + "`gmol_chinoch_cita`,\n"
				// + "`ymay_machala`,\n"
				// + "`tosefet_ezon`,\n"
				// + "`tosefet_1999`,\n"
				// + "`havraha`,\n"
				// + "`sacar_yesod`,\n"
				// + "`yom_bechirot`,\n"
				// + "`chagim`,\n"
				// + "`hafchatat_0.9324`,\n"
				// + "`headroyot`,\n"
				// + "`yemay_machala_2`,\n"
				// + "`yom_hatzmaot`,\n"
				// + "`hefresh_mascoret_mchodashim_kodmim`,\n"
				// + "`kizoz_machala`,\n"
				// + "`hechzer_scal`,\n"
				// + "`nesiot_2`,\n"
				// + "`kizoz_chofsha`,\n"
				// + "`hefreshei_horat_shaha`,\n"
				// + "`shot_shilov`,\n"
				// + "`shaot_nosafot2`,\n"
				// + "`machala_tashlum`,\n"
				// + "`shovi_telephone_nayad`,\n"
				// + "`broto`,\n"
				// + "`mascoret_chayevet_mh`,\n"
				// + "`mascoret_chayevet_bl`,\n"
				// + "`mh`,\n"
				// + "`bl`,\n"
				// + "`briot`,\n"
				// + "`hafkadot_oved`,\n"
				// + "`sach_nicoyim`,\n"
				// + "`neto`,\n"
				// + "`gemel_bleda`,\n"
				// + "`nicoyey_reshut`,\n"
				// + "`neto_letashlum`,\n"
				// + "`hetel_zarim`,\n"
				// + "`sach_alot`,\n"
				// + "`chufsha_nitzol`,\n"
				// + "`machala_nitzol`,\n"
				// + "`havraha_nitzul`,\n"
				// + "`miloim_nitzol`,m,dyear)\n"
				// + "VALUES(" + list.get(0) + ",'" + list.get(1) + "','" +
				// list.get(2) + "','" + list.get(3) + "'," + list.get(4) + ","
				// + list.get(5) + ",'"
				// + list.get(6) + "'," + list.get(7) + "," + list.get(8) + ","
				// + list.get(9) + "," + list.get(10) + "," + list.get(11) + ","
				// + list.get(12)
				// + "," + list.get(13) + "," + list.get(14) + "," +
				// list.get(15) + "," + list.get(16) + "," + list.get(17)
				// + "," + list.get(18) + "," + list.get(19) + "," +
				// list.get(20) + "," + list.get(21)
				// + "," + list.get(22) + ","
				// + list.get(23) + ","
				// + list.get(24) + ","
				// + list.get(25) + ","
				// + list.get(26) + ","
				// + list.get(27) + ","
				// + list.get(28) + ","
				// + list.get(29)
				// + "," + list.get(30)
				// + ","
				// + list.get(31) + ","
				// + list.get(32) + ","
				// + list.get(33) + ","
				// + list.get(34) + ","
				// + list.get(35) + "," + list.get(36) + ","
				// + list.get(37) + "," + list.get(38) + "," + list.get(39) +
				// "," + list.get(40)
				// + "," + list.get(41) + ","
				// + list.get(42) + "," + list.get(43) + "," + list.get(44) +
				// "," + list.get(45) + ","
				// + list.get(46) + "," + list.get(47) + "," + list.get(48) +
				// "," + list.get(49)
				// + "," + list.get(50) + "," + list.get(51) + "," +
				// list.get(52) + "," + list.get(53) + "," + list.get(54) + ","
				// + list.get(55)
				// + "," + list.get(56) + "," + list.get(57) + "," +
				// list.get(58) + "," + list.get(59) + "," + list.get(60) + ","
				// + list.get(61) + "," + list.get(62)
				// + "," + list.get(63) + ","
				// + list.get(64) + "," + list.get(65) + "," + list.get(66) +
				// ","
				// + list.get(67) + "," + list.get(68) + "," + list.get(69) +
				// "," + list.get(70) + "," + month + "," + year + ")";
				// tr.Insertintodb1(a);
				// }
			}
		}
	}

	void create_table_101_oketz(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists " + name_schema + ".`" + name_table + "` (\n"
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

		// tr.Insertintodb1(a);
	}

	public void insert_Oketz(String name_schema, String name_table, String name_table1, int cid) throws SQLException {

		// for (int i = 1; i < 13; i++) {
		String aa = "insert ignore into " + name_schema + "." + name_table1 + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved," + cid
				+ " as cid,`dyear`,`num_oved`, '2000' as Symbol,'שכר יסוד'  as SymbolName ,num_oved,'בראשית' \n"
				+ "   FROM " + name_schema + "." + name_table + "  where m=1 \n"
				+ "    on duplicate key update m1=values(m1);";

		tr.Insertintodb1(aa);

		String a = "insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '2100' as Symbol,'נסיעות'  as SymbolName ,nesiot,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(a);
		a = " insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '2200' as Symbol,'הבראה'  as SymbolName ,havraha1,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);   ";
		tr.Insertintodb1(a);
		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '2300' as Symbol,'טלפון'  as SymbolName ,telephone,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";

		tr.Insertintodb1(a);

		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '2400' as Symbol,'שעות נוספות'  as SymbolName ,shaot_nosafot,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(a);
		a = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '2500' as Symbol,'חופשה'  as SymbolName ,chufsha,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);\n";

		tr.Insertintodb1(a);

		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '2600' as Symbol,'מחלה'  as SymbolName ,machala,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);\n";

		tr.Insertintodb1(a);

		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '2700' as Symbol,'פדיון חופשה'  as SymbolName ,pidyon_chufsha,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(a);

		a = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '2800' as Symbol,'שכר משולב'  as SymbolName ,sacar_meshulav,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";

		tr.Insertintodb1(a);

		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '2900' as Symbol,'הסכם מסגרת'  as SymbolName ,hescem_misgeret,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";

		tr.Insertintodb1(a);
		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '3000' as Symbol,'תגבור הוראה'  as SymbolName ,tibor_horaha,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";

		tr.Insertintodb1(a);

		a = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '3100' as Symbol,'תוספת 2001'  as SymbolName ,tosefet_2001,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);\n";

		tr.Insertintodb1(a);
		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '3200' as Symbol,'תוספת 2008'  as SymbolName ,tosefet_2008,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);\n";

		tr.Insertintodb1(a);

		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '3300' as Symbol,'תוספת 2011'  as SymbolName ,tosefet_2011,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);\n";

		tr.Insertintodb1(a);

		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '3400' as Symbol,'גמול השתלמות '  as SymbolName ,gmol_hishtalmot,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";

		tr.Insertintodb1(a);
		a = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '3500' as Symbol,'גמול כפל תואר'  as SymbolName ,gmol_cefel_toar,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(a);
		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '3600' as Symbol,'השלמה למינימום'  as SymbolName ,hashlama_leminimom,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";

		tr.Insertintodb1(a);
		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '3700' as Symbol,'ביגוד'  as SymbolName ,bigod,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(a);
		a = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '3800' as Symbol,'תוספת למעונות'  as SymbolName ,tosefet_meonot,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";

		tr.Insertintodb1(a);

		String s = "insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '3900' as Symbol,'גמול פנסיה'  as SymbolName ,gmol_pensia,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '4000' as Symbol,'גמול דפנה'  as SymbolName ,gmol_dafna,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";

		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '4100' as Symbol,'גמול גלרשף'  as SymbolName ,gmol_glarshaf,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '4200' as Symbol,'גמול חינוך כיתה'  as SymbolName ,gmol_chinoch_cita,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '4300' as Symbol,'ימי מחלה'  as SymbolName ,ymay_machala,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '4400' as Symbol,'תוספת איזון'  as SymbolName ,tosefet_ezon,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '4500' as Symbol,'תוספת 1999'  as SymbolName ,tosefet_1999,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '4600' as Symbol,'הבראה'  as SymbolName ,havraha,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '4700' as Symbol,'שכר יסוד'  as SymbolName ,sacar_yesod,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '4800' as Symbol,'יום בחירות'  as SymbolName ,yom_bechirot,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "  insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '4900' as Symbol,'חגים'  as SymbolName ,chagim,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		s = "           insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '5000' as Symbol,'הפחתת 0.9324'  as SymbolName ,`hafchatat_0.9324`,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '5100' as Symbol,'העדרויות'  as SymbolName ,headroyot,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '5200' as Symbol,'ימי מחלה'  as SymbolName ,yemay_machala_2,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '5300' as Symbol,'יום העצמאות'  as SymbolName ,yom_hatzmaot,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '5400' as Symbol,'הפרש משכורת מחודשים קודמים'  as SymbolName ,hefresh_mascoret_mchodashim_kodmim,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '5500' as Symbol,'קיזוז מחלה'  as SymbolName ,kizoz_machala,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '5600' as Symbol,'החזר שכר לימוד'  as SymbolName ,hechzer_scal,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '5700' as Symbol,'נסיעות'  as SymbolName ,nesiot_2,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '5800' as Symbol,'קיזוז חופשה'  as SymbolName ,kizoz_chofsha,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '6000' as Symbol,'הפרשי הוראת שעה'  as SymbolName ,hefreshei_horat_shaha,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '6100' as Symbol,'שעות שילוב'  as SymbolName ,shot_shilov,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '6100' as Symbol,'שעות נוספות '  as SymbolName ,shaot_nosafot2,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '6200' as Symbol,'מחלה תשלום'  as SymbolName ,machala_tashlum,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "        insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '6300' as Symbol,'שווי טלפון ניד'  as SymbolName ,shovi_telephone_nayad,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '6400' as Symbol,'ברוטו'  as SymbolName ,broto,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '6500' as Symbol,'משכרת חיבת מס הכנסה '  as SymbolName ,mascoret_chayevet_mh,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '6600' as Symbol,'משכרת חיבת ביטוח לאומי'  as SymbolName ,mascoret_chayevet_bl,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '6700' as Symbol,'מס הכנסה '  as SymbolName ,mh,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '6800' as Symbol,'ביטוח לאומי'  as SymbolName ,bl,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "      insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '6900' as Symbol,'בריאות'  as SymbolName ,briot,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '7000' as Symbol,'הפקדות עובד'  as SymbolName ,hafkadot_oved,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '7100' as Symbol,'סך ניכוים'  as SymbolName ,sach_nicoyim,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1)";
		tr.Insertintodb1(s);
		s = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '7200' as Symbol,'נטו'  as SymbolName ,neto,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '7300' as Symbol,'גמל בלידה'  as SymbolName ,gemel_bleda,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '7400' as Symbol,'ניכוי רשות'  as SymbolName ,nicoyey_reshut,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);

		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '7500' as Symbol,'נטו לתשלום'  as SymbolName ,neto_letashlum,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '7600' as Symbol,'היטל זרים'  as SymbolName ,hetel_zarim,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '7700' as Symbol,'סך עלות'  as SymbolName ,sach_alot,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "       insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '7800' as Symbol,'חופשה ניצול'  as SymbolName ,chufsha_nitzol,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "      insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '7900' as Symbol,'מחלה ניצול'  as SymbolName ,machala_nitzol,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "    insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '8000' as Symbol,'הבראה ניצול'  as SymbolName ,havraha_nitzul,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";
		tr.Insertintodb1(s);
		s = "     insert ignore into Upload_file.Tbl_Oketz_101\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "   SELECT   name_oved,258741369 as cid,`dyear`,`num_oved`, '8100' as Symbol,'מילואים ניצול'  as SymbolName ,miloim_nitzol,'בראשית' \n"
				+ "   FROM Upload_file.Tbl_Oketz_temp  where m=1 \n" + "    on duplicate key update m1=values(m1);";

		tr.Insertintodb1(s);
	}

	public void create_table_chelan_with_months_101(String name_schema, String name_table) throws SQLException {

		String create = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "                `in_id` INT NOT NULL AUTO_INCREMENT,\n"
				+ "                `name_company` VARCHAR(200) NULL,\n"
				+ "                `num_Worker` VARCHAR(200) NULL,\n" + "                `l_name` VARCHAR(200) NULL,\n"
				+ "                `f_name` VARCHAR(200) NULL,\n" + "                `id_Worker` VARCHAR(200) NULL,\n"
				+ "               `symbol` VARCHAR(200) NULL,\n" + "                `symbolName` VARCHAR(200) NULL,\n"
				+ "               `m1` VARCHAR(200) NULL,\n" + "               `m2` VARCHAR(200) NULL,\n"
				+ "               `m3` VARCHAR(200) NULL,\n" + "               `m4` VARCHAR(200) NULL,\n"
				+ "               `m5` VARCHAR(200) NULL,\n" + "               `m6` VARCHAR(200) NULL,\n"
				+ "               `m7` VARCHAR(200) NULL,\n" + "               `m8` VARCHAR(200) NULL,\n"
				+ "               `m9` VARCHAR(200) NULL,\n" + "               `m10` VARCHAR(200) NULL,\n"
				+ "               `m11` VARCHAR(200) NULL,\n" + "               `m12` VARCHAR(200) NULL,\n"
				+ "               `dyear` VARCHAR(200) NULL,\n" + "               PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(create);
		//

	}

	void load_data_chelan_with_months_101(String name_schema, String name_file, String name_tabel, int year)
			throws SQLException {

		String a = "LOAD DATA  LOCAL INFILE"
				+ " '/home/user/NetBeansProjects/JavaApplication1/src/javaapplication1/Up_load_documents/" + name_file
				+ ".csv'" + " INTO TABLE `" + name_schema + "`." + name_tabel
				+ "  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 2 LINES "
				+ "  ( name_company, num_Worker, l_name, f_name, id_Worker, symbol, symbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12)"
				+ "set dyear=" + year;
		// in_id, name_company, tax_year, id_Worker, full_name, m, symbol,
		// symbolName, value, sicom_camot, dyear, name_machlaka
		tr.Insertintodb1(a);

	}

	public void create_tabel_101_chelan_with_months(String name_schema, String name) throws SQLException {

		String a = "CREATE TABLE  if not exists " + name_schema + "." + name + "(\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n" + "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n" + "  `FullName` varchar(51) NOT NULL,\n"
				+ "  `Symbol` int(11) NOT NULL,\n" + "  `SymbolName` varchar(100) NOT NULL,\n"
				+ "  `m1` varchar(200) NOT NULL DEFAULT '0',\n" + "  `m2` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m3` varchar(200) NULL DEFAULT '0',\n" + "  `m4` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m5` varchar(200) NOT NULL DEFAULT '0',\n" + "  `m6` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m7` varchar(200) NOT NULL DEFAULT '0',\n" + "  `m8` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m9` varchar(200) NOT NULL DEFAULT '0',\n" + "  `m10` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `m11` varchar(200) NOT NULL DEFAULT '0',\n" + "  `m12` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `total` varchar(200) NOT NULL DEFAULT '0',\n"
				+ "  `division` bigint(20) unsigned NOT NULL DEFAULT '0',\n" + "  `run_version` int(11) DEFAULT NULL,\n"
				+ "  `date_value` varchar(50) DEFAULT NULL,\n" + "  `source` varchar(100) DEFAULT NULL,\n"
				+ "  `type` varchar(400) DEFAULT NULL,\n" + "  `num_worker` int(11) unsigned DEFAULT '0',\n"
				+ "  `permission` int(11) unsigned DEFAULT NULL,\n" + "  `type_for_gemel` varchar(400) DEFAULT NULL,\n"
				+ "  PRIMARY KEY (`in_id`),\n"
				+ "  UNIQUE KEY `ind_unique` (`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,`division`) USING BTREE,\n"
				+ "  KEY `indi3` (`cid`,`dyear`,`Symbol`,`SymbolName`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";

		tr.Insertintodb1(a);
	}

	public void chelan_convert_to_101_with_months(String name_schema, String name_table, String name_table1, int cid)
			throws SQLException {

		String a = "insert ignore  into " + name_schema + "." + name_table + "\n"
				+ "            (cid, dyear, id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12) \n"
				+ "   SELECT " + cid + " as cid,`dyear`,`id`as id,   concat(`f_name`,' ',`l_name`) as FullName,\n"
				+ "    symbol as Symbol,symbol_name as SymbolName , ifnull(m1,'0'),ifnull(m2,'0'), ifnull(m3,'0'), ifnull(m4,'0'), ifnull(m5,'0'), "
				+ "ifnull(m6,'0'), ifnull(m7,'0'), ifnull(m8,'0'), ifnull(m9,'0'), ifnull(m10,'0'), ifnull(m11,'0'), ifnull(m12,'0') "
				+ "  FROM " + name_schema + "." + name_table1;

		tr.Insertintodb1(a);

	}

	void chelan_delete_extra_rows(String name_schema, String name_table) throws SQLException {
		String a = "DELETE FROM `" + name_schema + "`.`" + name_table + "` WHERE `id`='0'";
		String b = "DELETE   FROM " + name_schema + "." + name_table + " where Symbol=0 and SymbolName=\" \"";
		String c = "DELETE FROM " + name_schema + "." + name_table
				+ " where Symbol=5015 and SymbolName like '%אחוז משרה%'";
		tr.Insertintodb1(a);
		tr.Insertintodb1(b);
		tr.Insertintodb1(c);
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
		// ArrayList<ArrayList<String>> mat1 = new
		// ArrayList<ArrayList<String>>();
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
					while (mat[i][0] != null && !mat[i][0].contains("מספר עובד")) {
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

						// save= mat[l][k].replace(",", "");
						// save1= StringEscapeUtils.unescapeCsv(save);
						if (mat[l][k] != null) {
							if (!mat[l][k].contains("מספר עובד")) {
								if (mat[l][k - mikom].equals("עובד")) {
									list1.add(mat[l][k - mikom]);
									list1.add(mat[l][k]);
									list.add(list1);
									list1 = new ArrayList<String>();
								}
								if (mat[l][k].length() > 2 && mat[l][k].substring(0, 1).equals("\"")
										&& mat[l][k].substring(mat[l][k].length() - 1).equals("\"")) {
									mat[l][k] = mat[l][k].substring(1, mat[l][k].length() - 1);
								}

								if (mat[l][k].contains("[") || mat[l][k].contains("]")) {
									String a = mat[l][k].replace("[", "").replace("]", "");
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
					// System.out.println(i);

				}

			}

		}

		return mat1;
	}

	void insert(ArrayList<ArrayList<ArrayList<String>>> al, String name_schema, String name_table, int cid, int year,
			int month) throws SQLException {
		ArrayList<String> sy = new ArrayList<>();
		String SymbolName = null;
		int jj = 1;
		ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		// int symbol = 1000;
		String FullName = null, id = null;
		int counter = 1;
		for (int i = 0; i < al.size(); i++) {
			sy.clear();
			jj = 1;
			list = al.get(i);
			for (int j = 2; j < list.size(); j++) {
				FullName = escapeSQL(list.get(1).get(1));
				id = list.get(0).get(1);
				String z = StringEscapeUtils.unescapeCsv(list.get(j).get(1));
				z = z.replace(",", "");
				SymbolName = escapeSQL(StringEscapeUtils.unescapeCsv(list.get(j).get(0)));

				if (sy.contains(SymbolName)) {

					SymbolName = SymbolName + "" + jj;
					jj++;
				}

				sy.add(SymbolName);

				// if (SymbolName.contains("נסיעות"))
				// {
				//
				// System.out.println("gfjhgj");
				//
				// }
				String a = "insert into " + name_schema + "." + name_table + " (FullName,id,cid,dyear,SymbolName,m"
						+ month + ")values('" + FullName + "'," + id + "," + cid + "," + year + ",'" + SymbolName
						+ "','" + z + "')" + "on duplicate key update m" + month + "=values(m" + month + ");";

				tr.Insertintodb1(a);
			}

		}
	}

	void insert_magen_a(ArrayList<ArrayList<ArrayList<String>>> al, String name_schema, String name_table, int cid,
			int year, int month) throws SQLException {

		ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		// int symbol = 1000;
		String FullName = null, id = null, SymbolName1 = null, SymbolName = null;
		int counter = 1;
		for (int i = 0; i < al.size(); i++) {
			list = al.get(i);
			for (int j = 2; j < list.size(); j++) {
				FullName = escapeSQL(list.get(1).get(1));
				id = list.get(0).get(1);
				String z = StringEscapeUtils.unescapeCsv(list.get(j).get(1));
				z = z.replace(",", "");
				SymbolName = escapeSQL(StringEscapeUtils.unescapeCsv(list.get(j).get(0)));
				System.out.println(SymbolName);
				if (SymbolName.contains("גמל פנסיה") || SymbolName.contains("גמל דפנה")
						|| SymbolName.contains("גמל גלרשף")) {
					SymbolName1 = SymbolName.replace("[", "").replace("]", "");
					String a = "insert into " + name_schema + "." + name_table + " (FullName,id,cid,dyear,SymbolName,m"
							+ month + ")values('" + FullName + "'," + id + "," + cid + "," + year + ",'" + SymbolName1
							+ "','" + z + "')" + "on duplicate key update m" + month + "=values(m" + month + ");";

					tr.Insertintodb1(a);
				}

			}

		}
	}

	void idit_right_coulms(String[][] mat, String name_schema, String name_table, int year) throws SQLException {
		ArrayList<String> list = new ArrayList<String>();
		String id = null;
		String f_name = null;
		String l_name = null;
		String birth_date = null;
		Boolean bool = true;
		for (int i = 1; i < mat.length; i++) {

			if (mat[i][0] == null) {

				continue;
			}

			if (mat[i][0].contains("פרטים אישיים")) {
				i++;
				id = mat[i][2];
				l_name = mat[i][4];
				f_name = mat[i][6];
				birth_date = mat[i + 2][2];

				i += 6;

				bool = true;
			}
			if (mat[i][0].contains("כרטיס אישי לעובד")) {

				while (mat[i][0] != null && !mat[i][0].contains("פרטים אישיים")) {
					if (bool == true) {
						i += 3;
						bool = false;
					}
					int j = 1;

					if (mat[i][j] == " " || mat[i][j] == "" || mat[i][j] == null || mat[i][j].isEmpty()
							|| mat[i][j].contains("''")) {
						mat[i][j] = "0";
					}

					if (mat[i][j + 1] == null || mat[i][j + 1] == " " || mat[i][j + 1] == "" || mat[i][j + 1].isEmpty()
							|| mat[i][j + 1].contains("''")) {
						mat[i][j + 1] = "0";
					}
					String load = "insert into " + name_schema + "." + name_table + "\n"
							+ "            (`FullName`,`cid`,`dyear`,`id`,Symbol,SymbolName) values( '" + f_name + " "
							+ l_name + "',159738264," + year + "," + id + "," + escapeSQL(mat[i][j]) + ",'"
							+ escapeSQL(mat[i][j + 1]) + "')";

					tr.Insertintodb1(load);
					load = "select MAX(m.in_id )from  " + name_schema + " ." + name_table + " m";
					int s = tr.Readfromdb(load);

					j += 1;
					for (int k = 1; k < 13; k++) {
						if (mat[i][j + k] == null || mat[i][j + k].isEmpty() || " ".equals(mat[i][j + k])
								|| mat[i][j + k].equals("") || mat[i][j + k].isEmpty()
								|| mat[i][j + k].contains("''")) {
							mat[i][j + k] = "0";
						}
						mat[i][j + k] = mat[i][j + k].replace(",", "");

						load = "update " + name_schema + "." + name_table + " set m" + k + "=" + mat[i][j + k]
								+ " where in_id=" + s;

						tr.Insertintodb1(load);

					}

					i++;

				}

			}
			bool = true;
		}
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

	public void create_csv_format() throws FileNotFoundException {

		PrintWriter pw = new PrintWriter(
				new File("/home/user/NetBeansProjects/JavaApplication1/src/javaapplication1/idit/11.csv"));
		StringBuilder sb = new StringBuilder();
		sb.append("id");
		sb.append(',');
		sb.append("Name");
		sb.append('\n');

		sb.append("1");
		sb.append(',');
		sb.append("Prashant Ghimire");
		sb.append('\n');

		pw.write(sb.toString());
		pw.close();
		System.out.println("done!");

	}

	public void load_data_idit(String name_tabel, String name_file) throws SQLException {
		// name_tabel = name_tabel;

		String a = "LOAD DATA  LOCAL INFILE"
				+ " '/home/user/NetBeansProjects/JavaApplication1/src/javaapplication1/idit/1.csv'"
				+ " INTO TABLE `Upload_file`." + name_tabel
				+ "_tashlumim   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES   (name_company ,  tax_year , id_Worker , full_name, m ,symbol, symbolName ,value, sicom_camot, dyear , name_machlaka)"
				+ "set dyear=2009";
		// in_id, name_company, tax_year, id_Worker, full_name, m, symbol,
		// symbolName, value, sicom_camot, dyear, name_machlaka
		tr.Insertintodb1(a);

	}

	String sqlNumericEscape(String val) {
		String regex = "[0-9.,-]+";
		if (val == null) {
			return "0";
		}
		if (val.length() > 1 && val.substring(0, 1).equals("\"") && val.substring(val.length() - 1).equals("\"")) {
			val = val.substring(1, val.length() - 1);
		}
		if (val.equals(" ") || val.equals("") || val.isEmpty()) {
			return "0";
		}

		if (!val.matches(regex)) {
			return "0";
		}
		return val.replace(",", "");

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

	void idit_right_coulms_with_out_update(String[][] mat, String name_schema, String name_table, int year, int len)
			throws SQLException {
		ArrayList<Integer> list = new ArrayList<Integer>();
		ArrayList<String> Sym = new ArrayList<String>();
		String id = null;
		String f_name = null;
		String l_name = null;
		String birth_date = null;
		Boolean bool = true;
		String regex = "[0-9.,-]+";
		// int i=0;

		for (int g = 0; g < len; g++) {

			if (mat[g][0] == null) {

				continue;
			}
			if (mat[g][0].contains("פרטים אישיים")) {
				list.add(g + 1);

			}

		}

		list.add(len - 1);
		int c = 1;
		for (int i = 0; i < list.size() - 1; i++) {
			if (i == 1052) {
				System.out.println("tfgd");
			}
			int start = list.get(i);
			int end = list.get(i + 1);
			id = mat[start][2];
			l_name = mat[start][4];
			f_name = mat[start][6];
			String load = "insert into " + name_schema + "." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,Symbol,SymbolName,m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12) values";
			int flag = 0;
			for (int j = start + 9; j < end; j++) {

				if (mat[j][2] == null) {
					continue;
				} else {

					String Symble = (mat[j][1] != null && mat[j][1] != " " && mat[j][1] != "" && !mat[j][1].isEmpty()
							&& !mat[j][1].contains("''")) ? mat[j][1] : "0";

					if (!Symble.matches(regex)) {
						Symble = "3000";
					}

					String SymbleName = (mat[j][2] != null) ? mat[j][2] : "0";
					if (Sym.contains(SymbleName)) {
						SymbleName += " " + " נוסף" + " " + " - " + " " + c++;
					}

					Sym.add(SymbleName);

					String m1 = sqlNumericEscape(mat[j][3]);
					String m2 = sqlNumericEscape(mat[j][4]);
					String m3 = sqlNumericEscape(mat[j][5]);
					String m4 = sqlNumericEscape(mat[j][6]);
					String m5 = sqlNumericEscape(mat[j][7]);
					String m6 = sqlNumericEscape(mat[j][8]);
					String m7 = sqlNumericEscape(mat[j][9]);
					String m8 = sqlNumericEscape(mat[j][10]);
					String m9 = sqlNumericEscape(mat[j][11]);
					String m10 = sqlNumericEscape(mat[j][12]);
					String m11 = sqlNumericEscape(mat[j][13]);
					String m12 = sqlNumericEscape(mat[j][14]);

					if (flag == 0) {

						load += "('" + escapeSQL(f_name) + " " + escapeSQL(l_name) + "'," + 159874875 + "," + year + ","
								+ id + ",'" + escapeSQL(Symble) + "','" + escapeSQL(SymbleName) + "','" + m1 + "','"
								+ m2 + "','" + m3 + "','" + m4 + "','" + m5 + "','" + m6 + "','" + m7 + "','" + m8
								+ "','" + m9 + "','" + m10 + "','" + m11 + "','" + m12 + "')";

						flag = 1;
					} else {

						load += ",('" + escapeSQL(f_name) + " " + escapeSQL(l_name) + "'," + 159874875 + "," + year
								+ "," + id + ",'" + escapeSQL(Symble) + "','" + escapeSQL(SymbleName) + "','" + m1
								+ "','" + m2 + "','" + m3 + "','" + m4 + "','" + m5 + "','" + m6 + "','" + m7 + "','"
								+ m8 + "','" + m9 + "','" + m10 + "','" + m11 + "','" + m12 + "')";

					}
				}

			}
			tr.Insertintodb1(load);
			c = 1;
			Sym.clear();

		}

		//
		//
		//
		// for (int i = 1; i < mat.length; i++) {
		//
		// if (mat[i][0] == null) {
		//
		// continue;
		// }
		//
		// if (mat[i][0].contains("פרטים אישיים")) {
		// i++;
		// id = mat[i][2];
		// l_name = mat[i][4];
		// f_name = mat[i][6];
		// birth_date = mat[i + 2][2];
		//
		// i += 6;
		//
		// bool = true;
		// }
		// if (mat[i][0].contains("כרטיס אישי לעובד")) {
		//
		// while (mat[i][0] != null && !mat[i][0].contains("פרטים אישיים")) {
		// if (bool == true) {
		// i += 3;
		// bool = false;
		// }
		// int j = 1;
		//
		// if (mat[i][j] == " " || mat[i][j] == "" || mat[i][j] == null ||
		// mat[i][j].isEmpty() || mat[i][j].contains("''")) {
		// mat[i][j] = "0";
		// }
		//
		// if (mat[i][j + 1] == null || mat[i][j + 1] == " " || mat[i][j + 1] ==
		// "" || mat[i][j + 1].isEmpty() || mat[i][j + 1].contains("''")) {
		// mat[i][j + 1] = "0";
		// }
		// String load = "insert into " + name_schema + "." + name_table + "\n"
		// + " (`FullName`,`cid`,`dyear`,`id`,Symbol,SymbolName) values( '"
		// + f_name + " " + l_name + "',159738264," + year + "," + id + "," +
		// escapeSQL(mat[i][j]) + ",'"
		// + escapeSQL(mat[i][j + 1]) + "')";
		//
		// tr.Insertintodb1(load);
		// load = "select MAX(m.in_id )from " + name_schema + " ." + name_table
		// + " m";
		// int s = tr.Readfromdb(load);
		//
		// j += 1;
		// for (int k = 1; k < 13; k++) {
		// if (mat[i][j + k] == null || mat[i][j + k].isEmpty() || "
		// ".equals(mat[i][j + k]) || mat[i][j + k].equals("")
		// || mat[i][j + k].isEmpty() || mat[i][j + k].contains("''")) {
		// mat[i][j + k] = "0";
		// }
		// mat[i][j + k] = mat[i][j + k].replace(",", "");
		//
		// load = "update " + name_schema + "." + name_table
		// + " set m" + k + "=" + mat[i][j + k] + " where in_id=" + s;
		//
		// tr.Insertintodb1(load);
		//
		// }
		//
		// i++;
		//
		// }
		//
		// }
		// bool = true;
		// }
	}

	public void create_symbels_numbers_and_insert_into_main_table_idit(String name_scema, String name_tabletemp,
			String name_table_101) throws SQLException {
		// String a = "CREATE TABLE if not exists `" + name_scema + "`.`" +
		// name_tabletemp + "` (\n"
		// + " `in_id` INT NOT NULL AUTO_INCREMENT,\n"
		// + " `symble` VARCHAR(200) NULL,\n"
		// + " PRIMARY KEY (`in_id`));";
		//
		// tr.Insertintodb1(a);
		String x = "TRUNCATE `" + name_scema + "`.`" + name_tabletemp + "`;";
		tr.Insertintodb1(x);
		String s = "ALTER TABLE `" + name_scema + "`.`" + name_tabletemp + "`  AUTO_INCREMENT = 2000";
		tr.Insertintodb1(s);
		//
		String j = "insert into " + name_scema + "." + name_tabletemp + "(Symble)  SELECT symbolName FROM " + name_scema
				+ "." + name_table_101 + " where Symbol=0 or Symbol=3000 group by symbolName;";
		tr.Insertintodb1(j);

		// insert = "UPDATE " + name_scema + "." + name_table_101 + " SET
		// Symbol=(\n"
		// + " SELECT in_id FROM " + name_scema + "." + name_tabletemp + " WHERE
		// " + name_tabletemp + ".symble = " + name_table_101 + ".SymbolName
		// COLLATE utf8_general_ci);";
		String insert = " update " + name_scema + "." + name_table_101 + " as t1," + name_scema + "." + name_tabletemp
				+ " as sy set t1.Symbol=sy.in_id where  \n" + "             t1.SymbolName=sy.Symble  \n"
				+ "  and( t1.Symbol=0 or t1.Symbol=3000)\n" + "   ;";
		tr.Insertintodb1(insert);

	}

	void idit_right_coulms_for_birth_date(String[][] mat, String name_schema, String name_table, int year, int len)
			throws SQLException {
		ArrayList<Integer> list = new ArrayList<Integer>();
		ArrayList<String> Sym = new ArrayList<String>();
		String id = null;
		String f_name = null;
		String l_name = null;
		String birth_date = null;
		Boolean bool = true;
		String regex = "[0-9.,-]+";
		// int i=0;

		for (int g = 0; g < len; g++) {

			if (mat[g][0] == null) {

				continue;
			}
			if (mat[g][0].contains("פרטים אישיים")) {
				list.add(g + 1);

			}

		}

		list.add(len - 1);

		for (int i = 0; i < list.size() - 1; i++) {

			int start = list.get(i);
			int end = list.get(i + 1);
			id = mat[start][2];
			l_name = mat[start][4];
			f_name = mat[start][6];
			birth_date = mat[start + 2][2];
			// 4/8/1949
			String dyear = null, month = null, day = null;
			dyear = birth_date.substring(birth_date.lastIndexOf("/") + 1);
			month = birth_date.substring(birth_date.indexOf("/") + 1, birth_date.lastIndexOf("/"));
			day = birth_date.substring(0, birth_date.indexOf("/"));
			String load = "insert into " + name_schema + "." + name_table + "\n"
					+ "            (`name`,`id`,`birthdate`,`year`) values";

			load += "('" + escapeSQL(f_name) + " " + escapeSQL(l_name) + "'," + id + ",concat(" + dyear + ",'-',"
					+ month + ",'-'," + day + ")," + year + ")";
			tr.Insertintodb1(load);
		}

	}

	/// create_table_oketz/
	//
	//
	// for (int i = 1; i < mat.length; i++) {
	//
	// if (mat[i][0] == null) {
	//
	// continue;
	// }
	//
	// if (mat[i][0].contains("פרטים אישיים")) {
	// i++;
	// id = mat[i][2];
	// l_name = mat[i][4];
	// f_name = mat[i][6];
	// birth_date = mat[i + 2][2];
	//
	// i += 6;
	//
	// bool = true;
	// }
	// if (mat[i][0].contains("כרטיס אישי לעובד")) {
	//
	// while (mat[i][0] != null && !mat[i][0].contains("פרטים אישיים")) {
	// if (bool == true) {
	// i += 3;
	// bool = false;
	// }
	// int j = 1;
	//
	// if (mat[i][j] == " " || mat[i][j] == "" || mat[i][j] == null ||
	/// mat[i][j].isEmpty() || mat[i][j].contains("''")) {
	// mat[i][j] = "0";
	// }
	//
	// if (mat[i][j + 1] == null || mat[i][j + 1] == " " || mat[i][j + 1] == ""
	/// || mat[i][j + 1].isEmpty() || mat[i][j + 1].contains("''")) {
	// mat[i][j + 1] = "0";
	// }
	// String load = "insert into " + name_schema + "." + name_table + "\n"
	// + " (`FullName`,`cid`,`dyear`,`id`,Symbol,SymbolName) values( '"
	// + f_name + " " + l_name + "',159738264," + year + "," + id + "," +
	/// escapeSQL(mat[i][j]) + ",'"
	// + escapeSQL(mat[i][j + 1]) + "')";
	//
	// tr.Insertintodb1(load);
	// load = "select MAX(m.in_id )from " + name_schema + " ." + name_table + "
	/// m";
	// int s = tr.Readfromdb(load);
	//
	// j += 1;
	// for (int k = 1; k < 13; k++) {
	// if (mat[i][j + k] == null || mat[i][j + k].isEmpty() || "
	/// ".equals(mat[i][j + k]) || mat[i][j + k].equals("")
	// || mat[i][j + k].isEmpty() || mat[i][j + k].contains("''")) {
	// mat[i][j + k] = "0";
	// }
	// mat[i][j + k] = mat[i][j + k].replace(",", "");
	//
	// load = "update " + name_schema + "." + name_table
	// + " set m" + k + "=" + mat[i][j + k] + " where in_id=" + s;
	//
	// tr.Insertintodb1(load);
	//
	// }
	//
	// i++;
	//
	// }
	//
	// }
	// bool = true;
	// }

	void truncate_load_data_tashlumim(String name_scema, String name_table) throws SQLException {
		String a = "    TRUNCATE `" + name_scema + "`.`" + name_table + "`";
		tr.Insertintodb1(a);
	}

	void insert_into_101_isrotel(String name_scema, String name_table_101, String name_table_temp) throws SQLException {
		String a = "INSERT INTO  `" + name_scema + "`.`" + name_table_101 + "`\n" + "(\n" + "`cid`,\n" + "`dyear`,\n"
				+ "`id`,\n" +

				"`FullName`,\n" + "`Symbol`,\n" + "`SymbolName`,\n" + "`m1`,\n" + "`m2`,\n" + "`m3`,\n" + "`m4`,\n"
				+ "`m5`,\n" + "`m6`,\n" + "`m7`,\n" + "`m8`,\n" + "`m9`,\n" + "`m10`,\n" + "`m11`,\n" + "`m12`)\n" +

				"SELECT \n" + "  `cid`\n" + "  ,`dyear`,\n" + "    `id`,\n" + "    `FullName`,\n" + "  `Symbol`,\n"
				+ "   `SymbolName`,\n"
				+ "sum(m1) as m1, sum(m2) as m2, sum(m3) as m3, sum(m4) as m4, sum(m5) as m5, sum(m6) as m6, sum(m7) as m7, sum(m8) as m8,\n"
				+ "sum(m9) as m9, sum(m10) as m10, sum(m11) as m11, sum(m12) as m12\n" + "FROM  `" + name_scema + "`.`"
				+ name_table_temp + "`" + "group by  cid,  dyear, id, Symbol, SymbolName \n";

		tr.Insertintodb1(a);

	}

	void update_cid_101_isrotel(int cid, String table) throws SQLException {
		String a = "update Upload_file." + table + "\n" + " set cid=" + cid
				+ " where substr(num_oved,1,1) = 5 or substr(num_oved,1,1) = 1";
		tr.Insertintodb1(a);

	}

	public void load_data_details(String name_tabel) throws SQLException {
		// name_tabel = name_tabel;

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/spak_details.csv'"
				+ " INTO TABLE `Upload_file`." + name_tabel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 7 LINES "
				+ "  ( num_oved, id, l_name, f_name, machlaka, birth_date, age, min, `m.m`, city, street, house, mikod, `t.d`, telephone, bank)";

		tr.Insertintodb1(a);

	}

	public void create_table_fanitzia(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists  " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n"
				+ "  `F_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `L_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `num_oved` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `id` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `seif_taktzivi` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `Symbol` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `Symbol_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m1_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m1_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m2_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m2_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m3_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m3_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m4_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m4_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m5_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m5_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m6_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m6_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m7_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m7_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m8_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m8_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m9_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m9_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m10_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m10_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m11_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m11_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m12_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m12_c` varchar(45) CHARACTER SET utf8 DEFAULT NULL,\n"
				+ "  `dyear` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `cid` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" + "  PRIMARY KEY (`in_id`)\n" + ") ";
		tr.Insertintodb1(a);

	}

	public void create_table_fanitziaa(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists  " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n"
				+ "  `F_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `L_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `num_oved` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `id` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `seif_taktzivi` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `Symbol` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `Symbol_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m1` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m11` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m111` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m1_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m1_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m2_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m2_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m3_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m3_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m4_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m4_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m5_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m5_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m6_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m6_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m7_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m7_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m8_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m8_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m9_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m9_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m10_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m10_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m11_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m11_c` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m12_s` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m12_c` varchar(45) CHARACTER SET utf8 DEFAULT NULL,\n"
				+ "  `dyear` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `cid` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" + "  PRIMARY KEY (`in_id`)\n" + ") ";
		tr.Insertintodb1(a);

	}

	public void Malam_create_table_get_the_right_coulms_isrotel(String name_schema, String name_table)
			throws SQLException {

		String a = "CREATE TABLE if not exists `" + name_schema + "`.`" + name_table + "`\n" + "(\n"
				+ "`in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "`cid` varchar(45)  DEFAULT NULL,\n"
				+ "`dyear` varchar(45)  DEFAULT NULL,\n" + "`id` varchar(45)  DEFAULT NULL,\n"
				+ "`FullName` varchar(200)  DEFAULT NULL,\n" + "`Symbol` varchar(45)  DEFAULT NULL,\n"
				+ "`SymbolName` varchar(200)  DEFAULT NULL,\n" + "`m1` varchar(45)  DEFAULT NULL,\n"
				+ "`m2` varchar(45)  DEFAULT NULL,\n" + "`m3` varchar(45)  DEFAULT NULL,\n"
				+ "`m4` varchar(45)  DEFAULT NULL,\n" + "`m5` varchar(45)  DEFAULT NULL,\n"
				+ "`m6` varchar(45)  DEFAULT NULL,\n" + "`m7` varchar(45)  DEFAULT NULL,\n"
				+ "`m8` varchar(45)  DEFAULT NULL,\n" + "`m9` varchar(45)  DEFAULT NULL,\n"
				+ "`m10` varchar(45)  DEFAULT NULL,\n" + "`m11` varchar(45)  DEFAULT NULL,\n"
				+ "`m12` varchar(45)  DEFAULT NULL,\n" + "PRIMARY KEY (`in_id`)\n" + ") ;";
		tr.Insertintodb1(a);
	}

	public void create_101_malam(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "  `cid` int(45) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(45) NOT NULL,\n" + "  `id` int(45) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n" + "  `FullName` varchar(200) NOT NULL,\n"
				+ "  `Symbol` int(45) NOT NULL,\n" + "  `SymbolName` varchar(100) NOT NULL,\n"
				+ "  `m1` double NOT NULL DEFAULT NULL ,\n" + "  `m2` double NOT NULL DEFAULT NULL ,\n"
				+ "  `m3` double NOT NULL DEFAULT NULL ,\n" + "  `m4` double NOT NULL DEFAULT NULL ,\n"
				+ "  `m5` double NOT NULL DEFAULT NULL ,\n" + "  `m6` double NOT NULL DEFAULT NULL ,\n"
				+ "  `m7` double NOT NULL DEFAULT NULL ,\n" + "  `m8` double NOT NULL DEFAULT NULL ,\n"
				+ "  `m9` double NOT NULL DEFAULT NULL  ,\n" + "  `m10` double NOT NULL DEFAULT NULL ,\n"
				+ "  `m11` double NOT NULL DEFAULT NULL ,\n" + "  `m12` double NOT NULL DEFAULT NULL ,\n"
				+ "  `total` double NOT NULL DEFAULT NULL ,\n"
				+ "  `division` bigint(20) unsigned NOT NULL DEFAULT '0',\n" + "  `run_version` int(11) DEFAULT NULL,\n"
				+ "  `date_value` varchar(50) DEFAULT NULL,\n" + "  `source` varchar(100) DEFAULT NULL,\n"
				+ "  `type` varchar(400) DEFAULT NULL,\n" + "  `num_worker` int(11) unsigned DEFAULT '0',\n"
				+ "  `permission` int(11) unsigned DEFAULT NULL,\n" + "  `type_for_gemel` varchar(400) DEFAULT NULL,\n"
				+ "  PRIMARY KEY (`in_id`),\n"
				+ "  UNIQUE KEY `ind_unique` (`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,`division`) USING BTREE,\n"
				+ "  KEY `indi3` (`cid`,`dyear`,`Symbol`,`SymbolName`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";
		tr.Insertintodb1(a);

	}

	public void create_101_bituchim(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists   `" + name_schema + "`.`" + name_table + "`\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n" + "  `id` int(11) NOT NULL,\n"
				+ "  `num_polisa` varchar(45) DEFAULT NULL,\n" + "  `FullName` varchar(51) NOT NULL,\n"
				+ "  `motzar` varchar(100) DEFAULT '0',\n" + "  `sug_hafrasha` varchar(100) DEFAULT '0',\n"
				+ "  `m1` double NOT NULL DEFAULT '0',\n" + "  `m2` double NOT NULL DEFAULT '0',\n"
				+ "  `m3` double NOT NULL DEFAULT '0',\n" + "  `m4` double NOT NULL DEFAULT '0',\n"
				+ "  `m5` double NOT NULL DEFAULT '0',\n" + "  `m6` double NOT NULL DEFAULT '0',\n"
				+ "  `m7` double NOT NULL DEFAULT '0',\n" + "  `m8` double NOT NULL DEFAULT '0',\n"
				+ "  `m9` double NOT NULL DEFAULT '0',\n" + "  `m10` double NOT NULL DEFAULT '0',\n"
				+ "  `m11` double NOT NULL DEFAULT '0',\n" + "  `m12` double NOT NULL DEFAULT '0',\n"
				+ "  `total` double NOT NULL DEFAULT '0',\n" + "  `division` varchar(100) NOT NULL DEFAULT '0',\n"
				+ "  `run_version` int(11) DEFAULT NULL,\n" + "  `date_value` varchar(50) DEFAULT NULL,\n"
				+ "  `source` varchar(100) DEFAULT NULL,\n" + "  `type` varchar(400) DEFAULT NULL,\n"
				+ "  `num_worker` int(11) unsigned DEFAULT '0',\n" + "  `permission` int(11) unsigned DEFAULT NULL,\n"
				+ "  `type_for_gemel` varchar(400) DEFAULT NULL,\n" + "  PRIMARY KEY (`in_id`),\n"
				+ "  UNIQUE KEY `unik` (`cid`,`dyear`,`id`,`num_polisa`,`motzar`,`sug_hafrasha`,`division`),\n"
				+ "  KEY `indesx2` (`cid`,`dyear`,`id`,`sug_hafrasha`,`motzar`),\n"
				+ "  KEY `index3` (`cid`,`dyear`,`id`,`sug_hafrasha`,`num_polisa`),\n"
				+ "  KEY `index4` (`cid`,`dyear`,`division`,`id`,`sug_hafrasha`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";
		tr.Insertintodb1(a);

	}

	public void insert_mavid() throws SQLException {

		String a = "INSERT INTO `naomi`.`tbl_101_bituhim`\n" + "(`cid`,\n" + "`dyear`,\n" + "`id`,\n" + "`FullName`,\n"
				+ "`motzar`,\n" + "`sug_hafrasha`,\n" + "`m1`,\n" + "`m2`,\n" + "`m3`,\n" + "`m4`,\n" + "`m5`,\n"
				+ "`m6`,\n" + "`m7`,\n" + "`m8`,\n" + "`m9`,\n" + "`m10`,\n" + "`m11`,\n" + "`m12`,\n" + "`total`,\n"
				+ "`division`,\n" + "`run_version`,\n" + "`date_value`,\n" + "`source`,\n" + "`type`,\n"
				+ "`num_worker`,\n" + "`permission`,\n" + "`type_for_gemel`)\n" + "SELECT \n" + "'430',\n"
				+ "    `tbl_101`.`dyear`,\n" + "    `tbl_101`.`id`,\n" + "    `tbl_101`.`FullName`,\n"
				+ "    `tbl_101`.`SymbolName`,\n" + "    'מעביד',\n" + "    `tbl_101`.`m1`,\n" + "    `tbl_101`.`m2`,\n"
				+ "    `tbl_101`.`m3`,\n" + "    `tbl_101`.`m4`,\n" + "    `tbl_101`.`m5`,\n" + "    `tbl_101`.`m6`,\n"
				+ "    `tbl_101`.`m7`,\n" + "    `tbl_101`.`m8`,\n" + "    `tbl_101`.`m9`,\n" + "    `tbl_101`.`m10`,\n"
				+ "    `tbl_101`.`m11`,\n" + "    `tbl_101`.`m12`,\n" + "    `tbl_101`.`total`,\n" + "    'טבלה 101',\n"
				+ "    `tbl_101`.`run_version`,\n" + "    `tbl_101`.`date_value`,\n" + "    `tbl_101`.`source`,\n"
				+ "    `tbl_101`.`type`,\n" + "    `tbl_101`.`num_worker`,\n" + "    `tbl_101`.`permission`,\n"
				+ "    `tbl_101`.`type_for_gemel`\n" + "FROM `diff_taxes_reports`.`tbl_101` \n"
				+ "where cid=430  and dyear=2014  and SymbolName like  '%השתתפות%'  \n"
				+ "and SymbolName <> 'השתתפות: \"ק\"\"ג ד-פיצויים\"'  and SymbolName <> 'השתתפות: \"ק\"\"ג א-פיצויים\"'  and SymbolName <> 'השתתפות: \"ק\"\"ג ב-פיצויים\"\n"
				+ "'  and SymbolName <>  'השתתפות: \"ק\"\"ג ג-פיצויים\"'   and SymbolName <> 'השתתפות: \"ק\"\"ג ב-פיצויים\"'  and SymbolName <> 'השתתפות: ביטוח לאומי' \n"
				+ "";
		tr.Insertintodb1(a);

	}

	public void insert_oved() throws SQLException {

		String a = "INSERT INTO `naomi`.`tbl_101_bituhim`\n" + "(`cid`,\n" + "`dyear`,\n" + "`id`,\n" + "`FullName`,\n"
				+ "`motzar`,\n" + "`sug_hafrasha`,\n" + "`m1`,\n" + "`m2`,\n" + "`m3`,\n" + "`m4`,\n" + "`m5`,\n"
				+ "`m6`,\n" + "`m7`,\n" + "`m8`,\n" + "`m9`,\n" + "`m10`,\n" + "`m11`,\n" + "`m12`,\n" + "`total`,\n"
				+ "`division`,\n" + "`run_version`,\n" + "`date_value`,\n" + "`source`,\n" + "`type`,\n"
				+ "`num_worker`,\n" + "`permission`,\n" + "`type_for_gemel`)\n" + "SELECT \n" + "'430',\n"
				+ "    `tbl_101`.`dyear`,\n" + "    `tbl_101`.`id`,\n" + "    `tbl_101`.`FullName`,\n"
				+ "    `tbl_101`.`SymbolName`,\n" + "    'עובד',\n" + "    `tbl_101`.`m1`,\n" + "    `tbl_101`.`m2`,\n"
				+ "    `tbl_101`.`m3`,\n" + "    `tbl_101`.`m4`,\n" + "    `tbl_101`.`m5`,\n" + "    `tbl_101`.`m6`,\n"
				+ "    `tbl_101`.`m7`,\n" + "    `tbl_101`.`m8`,\n" + "    `tbl_101`.`m9`,\n" + "    `tbl_101`.`m10`,\n"
				+ "    `tbl_101`.`m11`,\n" + "    `tbl_101`.`m12`,\n" + "    `tbl_101`.`total`,\n" + "    'טבלה 101',\n"
				+ "    `tbl_101`.`run_version`,\n" + "    `tbl_101`.`date_value`,\n" + "    `tbl_101`.`source`,\n"
				+ "    `tbl_101`.`type`,\n" + "    `tbl_101`.`num_worker`,\n" + "    `tbl_101`.`permission`,\n"
				+ "    `tbl_101`.`type_for_gemel`\n" + "FROM `diff_taxes_reports`.`tbl_101`  \n"
				+ " where cid=430  and dyear=2014  and SymbolName like '%נכוי%'\n"
				+ "   and ( SymbolName like 'נכויי חובה: ק. גמל 2/ ביטוח'   or SymbolName like 'נכויי חובה: ק. גמל 1/ ביטוח'  or SymbolName like  'נכויי חובה: קרן השתלמות' \n"
				+ "   or  SymbolName like 'נכויי חובה: \"ק\"\"ג ג-נכוי\"'   or SymbolName like  'נכויי חובה: \"ק\"\"ג ד-נכוי\"'   or SymbolName like 'נכויי חובה: \"ק\"\"ג א-אכע -עובד\"')\n"
				+ "";
		tr.Insertintodb1(a);

	}

	public void insert_pitzoyim() throws SQLException {
		String a = "INSERT INTO `naomi`.`tbl_101_bituhim`\n" + "(`cid`,\n" + "`dyear`,\n" + "`id`,\n" + "`FullName`,\n"
				+ "`motzar`,\n" + "`sug_hafrasha`,\n" + "`m1`,\n" + "`m2`,\n" + "`m3`,\n" + "`m4`,\n" + "`m5`,\n"
				+ "`m6`,\n" + "`m7`,\n" + "`m8`,\n" + "`m9`,\n" + "`m10`,\n" + "`m11`,\n" + "`m12`,\n" + "`total`,\n"
				+ "`division`,\n" + "`run_version`,\n" + "`date_value`,\n" + "`source`,\n" + "`type`,\n"
				+ "`num_worker`,\n" + "`permission`,\n" + "`type_for_gemel`)\n" + "SELECT \n" + "'430',\n"
				+ "    `tbl_101`.`dyear`,\n" + "    `tbl_101`.`id`,\n" + "    `tbl_101`.`FullName`,\n"
				+ "    `tbl_101`.`SymbolName`,\n" + "    'פיצויים',\n" + "    `tbl_101`.`m1`,\n"
				+ "    `tbl_101`.`m2`,\n" + "    `tbl_101`.`m3`,\n" + "    `tbl_101`.`m4`,\n" + "    `tbl_101`.`m5`,\n"
				+ "    `tbl_101`.`m6`,\n" + "    `tbl_101`.`m7`,\n" + "    `tbl_101`.`m8`,\n" + "    `tbl_101`.`m9`,\n"
				+ "    `tbl_101`.`m10`,\n" + "    `tbl_101`.`m11`,\n" + "    `tbl_101`.`m12`,\n"
				+ "    `tbl_101`.`total`,\n" + "    'טבלה 101',\n" + "    `tbl_101`.`run_version`,\n"
				+ "    `tbl_101`.`date_value`,\n" + "    `tbl_101`.`source`,\n" + "    `tbl_101`.`type`,\n"
				+ "    `tbl_101`.`num_worker`,\n" + "    `tbl_101`.`permission`,\n" + "    `tbl_101`.`type_for_gemel`\n"
				+ "FROM `diff_taxes_reports`.`tbl_101`  \n"
				+ " where cid=430  and dyear=2014  and SymbolName like  '%פיצ%' ";
		tr.Insertintodb1(a);

	}

	public void insert() {
		for (int i = 1; i < 12; i++) {

			String a = "insert  into naomi.tbl_101_bituhim\n"
					+ "                              (cid, dyear,id, num_polisa, FullName, sug_hafrasha, m1, division)\n"
					+ "SELECT  '430',\n" + "     SUBSTRING(hodesh_tokef,-4,4)  as dyear,\n"
					+ "     `ikea_bituah`.`tz`,\n" + "     `ikea_bituah`.`num_polisa`,\n"
					+ "     `ikea_bituah`.`shem_mevoutah`,\n" + "     'עובד',\n"
					+ "     sum(`ikea_bituah`.`tagmulim_oved`),\n" + "     'אקסל'\n"
					+ "FROM `naomi`.`ikea_bituah` where SUBSTRING(hodesh_tokef,-7,2)=01  and SUBSTRING(hodesh_tokef,-4,4)=2014\n"
					+ "and  `ikea_bituah`.`tagmulim_oved` <>0  group by num_polisa;";
		}

	}

	void create_table_shiklolit_tashlumim_like_101(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE  if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n" + "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n" + "  `FullName` varchar(200) NOT NULL,\n"
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

				+ "  PRIMARY KEY (`in_id`)\n"

				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";
		tr.Insertintodb1(a);

	}

	public void create_ikea(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists `" + name_schema + "`." + name_table + " (\n"
				+ "  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n"
				+ "  `sicomim` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `mis_oved` int(11) DEFAULT NULL,\n"
				+ "  `l_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `f_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `sog_naton` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `symbol` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `symbolname` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m12` int(11) DEFAULT NULL,\n" + "  `m11` int(11) DEFAULT NULL,\n"
				+ "  `m10` int(11) DEFAULT NULL,\n" + "  `m9` int(11) DEFAULT NULL,\n"
				+ "  `m8` int(11) DEFAULT NULL,\n" + "  `m7` int(11) DEFAULT NULL,\n" + "  `m6` int(11) DEFAULT NULL,\n"
				+ "  `m5` int(11) DEFAULT NULL,\n" + "  `m4` int(11) DEFAULT NULL,\n" + "  `m3` int(11) DEFAULT NULL,\n"
				+ "  `m2` int(11) DEFAULT NULL,\n" + "  `m1` int(11) DEFAULT NULL,\n"
				+ "  `avg` int(11) DEFAULT NULL,\n" + "  `mitzber` int(11) DEFAULT NULL,\n"
				+ "  `dyear` int(11) DEFAULT NULL,\n" + "  PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";

		tr.Insertintodb1(a);

	}

	void load_data_ikea(String name_schema, String name_table, int year) throws SQLException {
		for (int i = 1; i < 4; i++) {

			String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/ikea/2015/" + i + ".csv'"
					+ " INTO TABLE `" + name_schema + "`." + name_table
					+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
					+ "  (sicomim, mis_oved, l_name, f_name, sog_naton, symbol, symbolname, m12, m11, m10, m9, m8, m7, m6, m5, m4, m3, m2, m1, avg, mitzber)"
					+ "set dyear=" + year;
			tr.Insertintodb1(a);

		}

	}

	public void create_101_micpal_ikea(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists " + name_schema + ".`" + name_table + "` (\n"
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
				+ "  PRIMARY KEY (`in_id`)\n"

				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";
		tr.Insertintodb1(a);

	}

	public void convert_101(String name_schema, String name_table, String name_table_101) throws SQLException {
		for (int i = 1; i < 13; i++) {

			String a = "insert into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,Symbol,`SymbolName`,m" + i + ",`source`  )\n"
					+ "            SELECT   concat(`f_name`,' ',`l_name`),430,`dyear`,`mis_oved`,`Symbol`, concat(`sog_naton`,' : ',`symbolName`),`m"
					+ i + "` , `sog_naton` \n" + "            FROM  Upload_file. " + name_table + "\n"
					+ "           on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
	}

	void delete_exstra_rows(String name_schema, String name_table) throws SQLException {
		String a = "delete from " + name_schema + "." + name_table + " where symbolname='' ";
		tr.Insertintodb1(a);
	}

	public void create_101_ikea(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n" + "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n" + "  `FullName` varchar(51) NOT NULL,\n"
				+ "  `Symbol` varchar(51) NOT NULL,\n" + "  `SymbolName` varchar(100) NOT NULL,\n"
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

	}

	public void create_table_ikea_bituchim1(String name_schema, String name_table) throws SQLException {
		// /home/shoshana/Downloads/onger_fisher

		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `sicomim` VARCHAR(45) NULL,\n"
				+ "  `mis_oved` VARCHAR(45) NULL,\n" + "  `l_name` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(45) NULL,\n" + "  `id`  VARCHAR(45)  NULL ,\n" + "  `vetek` VARCHAR(45) NULL,\n"
				+ "  `num_sochen` VARCHAR(45) NULL,\n" + "  `name_sochen` VARCHAR(45) NULL,\n"
				+ "  `mis_otzar` VARCHAR(45) NULL,\n" + "  `kod_keren` VARCHAR(45) NULL,\n"
				+ "  `mis_keren` VARCHAR(45) NULL,\n" + "  `name_keren` VARCHAR(45) NULL,\n"
				+ "  `mascoret` VARCHAR(45) NULL,\n" + "  `nicoy_oved` VARCHAR(45) NULL,\n"
				+ "  `hishtatfut_mavid` VARCHAR(45) NULL,\n" + "  `total` VARCHAR(45) NULL,\n"
				+ "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(a);
	}

	public void create_table_ikea_bituchim2(String name_schema, String name_table) throws SQLException {
		// /home/shoshana/Downloads/onger_fisher

		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `sicomim` VARCHAR(45) NULL,\n"
				+ "  `mis_oved` VARCHAR(45) NULL,\n" + "  `l_name` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(45) NULL,\n" + "  `id`  VARCHAR(45)  NULL ,\n"
				+ "  `mis_otzar`  VARCHAR(45)  NULL ,\n" + "  `num_c_p`  VARCHAR(45)  NULL ,\n"
				+ "  `kod_kupa`  VARCHAR(45)  NULL ,\n" + "  `mis_kupa`  VARCHAR(45)  NULL ,\n"
				+ "  `name_kupa`  VARCHAR(45)  NULL ,\n" + "  `mascoret` VARCHAR(45) NULL,\n"
				+ "  `nicoy_oved` VARCHAR(45) NULL,\n" + "  `a_c_a_oved` VARCHAR(45) NULL,\n"
				+ "  `tagmulim_mavid` VARCHAR(45) NULL,\n" + "  `pitzuim_mavid` VARCHAR(45) NULL,\n"
				+ "  `a_c_a_mavid` VARCHAR(45) NULL,\n" + "  `total` VARCHAR(45) NULL,\n"
				+ "  `hafrasha_odefet` VARCHAR(45) NULL,\n" + "  `achuz_oved` VARCHAR(45) NULL,\n"
				+ "  `achuz_mavid` VARCHAR(45) NULL,\n" + "  `achuz_a_c_a_mavid` VARCHAR(45) NULL,\n"
				+ "  `achuz_pitzuim` VARCHAR(45) NULL,\n" + "  `chodesh_yicos` VARCHAR(45) NULL,\n"
				+ "  `shnat_yicos` VARCHAR(45) NULL,\n" +

				"  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(a);
	}

	public void load_data_ikeaa(String name_schema, String name_tabel, String name_file, int year) throws SQLException {
		// name_tabel = name_tabel;

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/onger_fisher/" + name_file + ".csv'"
				+ " INTO TABLE `" + name_schema + "`." + name_tabel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  "
				+ " (  sicomim, mis_oved, l_name, f_name, id, mis_otzar, num_c_p, kod_kupa, mis_kupa, name_kupa, mascoret, nicoy_oved, a_c_a_oved, tagmulim_mavid, pitzuim_mavid, a_c_a_mavid, total, hafrasha_odefet, achuz_oved, achuz_mavid, achuz_a_c_a_mavid, achuz_pitzuim, chodesh_yicos, shnat_yicos, dyear)"
				+ "set dyear=" + year;
		// in_id, name_company, tax_year, id_Worker, full_name, m, symbol,
		// symbolName, value, sicom_camot, dyear, name_machlaka
		tr.Insertintodb1(a);

	}

	public void insert_shir_hafrasha_ikea() throws SQLException {
		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat('עובד',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat('עובד',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat('מעביד',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat('מעביד',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat('פיצוים',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='פיצוים'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat('פיצוים',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='פיצוים'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat( 'ע עובד.כ.א',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='ע עובד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat('ע עובד.כ.א',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='ע עובד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,555,concat( 'ע מעביד.כ.א',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='ע מעביד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,555,concat('ע מעביד.כ.א',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='ע מעביד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,444,concat( 'שונות עובד',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='שונות עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,444,concat('שונות עובד',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='שונות עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,333,concat( 'שונות מעביד',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='שונות מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,333,concat('שונות מעביד',' : ','אחוז הפרשה',' : ',num_file),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='שונות מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

	}

	public void insert_salary_ikea() throws SQLException {
		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat('עובד',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat('עובד',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat('מעביד',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat('מעביד',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat('פיצוים',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='פיצוים'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat('פיצוים',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='פיצוים'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat( 'ע עובד.כ.א',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='ע עובד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat('ע עובד.כ.א',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='ע עובד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,555,concat( 'ע מעביד.כ.א',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='ע מעביד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,555,concat('ע מעביד.כ.א',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='ע מעביד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,444,concat( 'שונות עובד',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='שונות עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,444,concat('שונות עובד',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='שונות עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,333,concat( 'שונות מעביד',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='שונות מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,333,concat('שונות מעביד',' : ','משכורת',' : ',num_file),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='שונות מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

	}

	public void insert_hafrash_codshit_ikea() throws SQLException {
		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat('עובד',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat('עובד',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat('מעביד',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat('מעביד',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat('פיצוים',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='פיצוים'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat('פיצוים',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='פיצוים'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat( 'ע עובד.כ.א',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='ע עובד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat('ע עובד.כ.א',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='ע עובד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,555,concat( 'ע מעביד.כ.א',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='ע מעביד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,555,concat('ע מעביד.כ.א',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='ע מעביד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,444,concat( 'שונות עובד',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='שונות עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,444,concat('שונות עובד',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='שונות עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,333,concat( 'שונות מעביד',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='שונות מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,333,concat('שונות מעביד',' : ','הפרשה חודשית',' : ',num_file),\n"
					+ " `hafrash_codshit` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='שונות מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

	}

	public void insert_scum_pitzulim_ikea() throws SQLException {
		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat('עובד',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat('עובד',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat('מעביד',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat('מעביד',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat('פיצוים',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='פיצוים'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat('פיצוים',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='פיצוים'\n" + "  on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat( 'ע עובד.כ.א',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='ע עובד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat('ע עובד.כ.א',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='ע עובד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,555,concat( 'ע מעביד.כ.א',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='ע מעביד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,555,concat('ע מעביד.כ.א',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='ע מעביד.כ.א'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,444,concat( 'שונות עובד',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='שונות עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,444,concat('שונות עובד',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='שונות עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

		for (int i = 1; i < 10; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,333,concat( 'שונות מעביד',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='שונות מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}
		for (int i = 10; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,333,concat('שונות מעביד',' : ','סכום פיצולים',' : ',num_file),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea  as a   where chodesh=" + i
					+ " and sug_hafrasha_desc='שונות מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(a);
		}

	}

	public void insert_xml_ikea_into_101() throws SQLException {

		for (int i = 2; i < 9; i++) {

			String a = "insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat(name_kupa,' : ',sug_hafrasha_desc,' : ','משכורת'),`salary` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + "  and sug_hafrasha_desc='עובד'\n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(a);
			String b = "  insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','משכורת'),`salary` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + " and sug_hafrasha_desc='מעביד'\n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(b);
			String c = "insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','משכורת'),`salary` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + " and sug_hafrasha_desc='פיצוים'\n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(c);
			String d = " insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','משכורת'),`salary` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='א.כ.ע עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ")";
			tr.Insertintodb1(d);
			String e = "   insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,999,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','משכורת'),`salary` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='א.כ.ע מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ")";

			tr.Insertintodb1(e);
			String f = " insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','הפרשה חודשית'),`hafrash_codshit` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + " and sug_hafrasha_desc='עובד' \n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(f);
			String g = "  insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','הפרשה חודשית'),`hafrash_codshit` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + " and sug_hafrasha_desc='מעביד'\n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(g);
			String h = "    insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','הפרשה חודשית'),`hafrash_codshit` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + " and sug_hafrasha_desc='פיצוים'\n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(h);

			String z = " insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','הפרשה חודשית'),`hafrash_codshit` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='א.כ.ע עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ")";
			tr.Insertintodb1(z);

			String j = "  insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,888,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','הפרשה חודשית'),`hafrash_codshit` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='א.כ.ע מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ")";
			tr.Insertintodb1(j);
			String k = "insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','סכום פיצולים'),`scum_pitzulim` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + " and sug_hafrasha_desc='עובד'\n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(k);
			String l = "  insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','סכום פיצולים'),`scum_pitzulim` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + " and sug_hafrasha_desc='מעביד'\n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(l);
			String m = "    insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','סכום פיצולים'),`scum_pitzulim` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + " and sug_hafrasha_desc='פיצוים'\n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(m);
			String n = " insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','סכום פיצולים'),`scum_pitzulim` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a  where chodesh=0" + i
					+ " and sug_hafrasha_desc='א.כ.ע עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ")";
			tr.Insertintodb1(n);
			String p = "   insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,777,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','סכום פיצולים'),`scum_pitzulim` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='א.כ.ע מעביד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ")";
			tr.Insertintodb1(p);
			String q = "insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','שעור הפרשה'),`shiur_hafrasha` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + " and sug_hafrasha_desc='עובד'\n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(q);
			String r = "  insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','שעור הפרשה'),`shiur_hafrasha` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + " and sug_hafrasha_desc='מעביד'\n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(r);
			String s = "    insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','שעור הפרשה'),`shiur_hafrasha` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i + " and sug_hafrasha_desc='פיצוים'\n"
					+ "  on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(s);
			String t = "  insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','שעור הפרשה'),`shiur_hafrasha` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a  where chodesh=0" + i
					+ " and sug_hafrasha_desc='א.כ.ע עובד'\n" + "  on duplicate key update m" + i + "=values(m" + i
					+ ")";
			tr.Insertintodb1(t);
			String u = "   insert into shoshana.tbl_101_ikea_xml_08_16 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,666,concat(name_kupa,' : ',sug_hafrasha_desc,' : ',num_file,' : ','שעור הפרשה'),`shiur_hafrasha` ,`num_file` \n"
					+ " FROM shoshana.tbl_sum_fisher  as a   where chodesh=0" + i
					+ " and sug_hafrasha_desc='א.כ.ע מעביד' " + " on duplicate key update m" + i + "=values(m" + i
					+ ");";
			tr.Insertintodb1(u);
		}

	}

	public void create_table_avidar1(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(60) NULL,\n" + "  `l_name` VARCHAR(60) NULL,\n"
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
				+ "  `sub` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

	}

	public void create_table_avidar2(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(60) NULL,\n" + "  `l_name` VARCHAR(60) NULL,\n"
				+ "  `tat_mifal` VARCHAR(60) NULL,\n" + "  `agaf` VARCHAR(45) NULL,\n"
				+ "  `machlaka` VARCHAR(45) NULL,\n" + "  `name_machlaka` VARCHAR(45) NULL,\n"
				+ "  `derog` VARCHAR(45) NULL,\n" + "  `tear_derog` VARCHAR(45) NULL,\n"
				+ "  `darga` VARCHAR(45) NULL,\n" + "  `teor_darga` VARCHAR(45) NULL,\n"
				+ "  `tchilat_avoda` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `kod_esok` VARCHAR(45) NULL,\n" + "  `tear_isok` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(60) NULL,\n" +

				"  `m2` VARCHAR(45) NULL,\n" + "  `m3` VARCHAR(45) NULL,\n" + "  `m4` VARCHAR(45) NULL,\n"
				+ "  `m5` VARCHAR(45) NULL,\n" + "  `m6` VARCHAR(45) NULL,\n" + "  `m7` VARCHAR(45) NULL,\n"
				+ "  `m8` VARCHAR(45) NULL,\n" + "  `m9` VARCHAR(45) NULL,\n" + "  `m10` VARCHAR(45) NULL,\n"
				+ "  `m11` VARCHAR(45) NULL,\n" + "  `m12` VARCHAR(45) NULL,\n" + "  `m1` VARCHAR(45) NULL,\n"
				+ "  `total` VARCHAR(45) NULL,\n" + "  `avg` VARCHAR(45) NULL,\n" + "  `avg_p` VARCHAR(45) NULL,\n"
				+ "  `sub` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

	}

	public void create_table_avidar3(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(60) NULL,\n" + "  `l_name` VARCHAR(60) NULL,\n"
				+ "  `tat_mifal` VARCHAR(60) NULL,\n" + "  `agaf` VARCHAR(45) NULL,\n"
				+ "  `machlaka` VARCHAR(45) NULL,\n" + "  `name_machlaka` VARCHAR(45) NULL,\n"
				+ "  `derog` VARCHAR(45) NULL,\n" + "  `tear_derog` VARCHAR(45) NULL,\n"
				+ "  `darga` VARCHAR(45) NULL,\n" + "  `teor_darga` VARCHAR(45) NULL,\n"
				+ "  `tchilat_avoda` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `kod_esok` VARCHAR(45) NULL,\n" + "  `tear_isok` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(60) NULL,\n" +

				"  `m3` VARCHAR(45) NULL,\n" + "  `m4` VARCHAR(45) NULL,\n" + "  `m5` VARCHAR(45) NULL,\n"
				+ "  `m6` VARCHAR(45) NULL,\n" + "  `m7` VARCHAR(45) NULL,\n" + "  `m8` VARCHAR(45) NULL,\n"
				+ "  `m9` VARCHAR(45) NULL,\n" + "  `m10` VARCHAR(45) NULL,\n" + "  `m11` VARCHAR(45) NULL,\n"
				+ "  `m12` VARCHAR(45) NULL,\n" + "  `m1` VARCHAR(45) NULL,\n" + "  `m2` VARCHAR(45) NULL,\n"
				+ "  `total` VARCHAR(45) NULL,\n" + "  `avg` VARCHAR(45) NULL,\n" + "  `avg_p` VARCHAR(45) NULL,\n"
				+ "  `sub` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

	}

	public void create_table_avidar4(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(60) NULL,\n" + "  `l_name` VARCHAR(60) NULL,\n"
				+ "  `tat_mifal` VARCHAR(60) NULL,\n" + "  `agaf` VARCHAR(45) NULL,\n"
				+ "  `machlaka` VARCHAR(45) NULL,\n" + "  `name_machlaka` VARCHAR(45) NULL,\n"
				+ "  `derog` VARCHAR(45) NULL,\n" + "  `tear_derog` VARCHAR(45) NULL,\n"
				+ "  `darga` VARCHAR(45) NULL,\n" + "  `teor_darga` VARCHAR(45) NULL,\n"
				+ "  `tchilat_avoda` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `kod_esok` VARCHAR(45) NULL,\n" + "  `tear_isok` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(60) NULL,\n" +

				"  `m4` VARCHAR(45) NULL,\n" + "  `m5` VARCHAR(45) NULL,\n" + "  `m6` VARCHAR(45) NULL,\n"
				+ "  `m7` VARCHAR(45) NULL,\n" + "  `m8` VARCHAR(45) NULL,\n" + "  `m9` VARCHAR(45) NULL,\n"
				+ "  `m10` VARCHAR(45) NULL,\n" + "  `m11` VARCHAR(45) NULL,\n" + "  `m12` VARCHAR(45) NULL,\n"
				+ "  `m1` VARCHAR(45) NULL,\n" + "  `m2` VARCHAR(45) NULL,\n" + "  `m3` VARCHAR(45) NULL,\n"
				+ "  `total` VARCHAR(45) NULL,\n" + "  `avg` VARCHAR(45) NULL,\n" + "  `avg_p` VARCHAR(45) NULL,\n"
				+ "  `sub` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

	}

	public void create_table_avidar5(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(60) NULL,\n" + "  `l_name` VARCHAR(60) NULL,\n"
				+ "  `tat_mifal` VARCHAR(60) NULL,\n" + "  `agaf` VARCHAR(45) NULL,\n"
				+ "  `machlaka` VARCHAR(45) NULL,\n" + "  `name_machlaka` VARCHAR(45) NULL,\n"
				+ "  `derog` VARCHAR(45) NULL,\n" + "  `tear_derog` VARCHAR(45) NULL,\n"
				+ "  `darga` VARCHAR(45) NULL,\n" + "  `teor_darga` VARCHAR(45) NULL,\n"
				+ "  `tchilat_avoda` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `kod_esok` VARCHAR(45) NULL,\n" + "  `tear_isok` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(60) NULL,\n" +

				"  `m5` VARCHAR(45) NULL,\n" + "  `m6` VARCHAR(45) NULL,\n" + "  `m7` VARCHAR(45) NULL,\n"
				+ "  `m8` VARCHAR(45) NULL,\n" + "  `m9` VARCHAR(45) NULL,\n" + "  `m10` VARCHAR(45) NULL,\n"
				+ "  `m11` VARCHAR(45) NULL,\n" + "  `m12` VARCHAR(45) NULL,\n" + "  `m1` VARCHAR(45) NULL,\n"
				+ "  `m2` VARCHAR(45) NULL,\n" + "  `m3` VARCHAR(45) NULL,\n" + "  `m4` VARCHAR(45) NULL,\n"
				+ "  `total` VARCHAR(45) NULL,\n" + "  `avg` VARCHAR(45) NULL,\n" + "  `avg_p` VARCHAR(45) NULL,\n"
				+ "  `sub` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

	}

	public void create_table_avidar6(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(60) NULL,\n" + "  `l_name` VARCHAR(60) NULL,\n"
				+ "  `tat_mifal` VARCHAR(60) NULL,\n" + "  `agaf` VARCHAR(45) NULL,\n"
				+ "  `machlaka` VARCHAR(45) NULL,\n" + "  `name_machlaka` VARCHAR(45) NULL,\n"
				+ "  `derog` VARCHAR(45) NULL,\n" + "  `tear_derog` VARCHAR(45) NULL,\n"
				+ "  `darga` VARCHAR(45) NULL,\n" + "  `teor_darga` VARCHAR(45) NULL,\n"
				+ "  `tchilat_avoda` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `kod_esok` VARCHAR(45) NULL,\n" + "  `tear_isok` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(60) NULL,\n"
				+ "  `m6` VARCHAR(45) NULL,\n" + "  `m7` VARCHAR(45) NULL,\n" + "  `m8` VARCHAR(45) NULL,\n"
				+ "  `m9` VARCHAR(45) NULL,\n" + "  `m10` VARCHAR(45) NULL,\n" + "  `m11` VARCHAR(45) NULL,\n"
				+ "  `m12` VARCHAR(45) NULL,\n" + "  `m1` VARCHAR(45) NULL,\n" + "  `m2` VARCHAR(45) NULL,\n"
				+ "  `m3` VARCHAR(45) NULL,\n" + "  `m4` VARCHAR(45) NULL,\n" + "  `m5` VARCHAR(45) NULL,\n"
				+ "  `total` VARCHAR(45) NULL,\n" + "  `avg` VARCHAR(45) NULL,\n" + "  `avg_p` VARCHAR(45) NULL,\n"
				+ "  `sub` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

	}

	public void create_table_avidar7(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(60) NULL,\n" + "  `l_name` VARCHAR(60) NULL,\n"
				+ "  `tat_mifal` VARCHAR(60) NULL,\n" + "  `agaf` VARCHAR(45) NULL,\n"
				+ "  `machlaka` VARCHAR(45) NULL,\n" + "  `name_machlaka` VARCHAR(45) NULL,\n"
				+ "  `derog` VARCHAR(45) NULL,\n" + "  `tear_derog` VARCHAR(45) NULL,\n"
				+ "  `darga` VARCHAR(45) NULL,\n" + "  `teor_darga` VARCHAR(45) NULL,\n"
				+ "  `tchilat_avoda` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `kod_esok` VARCHAR(45) NULL,\n" + "  `tear_isok` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(60) NULL,\n" +

				"  `m7` VARCHAR(45) NULL,\n" + "  `m8` VARCHAR(45) NULL,\n" + "  `m9` VARCHAR(45) NULL,\n"
				+ "  `m10` VARCHAR(45) NULL,\n" + "  `m11` VARCHAR(45) NULL,\n" + "  `m12` VARCHAR(45) NULL,\n"
				+ "  `m1` VARCHAR(45) NULL,\n" + "  `m2` VARCHAR(45) NULL,\n" + "  `m3` VARCHAR(45) NULL,\n"
				+ "  `m4` VARCHAR(45) NULL,\n" + "  `m5` VARCHAR(45) NULL,\n" + "  `m6` VARCHAR(45) NULL,\n"
				+ "  `total` VARCHAR(45) NULL,\n" + "  `avg` VARCHAR(45) NULL,\n" + "  `avg_p` VARCHAR(45) NULL,\n"
				+ "  `sub` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

	}

	public void create_table_avidar8(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(60) NULL,\n" + "  `l_name` VARCHAR(60) NULL,\n"
				+ "  `tat_mifal` VARCHAR(60) NULL,\n" + "  `agaf` VARCHAR(45) NULL,\n"
				+ "  `machlaka` VARCHAR(45) NULL,\n" + "  `name_machlaka` VARCHAR(45) NULL,\n"
				+ "  `derog` VARCHAR(45) NULL,\n" + "  `tear_derog` VARCHAR(45) NULL,\n"
				+ "  `darga` VARCHAR(45) NULL,\n" + "  `teor_darga` VARCHAR(45) NULL,\n"
				+ "  `tchilat_avoda` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `kod_esok` VARCHAR(45) NULL,\n" + "  `tear_isok` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(60) NULL,\n" +

				"  `m8` VARCHAR(45) NULL,\n" + "  `m9` VARCHAR(45) NULL,\n" + "  `m10` VARCHAR(45) NULL,\n"
				+ "  `m11` VARCHAR(45) NULL,\n" + "  `m12` VARCHAR(45) NULL,\n" + "  `m1` VARCHAR(45) NULL,\n"
				+ "  `m2` VARCHAR(45) NULL,\n" + "  `m3` VARCHAR(45) NULL,\n" + "  `m4` VARCHAR(45) NULL,\n"
				+ "  `m5` VARCHAR(45) NULL,\n" + "  `m6` VARCHAR(45) NULL,\n" + "  `m7` VARCHAR(45) NULL,\n"
				+ "  `total` VARCHAR(45) NULL,\n" + "  `avg` VARCHAR(45) NULL,\n" + "  `avg_p` VARCHAR(45) NULL,\n"
				+ "  `sub` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

	}

	public void create_table_avidar9(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(60) NULL,\n" + "  `l_name` VARCHAR(60) NULL,\n"
				+ "  `tat_mifal` VARCHAR(60) NULL,\n" + "  `agaf` VARCHAR(45) NULL,\n"
				+ "  `machlaka` VARCHAR(45) NULL,\n" + "  `name_machlaka` VARCHAR(45) NULL,\n"
				+ "  `derog` VARCHAR(45) NULL,\n" + "  `tear_derog` VARCHAR(45) NULL,\n"
				+ "  `darga` VARCHAR(45) NULL,\n" + "  `teor_darga` VARCHAR(45) NULL,\n"
				+ "  `tchilat_avoda` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `kod_esok` VARCHAR(45) NULL,\n" + "  `tear_isok` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(60) NULL,\n" +

				"  `m9` VARCHAR(45) NULL,\n" + "  `m10` VARCHAR(45) NULL,\n" + "  `m11` VARCHAR(45) NULL,\n"
				+ "  `m12` VARCHAR(45) NULL,\n" + "  `m1` VARCHAR(45) NULL,\n" + "  `m2` VARCHAR(45) NULL,\n"
				+ "  `m3` VARCHAR(45) NULL,\n" + "  `m4` VARCHAR(45) NULL,\n" + "  `m5` VARCHAR(45) NULL,\n"
				+ "  `m6` VARCHAR(45) NULL,\n" + "  `m7` VARCHAR(45) NULL,\n" + "  `m8` VARCHAR(45) NULL,\n"
				+ "  `total` VARCHAR(45) NULL,\n" + "  `avg` VARCHAR(45) NULL,\n" + "  `avg_p` VARCHAR(45) NULL,\n"
				+ "  `sub` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

	}

	public void load_data_avidar1(String name_schema, String name_tabel) throws SQLException {

		String b = "LOAD DATA LOCAL INFILE " + " '/home/shoshana/Downloads/avidar/avidar_tikshoret9.csv'"
				+ "INTO TABLE `Upload_file`." + name_tabel
				+ "  CHARACTER SET hebrew FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES\n"
				+ " (mis_oved, f_name, l_name, tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n"
				+ " m1, m2, m3, m4, m5, m6, m7, m8, m9, \n"
				+ " m10, m11, m12, total, avg, avg_p, sub,dyear) set dyear=2016";
		tr.Insertintodb1(b);

	}

	public void load_data_avidar2(String name_schema, String name_tabel) throws SQLException {

		String b = "LOAD DATA LOCAL INFILE " + " '/home/shoshana/Downloads/avidar/2.csv'"
				+ "INTO TABLE `Upload_file`.tbl_avidar2  CHARACTER SET hebrew FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES\n"
				+ " (mis_oved, f_name, l_name, tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n"
				+ "  m2, m3, m4, m5, m6, m7, m8, m9, \n"
				+ " m10, m11, m12,m1, total, avg, avg_p, sub,dyear) set dyear=2016";
		tr.Insertintodb1(b);

	}

	public void load_data_avidar3(String name_schema, String name_tabel) throws SQLException {

		String b = "LOAD DATA LOCAL INFILE " + " '/home/shoshana/Downloads/avidar/3.csv'"
				+ "INTO TABLE `Upload_file`.tbl_avidar3  CHARACTER SET hebrew FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES\n"
				+ " (mis_oved, f_name, l_name, tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n"
				+ "  m3, m4, m5, m6, m7, m8, m9, \n"
				+ " m10, m11, m12,m1, m2, total, avg, avg_p, sub,dyear) set dyear=2016";
		tr.Insertintodb1(b);

	}

	public void load_data_avidar4(String name_schema, String name_tabel) throws SQLException {

		String b = "LOAD DATA LOCAL INFILE " + " '/home/shoshana/Downloads/avidar/4.csv'"
				+ "INTO TABLE `Upload_file`.tbl_avidar4  CHARACTER SET hebrew FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES\n"
				+ " (mis_oved, f_name, l_name, tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n"
				+ "  m4, m5, m6, m7, m8, m9, \n"
				+ " m10, m11, m12,m1, m2, m3, total, avg, avg_p, sub,dyear) set dyear=2016";
		tr.Insertintodb1(b);

	}

	public void load_data_avidar5(String name_schema, String name_tabel) throws SQLException {

		String b = "LOAD DATA LOCAL INFILE " + " '/home/shoshana/Downloads/avidar/5.csv'"
				+ "INTO TABLE `Upload_file`.tbl_avidar5  CHARACTER SET hebrew FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES\n"
				+ " (mis_oved, f_name, l_name, tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n"
				+ "  m5, m6, m7, m8, m9, \n"
				+ " m10, m11, m12,m1, m2, m3, m4, total, avg, avg_p, sub,dyear) set dyear=2016";
		tr.Insertintodb1(b);

	}

	public void load_data_avidar6(String name_schema, String name_tabel) throws SQLException {

		String b = "LOAD DATA LOCAL INFILE " + " '/home/shoshana/Downloads/avidar/6.csv'"
				+ "INTO TABLE `Upload_file`.tbl_avidar6  CHARACTER SET hebrew FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES\n"
				+ " (mis_oved, f_name, l_name, tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n"
				+ "  m6, m7, m8, m9, \n"
				+ " m10, m11, m12,m1, m2, m3, m4, m5, total, avg, avg_p, sub,dyear) set dyear=2016";
		tr.Insertintodb1(b);

	}

	public void load_data_avidar7(String name_schema, String name_tabel) throws SQLException {

		String b = "LOAD DATA LOCAL INFILE " + " '/home/shoshana/Downloads/avidar/7.csv'"
				+ "INTO TABLE `Upload_file`.tbl_avidar7  CHARACTER SET hebrew FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES\n"
				+ " (mis_oved, f_name, l_name, tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n"
				+ "  m7, m8, m9, \n"
				+ " m10, m11, m12,m1, m2, m3, m4, m5, m6, total, avg, avg_p, sub,dyear) set dyear=2016";
		tr.Insertintodb1(b);

	}

	public void load_data_avidar8(String name_schema, String name_tabel) throws SQLException {

		String b = "LOAD DATA LOCAL INFILE " + " '/home/shoshana/Downloads/avidar/8.csv'"
				+ "INTO TABLE `Upload_file`.tbl_avidar8  CHARACTER SET hebrew FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES\n"
				+ " (mis_oved, f_name, l_name, tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n"
				+ " m8, m9, \n"
				+ " m10, m11, m12, m1, m2, m3, m4, m5, m6, m7, total, avg, avg_p, sub,dyear) set dyear=2016";
		tr.Insertintodb1(b);

	}

	public void load_data_avidar9(String name_schema, String name_tabel) throws SQLException {

		String b = "LOAD DATA LOCAL INFILE " + " '/home/shoshana/Downloads/avidar/9.csv'"
				+ "INTO TABLE `Upload_file`.tbl_avidar9  CHARACTER SET hebrew FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES\n"
				+ " (mis_oved, f_name, l_name, tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n"
				+ "  m9, \n"
				+ " m10, m11, m12, m1, m2, m3, m4, m5, m6, m7,m8, total, avg, avg_p, sub,dyear) set dyear=2016";
		tr.Insertintodb1(b);

	}

	public void convert_to_101_avidar(String name_schema, String name, String name_table, int cid) throws SQLException {

		String load = "insert into " + name_schema + "." + name + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,source) \n"
				+ "            SELECT    concat(`f_name`,' ',`l_name`),888,`dyear`,`id`, `symbol`,concat(`symbol_name`,':',`sub`),sum(m1),tat_mifal\n"
				+ "            FROM Upload_file.tbl_avidar1"
				+ "   group by dyear,id,`symbol`,concat(`symbol_name`,':',`sub`)";

		tr.Insertintodb1(load);

		String load1 = "insert into " + name_schema + "." + name + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m2,source) \n"
				+ "            SELECT    concat(`f_name`,' ',`l_name`),888,`dyear`,`id`, `symbol`,concat(`symbol_name`,':',`sub` ),sum(m2),tat_mifal\n"
				+ "            FROM Upload_file.tbl_avidar2    group by dyear,id,`symbol`,concat(`symbol_name`,':',`sub`) "
				+ "            on duplicate key update m2=values(m2);";

		tr.Insertintodb1(load1);

		String load2 = "insert into " + name_schema + "." + name + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m3,source) \n"
				+ "            SELECT    concat(`f_name`,' ',`l_name`),888,`dyear`,`id`, `symbol`,concat(`symbol_name`,':',`sub`),sum(m3),tat_mifal\n"
				+ "            FROM Upload_file.tbl_avidar3   group by dyear,id,`symbol`,concat(`symbol_name`,':',`sub`) "
				+ "            on duplicate key update m3=values(m3);";

		tr.Insertintodb1(load2);

		String load3 = "insert into " + name_schema + "." + name + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m4,source) \n"
				+ "            SELECT    concat(`f_name`,' ',`l_name`),888,`dyear`,`id`, `symbol`,concat(`symbol_name`,':',`sub`),sum(m4),tat_mifal\n"
				+ "            FROM Upload_file.tbl_avidar4  group by dyear,id,`symbol`,concat(`symbol_name`,':',`sub`) "
				+ "            on duplicate key update m4=values(m4);";

		tr.Insertintodb1(load3);
		String load4 = "insert into " + name_schema + "." + name + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m5,source) \n"
				+ "            SELECT    concat(`f_name`,' ',`l_name`),888,`dyear`,`id`, `symbol`,concat(`symbol_name`,':',`sub`),sum(m5),tat_mifal\n"
				+ "            FROM Upload_file.tbl_avidar5  group by dyear,id,`symbol`,concat(`symbol_name`,':',`sub`) "
				+ "            on duplicate key update m5=values(m5);";

		tr.Insertintodb1(load4);
		String load5 = "insert into " + name_schema + "." + name + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m6,source) \n"
				+ "            SELECT    concat(`f_name`,' ',`l_name`),888,`dyear`,`id`, `symbol`,concat(`symbol_name`,':',`sub`),sum(m6),tat_mifal\n"
				+ "            FROM Upload_file.tbl_avidar6  group by dyear,id,`symbol`,concat(`symbol_name`,':',`sub`) "
				+ "            on duplicate key update m6=values(m6);";

		tr.Insertintodb1(load5);

		String load6 = "insert into " + name_schema + "." + name + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m7,source) \n"
				+ "            SELECT    concat(`f_name`,' ',`l_name`),888,`dyear`,`id`, `symbol`,concat(`symbol_name`,':',`sub`),sum(m7),tat_mifal\n"
				+ "            FROM Upload_file.tbl_avidar7 group by dyear,id,`symbol`,concat(`symbol_name`,':',`sub`) "
				+ "            on duplicate key update m7=values(m7);";

		tr.Insertintodb1(load6);

		String load7 = "insert into " + name_schema + "." + name + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m8,source) \n"
				+ "            SELECT    concat(`f_name`,' ',`l_name`),888,`dyear`,`id`, `symbol`,concat(`symbol_name`,':',`sub`),sum(m8),tat_mifal\n"
				+ "            FROM Upload_file.tbl_avidar8 group by dyear,id,`symbol`,concat(`symbol_name`,':',`sub`) "
				+ "            on duplicate key update m8=values(m8);";

		tr.Insertintodb1(load7);
		String load8 = "insert into " + name_schema + "." + name + "\n"
				+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m9,source) \n"
				+ "            SELECT    concat(`f_name`,' ',`l_name`),888,`dyear`,`id`, `symbol`,concat(`symbol_name`,':',`sub`),sum(m9),tat_mifal\n"
				+ "            FROM Upload_file.tbl_avidar9 group by dyear,id,`symbol`,concat(`symbol_name`,':',`sub`) "
				+ "            on duplicate key update m9=values(m9);";

		tr.Insertintodb1(load8);

	}

	public void create_101_avidar(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE if not exists  " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n" + "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n" + "  `FullName` varchar(51) NOT NULL,\n"
				+ "  `Symbol` varchar(100) NOT NULL DEFAULT '0' ,\n" + "  `SymbolName` varchar(100) NOT NULL,\n"
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
				+ "  `Symbol1` varchar(100) NOT NULL DEFAULT '0' ,\n" + "  PRIMARY KEY (`in_id`),\n"
				+ "  UNIQUE KEY `ind_unique` (`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,`division`) USING BTREE,\n"
				+ "  KEY `indi3` (`cid`,`dyear`,`Symbol`,`SymbolName`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";
		tr.Insertintodb1(a);

	}

	// /home/shoshana/Downloads/ikea
	public void create_table_ikea_alphone() throws SQLException {

		String a = "CREATE TABLE if not exists `Upload_file`.`tbl_ikea_alphone` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `sicomim` VARCHAR(45) NULL,\n"
				+ "  `mis_oved` VARCHAR(45) NULL,\n" + "  `l_name` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(45) NULL,\n" + "  `machlaka` VARCHAR(45) NULL,\n"
				+ "  `tat_mifal` VARCHAR(45) NULL,\n" + "  `matzav` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(a);
	}

	public void load_data_ikea_alphone() throws SQLException {
		// name_tabel = name_tabel;

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/ikea/alphone.csv'"
				+ " INTO TABLE `Upload_file`.`tbl_ikea_alphone`"
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  "
				+ " (`sicomim` ,  `mis_oved` ,  `l_name` ,  `f_name` ,  `machlaka` ,  `tat_mifal`,  `matzav` ,  `id` )";

		// in_id, name_company, tax_year, id_Worker, full_name, m, symbol,
		// symbolName, value, sicom_camot, dyear, name_machlaka
		tr.Insertintodb1(a);

	}

	public void insert_into_101_avidar() throws SQLException {

		for (int i = 1; i < 13; i++) {

			String a = "insert ignore into shoshana.tbl_101_avidar\n"
					+ "(cid, dyear, id, FullName, Symbol, SymbolName, m" + i + ", source)\n"
					+ "SELECT '924123740' as cid,dyear,id,full_name ,'1111' as symbol,concat('ברוטו',' ',sug_hafrasha_desc,' ',name_kupa),salary,num_file as source \n"
					+ "FROM shoshana.tbl_sum_avidar where   chodesh=" + i + "\n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			tr.Insertintodb1(a);
			String b = "insert ignore into shoshana.tbl_101_avidar\n"
					+ "(cid, dyear, id, FullName, Symbol, SymbolName, m" + i + ", source)\n"
					+ "SELECT '924123740' as cid,dyear,id,full_name ,'2222' as symbol,concat('הפרשה חודשית',' ',sug_hafrasha_desc,' ',name_kupa),hafrash_codshit,num_file as source \n"
					+ "FROM shoshana.tbl_sum_avidar where   chodesh=" + i + "\n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			tr.Insertintodb1(b);
			String c = "insert ignore into shoshana.tbl_101_avidar\n"
					+ "(cid, dyear, id, FullName, Symbol, SymbolName, m" + i + ", source)\n"
					+ "SELECT '924123740' as cid,dyear,id,full_name ,'3333' as symbol,concat('סכום פיצולים',' ',sug_hafrasha_desc,' ',name_kupa),scum_pitzulim,num_file as source \n"
					+ "FROM shoshana.tbl_sum_avidar where   chodesh=" + i + "\n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			tr.Insertintodb1(c);
			String d = "insert ignore into shoshana.tbl_101_avidar\n"
					+ "(cid, dyear, id, FullName, Symbol, SymbolName, m" + i + ", source)\n"
					+ "SELECT '924123740' as cid,dyear,id,full_name ,'4444' as symbol,concat('שעור  הפרשה',' ',sug_hafrasha_desc,' ',name_kupa),shiur_hafrasha,num_file as source \n"
					+ "FROM shoshana.tbl_sum_avidar where   chodesh=" + i + "\n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			tr.Insertintodb1(d);
		}

	}

	public void insert_into_101_ikea() throws SQLException

	{

		for (int i = 1; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea1 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,99,concat( 'אחוז הפרשה',' : ', sug_hafrasha_desc  ),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea1  as a  where chodesh=" + i + " \n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(a);
			String b = "insert into shoshana.tbl_101_ikea1 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n" + " SELECT full_name,430,dyear,id,88,concat( 'ברוטו',' : ', sug_hafrasha_desc  ),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea1  as a   where chodesh=" + i + " \n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(b);

			String c = "insert into shoshana.tbl_101_ikea1 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,77,concat( 'סכום פיצולים' ,' : ', sug_hafrasha_desc  ),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea1  as a  where chodesh=" + i + " \n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(c);
			String d = " insert into shoshana.tbl_101_ikea1 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,66,concat('שעור הפרשה'   ,' : ', sug_hafrasha_desc  ),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea1  as a   where chodesh=" + i + " \n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(d);

		}

	}

	public void insert_into_101_ikea1() throws SQLException

	{

		for (int i = 1; i < 13; i++) {

			String a = "insert into shoshana.tbl_101_ikea2 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,99,concat( 'אחוז הפרשה',' : ', sug_hafrasha_desc  ),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea_sofi  as a  where chodesh=" + i + " \n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(a);
			String b = "insert into shoshana.tbl_101_ikea2 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n" + " SELECT full_name,430,dyear,id,88,concat( 'ברוטו',' : ', sug_hafrasha_desc  ),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_ikea_sofi  as a   where chodesh=" + i + " \n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(b);

			String c = "insert into shoshana.tbl_101_ikea2 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,77,concat( 'סכום פיצולים' ,' : ', sug_hafrasha_desc  ),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_ikea_sofi  as a  where chodesh=" + i + " \n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(c);
			String d = " insert into shoshana.tbl_101_ikea2 (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source)\n"
					+ " SELECT full_name,430,dyear,id,66,concat('שעור הפרשה'   ,' : ', sug_hafrasha_desc  ),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_ikea_sofi  as a   where chodesh=" + i
					+ " \n" + "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(d);

		}

	}

	public void insert_into_101_ikea_2_xml() throws SQLException {
		// ,' : ',num_file

		for (int i = 1; i < 13; i++) {

			String a = "insert ignore into shoshana.tbl_101_ikea_2xml (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,99,concat( 'אחוז הפרשה',' : ', sug_hafrasha_desc , ' ',name_kupa ),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_fisher1  as a  where chodesh=" + i + " \n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(a);
			String b = "insert ignore into shoshana.tbl_101_ikea_2xml (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,88,concat( 'ברוטו',' : ', sug_hafrasha_desc , ' ',name_kupa ),\n"
					+ " `salary` ,`num_file`  FROM shoshana.tbl_sum_fisher1  as a   where chodesh=" + i + " \n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(b);

			String c = "insert ignore into shoshana.tbl_101_ikea_2xml (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,77,concat( 'סכום פיצולים' ,' : ', sug_hafrasha_desc , ' ',name_kupa ),\n"
					+ " `scum_pitzulim` ,`num_file`  FROM shoshana.tbl_sum_fisher1  as a  where chodesh=" + i + " \n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(c);
			String d = " insert ignore into shoshana.tbl_101_ikea_2xml (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"
					+ i + ",source)\n"
					+ " SELECT full_name,430,dyear,id,66,concat(  'שעור הפרשה'   ,' : ', sug_hafrasha_desc , ' ',name_kupa ),\n"
					+ " `shiur_hafrasha` ,`num_file`  FROM shoshana.tbl_sum_fisher1  as a   where chodesh=" + i + " \n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(d);

		}

	}

	public void create_table_brand(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `l_name` VARCHAR(45) NULL,\n" + "  `f_name` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(45) NULL,\n"
				+ "  `m1` VARCHAR(45) NULL,\n" + "  `m2` VARCHAR(45) NULL,\n" + "  `m3` VARCHAR(45) NULL,\n"
				+ "  `m4` VARCHAR(45) NULL,\n" + "  `m5` VARCHAR(45) NULL,\n" + "  `m6` VARCHAR(45) NULL,\n"
				+ "  `m7` VARCHAR(45) NULL,\n" + "  `m8` VARCHAR(45) NULL,\n" + "  `m9` VARCHAR(45) NULL,\n"
				+ "  `m10` VARCHAR(45) NULL,\n" + "  `m11` VARCHAR(45) NULL,\n" + "  `m12` VARCHAR(45) NULL,\n"
				+ "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb(a);

	}

	public void create_table_h(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `l_name` VARCHAR(45) NULL,\n" + "  `f_name` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(45) NULL,\n" +

				"  `m9` VARCHAR(45) NULL,\n" + "  `m10` VARCHAR(45) NULL,\n" + "  `m11` VARCHAR(45) NULL,\n" +

				"  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb(a);

	}

	public void create_table_b(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `l_name` VARCHAR(45) NULL,\n" + "  `f_name` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `symbol` VARCHAR(45) NULL,\n" + "  `symbol_name` VARCHAR(45) NULL,\n" +

				"  `m9` VARCHAR(250) NULL,\n" + "  `m10`VARCHAR(250) NULL,\n" + "  `m11`  VARCHAR(250) NULL,\n" +

				"  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb(a);

	}

	public void load_data_brand(String name_schema, String name_tabel) throws SQLException {
		// name_tabel = name_tabel;

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/avidar/chevrot_btoch.csv'" + " INTO TABLE `"
				+ name_schema + "`." + name_tabel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  "
				+ " ( masad, sog_motzr, mis_c_p, name_chevra, num_acount, num_tick_nicoyim_of_kupot_gemel, mis_eshor_kupat_gemel, mis_kupot_shemuzgo)";

		// in_id, name_company, tax_year, id_Worker, full_name, m, symbol,
		// symbolName, value, sicom_camot, dyear, name_machlaka
		tr.Insertintodb1(a);

	}

	public void create_101_brand(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists " + name_schema + ".`" + name_table + "` (\n"
				+ "  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" + "  `cid` int(10) unsigned NOT NULL,\n"
				+ "  `dyear` smallint(6) NOT NULL,\n" + "  `id` int(11) NOT NULL,\n"
				+ "  `original_id` int(11) DEFAULT NULL,\n" + "  `FullName` varchar(51) NOT NULL,\n"
				+ "  `Symbol` varchar(51) NOT NULL,\n" + "  `SymbolName` varchar(100) NOT NULL,\n"
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

	}

	public void create_tabel_brand(String name_schema, String name_tabel) throws SQLException {

		String create = "CREATE TABLE  if not exists  `" + name_schema + "`." + name_tabel + " ("
				+ "`in_id` INT NOT NULL AUTO_INCREMENT,"

				+ "`id_Worker` VARCHAR(200) NULL," + " `l_name` VARCHAR(200) NULL," + " `f_name` VARCHAR(200) NULL,"
				+ " `id` VARCHAR(200) NULL," + " `symbol` VARCHAR(200) NULL," + "`symbolName` VARCHAR(200) NULL,"
				+ "`value` VARCHAR(200) NULL," + "`m` VARCHAR(200) NULL," + "`dyear` VARCHAR(200) NULL,"
				+ "PRIMARY KEY (`in_id`))";

		tr.Insertintodb1(create);

	}

	public void create_table_chevrot_betoch(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL,\n" + "  `masad` VARCHAR(45) NULL,\n" + "  `sog_motzr` VARCHAR(45) NULL,\n"
				+ "  `mis_c_p` VARCHAR(45) NULL,\n" + "  `name_chevra` VARCHAR(45) NULL,\n"
				+ "  `num_acount` VARCHAR(45) NULL,\n" + "  `num_tick_nicoyim_of_kupot_gemel` VARCHAR(45) NULL,\n"
				+ "  `mis_eshor_kupat_gemel` VARCHAR(45) NULL,\n" + "  `mis_kupot_shemuzgo` VARCHAR(45) NULL,\n" +

				"  PRIMARY KEY (`in_id`));";
		tr.Insertintodb(a);

	}

	public void create_table_achoz_misra(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `l_name` VARCHAR(45) NULL,\n" + "  `f_name` VARCHAR(45) NULL,\n"
				+ "  `tchilat_avoda` VARCHAR(45) NULL,\n" + "  `vetek` VARCHAR(45) NULL,\n"
				+ "  `tkufa` VARCHAR(45) NULL,\n" + "  `camut` VARCHAR(45) NULL,\n" + "  `camut1` VARCHAR(45) NULL,\n"
				+ "  `calaf` VARCHAR(45) NULL,\n" + "  `sach` VARCHAR(45) NULL,\n"
				+ "  `achoz_misra` VARCHAR(45) NULL,\n" + "  `ca` VARCHAR(45) NULL,\n" + "  `oved` VARCHAR(45) NULL,\n"
				+ "  `id` VARCHAR(45) NULL,\n" + "  `maclaka` VARCHAR(45) NULL,\n" + "  `tear_m` VARCHAR(45) NULL,\n"
				+ "  `machlaka` VARCHAR(45) NULL,\n" + "  `agaf` VARCHAR(45) NULL,\n"
				+ "  `tear_agaf` VARCHAR(45) NULL,\n" + "  `tat` VARCHAR(45) NULL,\n"
				+ "  `tear100` VARCHAR(45) NULL,\n" + "  `mam` VARCHAR(45) NULL,\n" + "  `t_leada` VARCHAR(45) NULL,\n"
				+ "  `status` VARCHAR(45) NULL,\n" + "  `hafsaka` VARCHAR(45) NULL,\n" + "  `tear` VARCHAR(45) NULL,\n"
				+ "  `tear1` VARCHAR(45) NULL,\n" + "  `tear2` VARCHAR(45) NULL,\n" + "  `tear3` VARCHAR(45) NULL,\n"
				+ "  `tear4` VARCHAR(45) NULL,\n" + "  `tear5` VARCHAR(45) NULL,\n" + "  `tear6` VARCHAR(45) NULL,\n"
				+ "  `tear7` VARCHAR(45) NULL,\n" + "  `tear8` VARCHAR(45) NULL,\n" + "  `tear9` VARCHAR(45) NULL,\n"
				+ "  `tear10` VARCHAR(45) NULL,\n" + "  `tear11` VARCHAR(45) NULL,\n" + "  `tear12` VARCHAR(45) NULL,\n"
				+ "  `tear13` VARCHAR(45) NULL,\n" + "  `tear14` VARCHAR(45) NULL,\n" + "  `tear15` VARCHAR(45) NULL,\n"
				+ "  `tear16` VARCHAR(45) NULL,\n" + "  `tear17` VARCHAR(45) NULL,\n" + "  `tear18` VARCHAR(45) NULL,\n"
				+ "  `tear19` VARCHAR(45) NULL,\n" + "  `tear20` VARCHAR(45) NULL,\n" + "  `tear21` VARCHAR(45) NULL,\n"
				+ "  `tear22` VARCHAR(45) NULL,\n" + "  `tear23` VARCHAR(45) NULL,\n" + "  `tear24` VARCHAR(45) NULL,\n"
				+ "  `tear25` VARCHAR(45) NULL,\n" + "  `tear26` VARCHAR(45) NULL,\n" + "  `tear27` VARCHAR(45) NULL,\n"
				+ "  `tear28` VARCHAR(45) NULL,\n" + "  `tear29` VARCHAR(45) NULL,\n" + "  `tear30` VARCHAR(45) NULL,\n"
				+ "  `tear31` VARCHAR(45) NULL,\n" + "  `tear32` VARCHAR(45) NULL,\n" + "  `tear33` VARCHAR(45) NULL,\n"
				+ "  `tear34` VARCHAR(45) NULL,\n" + "  `tear35` VARCHAR(45) NULL,\n" + "  `tear36` VARCHAR(45) NULL,\n"
				+ "  `tear37` VARCHAR(45) NULL,\n" + "  `tear38` VARCHAR(45) NULL,\n" + "  `tear39` VARCHAR(45) NULL,\n"
				+ "  `tea40` VARCHAR(45) NULL,\n" + "  `tear41` VARCHAR(45) NULL,\n" + "  `tear42` VARCHAR(45) NULL,\n"
				+ "  `tear43` VARCHAR(45) NULL,\n" + "  `tear44` VARCHAR(45) NULL,\n" + "  `tear45` VARCHAR(45) NULL,\n"
				+ "  `tear46` VARCHAR(45) NULL,\n" + "  `tear47` VARCHAR(45) NULL,\n" + "  `tear48` VARCHAR(45) NULL,\n"
				+ "  `tear49` VARCHAR(45) NULL,\n" + "  `tear50` VARCHAR(45) NULL,\n" + "  `tear51` VARCHAR(45) NULL,\n"
				+ "  `tear52` VARCHAR(45) NULL,\n" + "  `tear53` VARCHAR(45) NULL,\n" + "  `tear54` VARCHAR(45) NULL,\n"
				+ "  `chodesh` VARCHAR(45) NULL,\n" +

				"  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb(a);

	}

	public void load_data_achoz_misra(String name_schema, String name_tabel) throws SQLException {
		// name_tabel = name_tabel;

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/achoz_misra.csv'" + " INTO TABLE `"
				+ name_schema + "`." + name_tabel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  "
				+ " (  mis_oved, l_name, f_name, tchilat_avoda, vetek, tkufa, camut, camut1, calaf, sach, achoz_misra, ca, oved, id, maclaka, tear_m, machlaka, agaf, tear_agaf, tat, tear100, mam, t_leada, status, hafsaka, tear, tear1, tear2, tear3, tear4, tear5, tear6, tear7, tear8, tear9, tear10, tear11, tear12, tear13, tear14, tear15, tear16, tear17, tear18, tear19, tear20, tear21, tear22, tear23, tear24, tear25, tear26, tear27, tear28, tear29, tear30, tear31, tear32, tear33, tear34, tear35, tear36, tear37, tear38, tear39, tea40, tear41, tear42, tear43, tear44, tear45, tear46, tear47, tear48, tear49, tear50, tear51, tear52, tear53, tear54)";

		// in_id, name_company, tax_year, id_Worker, full_name, m, symbol,
		// symbolName, value, sicom_camot, dyear, name_machlaka
		tr.Insertintodb1(a);

	}

	public void load_data_achoz_misraa(String name_schema, String name_tabel) throws SQLException {
		// name_tabel = name_tabel;
		for (int i = 1; i < 13; i++) {

			String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/avidar/achoz_misra/2015/" + i + ".csv'"
					+ " INTO TABLE  `" + name_schema + "`." + name_tabel
					+ " CHARACTER SET hebrew  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES "
					+ "  ( mis_oved, l_name, f_name, tchilat_avoda, vetek, tkufa, camut, camut1, calaf, sach, achoz_misra, ca, oved, id, maclaka, tear_m, machlaka, agaf, tear_agaf, tat, tear100, mam, t_leada, status, hafsaka, tear, tear1, tear2, tear3, tear4, tear5, tear6, tear7, tear8, tear9, tear10, tear11, tear12, tear13, tear14, tear15, tear16, tear17, tear18, tear19, tear20, tear21, tear22, tear23, tear24, tear25, tear26, tear27, tear28, tear29, tear30, tear31, tear32, tear33, tear34, tear35, tear36, tear37, tear38, tear39, tea40, tear41, tear42, tear43, tear44, tear45, tear46, tear47, tear48, tear49, tear50, tear51, tear52, tear53, tear54)"
					+ "set dyear=2015,chodesh=" + i + ";";

			tr.Insertintodb1(a);
		}
	}

	public void convert_to_101_achoz_misra(String name_schema, String name, String name_table, int cid)
			throws SQLException {
		for (int i = 1; i < 13; i++) {
			String load = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n"
					+ "            SELECT   concat(`f_name`,' ',`l_name`)," + cid
					+ ",`dyear`,`mis_oved`, '5556','אחוז מישרה',`achoz_misra`,';ahuz_misra;'\n" + "            FROM  "
					+ name_schema + "." + name_table + "  where chodesh=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load);
		}

	}

	public void create_table_micsat_havraha(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(45) NULL,\n" + "  `l_name` VARCHAR(45) NULL,\n"
				+ "  `tchilat_avoda` VARCHAR(45) NULL,\n" + "  `vetek` VARCHAR(45) NULL,\n"
				+ "  `tkufa` VARCHAR(45) NULL,\n" + "  `camut` VARCHAR(45) NULL,\n" + "  `camut1` VARCHAR(45) NULL,\n"
				+ "  `calaf` VARCHAR(45) NULL,\n" + "  `sach` VARCHAR(45) NULL,\n" + "  `sach1` VARCHAR(45) NULL,\n"
				+ "  `achoz_misra` VARCHAR(45) NULL,\n" + "  `status` VARCHAR(45) NULL,\n"
				+ "  `micsa_shnatit` VARCHAR(45) NULL,\n" + "  `yitrat_pticha` VARCHAR(45) NULL,\n"
				+ "  `zicoy` VARCHAR(45) NULL,\n" + "  `nitzol` VARCHAR(45) NULL,\n"
				+ "  `yitrat_sgira` VARCHAR(45) NULL,\n" + "  `aaa` VARCHAR(45) NULL,\n" +

				"  `zzz` VARCHAR(45) NULL,\n" + "  `potznteal` VARCHAR(45) NULL,\n"
				+ "  `zicoy_tzafu` VARCHAR(45) NULL,\n" + "  `yitra_tzafui` VARCHAR(45) NULL,\n"
				+ "  `micsa` VARCHAR(45) NULL,\n" + "  `chodesh` VARCHAR(45) NULL,\n" +

				"  `dyear` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb(a);

	}

	public void load_data_micsat_havra(String name_schema, String name_tabel) throws SQLException {
		// name_tabel = name_tabel;
		for (int i = 1; i < 13; i++) {

			String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/avidar/micsat_havrah/2015/" + i + ".csv'"
					+ " INTO TABLE  `" + name_schema + "`." + name_tabel
					+ " CHARACTER SET hebrew  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES "
					+ "  ( mis_oved, f_name, l_name, tchilat_avoda, vetek, tkufa, camut, camut1, calaf, sach, sach1, achoz_misra, status, micsa_shnatit, yitrat_pticha, zicoy, nitzol, yitrat_sgira, aaa, zzz, potznteal, zicoy_tzafu, yitra_tzafui, micsa, chodesh, dyear)"
					+ "set dyear=2015,chodesh=" + i + ";";

			tr.Insertintodb1(a);
		}
	}

	public void convert_to_101_micsat_havraha(String name_schema, String name, String name_table, int cid)
			throws SQLException {
		for (int i = 1; i < 13; i++) {
			String load = "insert  into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type) \n"
					+ "            SELECT   concat(`f_name`,' ',`l_name`)," + cid
					+ ",`dyear`,ifnull(`mis_oved`,0), '5552','מכסת הבראה שנתי',`micsa_shnatit`,';mihsat_havraa;'\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where chodesh=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load);
		}

	}

	public void create_table_achoz_misra_shagoy(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mifal` VARCHAR(45) NULL,\n"
				+ "  `reciv_sachar` VARCHAR(45) NULL,\n" + "  `tear` VARCHAR(45) NULL,\n"
				+ "  `mis_oved` VARCHAR(45) NULL,\n" + "  `l_name` VARCHAR(45) NULL,\n"
				+ "  `f_name` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n" + "  `machlaka` VARCHAR(45) NULL,\n"
				+ "  `name_machlaka` VARCHAR(45) NULL,\n" + "  `cod_esok` VARCHAR(45) NULL,\n"
				+ "  `tear_esok` VARCHAR(45) NULL,\n" + "  `aaa` VARCHAR(45) NULL,\n" + "  `bbb` VARCHAR(45) NULL,\n"
				+ "  `ccc` VARCHAR(45) NULL,\n" + "  `scum_achoz_misra_sahgoy` VARCHAR(45) NULL,\n"
				+ "  `sach` VARCHAR(45) NULL,\n" + "  `sach1` VARCHAR(45) NULL,\n"
				+ "  `achoz_misra` VARCHAR(45) NULL,\n" + "  `status` VARCHAR(45) NULL,\n"
				+ "  `micsa_shnatit` VARCHAR(45) NULL,\n" + "  `yitrat_pticha` VARCHAR(45) NULL,\n"
				+ "  `zicoy` VARCHAR(45) NULL,\n" +

				"  `chodesh` VARCHAR(45) NULL,\n" +

				"  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb(a);

	}

	public void load_data_achoz_misra_shagoy(String name_schema, String name_tabel) throws SQLException {
		// name_tabel = name_tabel;
		for (int i = 1; i < 13; i++) {

			String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/avidar/achoz_misra_shagoy/2015/" + i
					+ ".csv'" + " INTO TABLE  `" + name_schema + "`." + name_tabel
					+ " CHARACTER SET hebrew  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES "
					+ "  (mifal, reciv_sachar, tear, mis_oved, l_name, f_name, id, machlaka, name_machlaka, cod_esok, tear_esok, aaa, bbb, ccc, scum_achoz_misra_sahgoy, sach, sach1, achoz_misra, status, micsa_shnatit, yitrat_pticha, zicoy, chodesh, dyear)"
					+ "set dyear=2015,chodesh=" + i + ";";

			tr.Insertintodb1(a);
		}
	}

	public void insert_101_hepenix_ikea(String name_schema, String name_table) throws SQLException {
		for (int i = 1; i < 13; i++) {

			String d = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat(mis_polisa,' : ','סוג_תנועה'),`sug_tnua`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(d);

			String a = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'   ,concat(mis_polisa,' : ','מעביד',' ','פיצוי אוני'),`p_mavid_oney`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(a);
			String v = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  ,concat(mis_polisa,' : ','מעביד',' ','תגמולי מעביד הוני'),`t_mavid_oney`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(v);

			String c = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat(mis_polisa,' : ','מעביד',' ','פיצוי קיצבה')     ,`p_mavid_kitzva`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(c);

			String m = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat(mis_polisa,' : ','מעביד',' ','תגמולי קיצבה' )  ,    `t_mavid_kitzva`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(m);

			String n = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat(mis_polisa,' : ','מעביד',' ','שלב מחוץ') ,`shlav_mavid_mchotz`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(n);

			String z = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat(mis_polisa,' : ','מעביד',' ','שונות מחוץ') ,`shonut_mavid_mchotz`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(z);

			String zz = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat('עובד',' ','תגמולים 45 הוני',' ',mis_polisa) ,`tagmulim_45_oved_hony`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(zz);

			String za = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat(mis_polisa,' : ','עובד',' ','תגמולים 47 הוני') ,`tagmulim_47_oved_hony`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(za);

			String zaa = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat(mis_polisa,' : ','עובד',' ','תגמולים 45 קיצבה'),`tagmulim_45_oved_kitzva`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(zaa);

			String zab = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat(mis_polisa,' : ','עובד',' ','תגמולים 47 קצבה'),`tagmulim_47_oved_kitzva`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(zab);

			String zas = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat(mis_polisa,' : ','עובד',' ','שלב מחוץ'),`shlav_oved_mchotz`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(zas);

			String zc = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat(mis_polisa,' : ','עובד',' ','שונות מחוץ' )     ,`shonut_oved_mchotz`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(zc);
			String zd = "insert ignore into " + name_schema + "." + name_table + "\n"
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source)\n"
					+ "          select concat(`f_name`,' ',`l_name`),430 as cid,\n"
					+ "  YEAR(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y')),`id`, '12'  , concat(mis_polisa,' : ','עובד',' ','פרמיה' ),`premia`,'איקאה נפרעים הפניקס'\n"
					+ "            FROM " + name_schema
					+ ".tbl_ikea_hepnix_hitkabel  where MONTH(STR_TO_DATE(`chodesh_gvia`, '%m/%d/%Y'))=" + i + "\n"
					+ "on duplicate key update m" + i + "=values(m" + i + ")";
			tr.Insertintodb1(zd);
		}

	}

	ResultSet r;
	Trysql t = new Trysql();
	Trysql tt = new Trysql();

	public void insert_into_runs_q(String name_function) throws SQLException {

		String a = "SELECT cid FROM system_management.tbl_companies where comp_name like '%פתאל%' and cid<>1500;";
		r = tr.gettabledb(a);
		int s;
		while (r.next()) {
			s = r.getInt(1);
			String x = "select max(run_group) from system_management.db_runs_queue1 ";
			int max = t.Readfromdb(x);
			max += 1;
			String v = "insert into system_management.db_runs_queue1 \n" + "(query,user,run_group)values('##"
					+ name_function + "##MONTH=0##CID=" + s + "##DYEAR=2016##RUN_GROUP=" + max + "','shoshana' ," + max
					+ ")";
			tt.Insertintodb1(v);

		}

	}

	public void insert_into_runs_q_all_years(String name_function) throws SQLException {

		for (int i = 2009; i < 2016; i++) {

			String a = "SELECT cid FROM system_management.tbl_companies where comp_name like '%פתאל%' and cid<>1500;";
			r = tr.gettabledb(a);
			int s;
			while (r.next()) {
				s = r.getInt(1);
				String x = "select max(run_group) from system_management.db_runs_queue1 ";
				int max = t.Readfromdb(x);
				max += 1;
				String v = "insert into system_management.db_runs_queue1 \n" + "(query,user,run_group)values('##"
						+ name_function + "##MONTH=0##CID=" + s + "##DYEAR=" + i + "##RUN_GROUP=" + max
						+ "','shoshana' ," + max + ")";
				tt.Insertintodb1(v);

			}
		}

	}

	public void insert_into_runs_q_3(String name_function, String name_function1, String name_function2)
			throws SQLException {

		for (int i = 2016; i < 2017; i++) {
			String a = "SELECT cid FROM system_management.tbl_companies where comp_name like '%פתאל%' and cid<>1500;";
			r = tr.gettabledb(a);
			int s;
			while (r.next()) {
				s = r.getInt(1);
				String x = "select max(run_group) from system_management.db_runs_queue1 ";
				int max = t.Readfromdb(x);
				max += 1;
				String v = "insert into system_management.db_runs_queue1 \n" + "(query,user,run_group)values('##"
						+ name_function + "##MONTH=0##CID=" + s + "##DYEAR=" + i + "##RUN_GROUP=" + max
						+ "','shoshana' ," + max + ")";
				tt.Insertintodb1(v);
				String vv = "insert into system_management.db_runs_queue1 \n" + "(query,user,run_group)values('##"
						+ name_function1 + "##MONTH=0##CID=" + s + "##DYEAR=" + i + "##RUN_GROUP=" + max
						+ "','shoshana' ," + max + ")";
				tt.Insertintodb1(vv);
				String vvv = "insert into system_management.db_runs_queue1 \n" + "(query,user,run_group)values('##"
						+ name_function2 + "##MONTH=0##CID=" + s + "##DYEAR=" + i + "##RUN_GROUP=" + max
						+ "','shoshana' ," + max + ")";
				tt.Insertintodb1(vvv);

			}
		}
	}

	void update_id() throws SQLException {

		String ddd = "UPDATE\n" + "    Upload_file.tbl_micsat_havraha5 as a\n" + "    ,\n"
				+ "   (select * from diff_taxes_reports.tbl_achoz_misra group by mis_oved) as b  SET\n"
				+ "    a.id  =b.id  where  a.mis_oved COLLATE utf8_unicode_ci =b.mis_oved;";
		tr.Insertintodb1(ddd);

		String e = "  update\n" + "    Upload_file.tbl_micsat_havraha5 as a\n" + "    ,\n"
				+ "   (select * from Upload_file.tbl_micsat_havraha1 group by mis_oved) as b \n" + "   SET\n"
				+ "    a.id  =b.id\n"
				+ "    where  concat(a.f_name,'',a.l_name)COLLATE utf8_unicode_ci=concat(b.f_name,'',b.l_name) ;";
		tr.Insertintodb1(e);

		String dd = "UPDATE\n" + "    Upload_file.tbl_micsat_havraha5 as a\n" + "    ,\n"
				+ "   (select * from Upload_file.tbl_micsat_havraha1 group by mis_oved) as b  SET\n"
				+ "    a.id  =b.id  where  a.mis_oved COLLATE utf8_unicode_ci =b.mis_oved;";
		tr.Insertintodb1(dd);

		String d = "UPDATE\n" + "    Upload_file.tbl_micsat_havraha5 as a\n" + "    ,\n"
				+ "   (select * from Upload_file.tbl_achoz_misra_shagoy1 group by mis_oved) as b  SET\n"
				+ "    a.id  =b.id  where  a.mis_oved COLLATE utf8_unicode_ci=b.mis_oved;";
		tr.Insertintodb1(d);

		String ll = "UPDATE\n" + "    Upload_file.tbl_micsat_havraha5 as a\n" + "    ,\n"
				+ "   (select * from  Upload_file.tbl_micsat_havraha2 group by mis_oved) as b  SET\n"
				+ "    a.id  =b.id  where  a.mis_oved COLLATE utf8_unicode_ci=b.mis_oved;";
		tr.Insertintodb1(ll);

		for (int i = 1; i < 9; i++) {
			String a = "UPDATE\n" + "    Upload_file.tbl_micsat_havraha5 as a\n" + "    ,\n"
					+ "   (select * from Upload_file.tbl_avidar" + i + " group by mis_oved) as b  SET\n"
					+ "    a.id  =b.id  where  a.mis_oved COLLATE utf8_unicode_ci =b.mis_oved;";
			tr.Insertintodb1(a);
		}

		String f = "UPDATE\n" + "    Upload_file.tbl_micsat_havraha5 as a\n" + "    ,\n"
				+ "   (select * from diff_taxes_reports.tbl_achoz_misra1 group by mis_oved) as b  SET\n"
				+ "    a.id  =b.id  where  a.mis_oved COLLATE utf8_unicode_ci =b.mis_oved;";
		tr.Insertintodb1(f);
		// String d="UPDATE\n" +
		// " Upload_file.tbl_micsat_havraha5 as a\n" +
		// " ,\n" +
		// " (select * from Upload_file.tbl_achoz_misra_shagoy1 group by
		// mis_oved) as b SET\n" +
		// " a.id =b.id where a.mis_oved COLLATE utf8_unicode_ci=b.mis_oved;";
		// tr.Insertintodb1(d);
		String l = "UPDATE\n" + "    Upload_file.tbl_micsat_havraha5 as a\n" + "    ,\n"
				+ "   (select * from Upload_file.tbl_achoz_misra_shagoy group by mis_oved) as b  SET\n"
				+ "    a.id  =b.id  where  a.mis_oved COLLATE utf8_unicode_ci=b.mis_oved;";
		tr.Insertintodb1(l);
		String s = "update Upload_file.tbl_micsat_havraha5 as a join  diff_taxes_reports.tbl_achoz_misra1 as b  on  a.mis_oved  =b.mis_oved\n"
				+ "\n" + "set a.id=b.id;";

	}

	public void convert_to_101_avidar_tikshoret(String name_schema, String name_table, String name, int cid)
			throws SQLException {
		for (int i = 1; i < 13; i++) {

			String load1 = "insert into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",source) \n"
					+ "            SELECT    concat(`f_name`,' ',`l_name`)," + cid
					+ ",`dyear`,`id`, `symbol`,concat(`symbol_name`,':',`sub`),sum(m" + i + "),13\n"
					+ "            FROM Upload_file." + name_table
					+ "    group by dyear,id,`symbol`,concat(`symbol_name`,':',`sub`) "
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";

			tr.Insertintodb1(load1);
		}

	}

	void convert_to_101_9(String name_schema, String name, String name_table, int cid) throws SQLException {

		for (int i = 1; i < 13; i++) {

			String load8 = "insert into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,Symbol,`SymbolName`,m" + i + ",type,permission) \n"
					+ "            SELECT concat(f_name,'  ',l_name) , tat_mifal,`dyear`,mis_oved,`symbol`, concat(`symbol_name`,':',sub),sum(m"
					+ i + "),sub,tat_mifal\n" + "            FROM " + name_schema + "." + name_table
					+ "  group by dyear,id,`symbol`, concat(`symbol_name`,':',sub)  " + "   on duplicate key update m"
					+ i + "=values(m" + i + ");";

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

			tr.Insertintodb1(load8);
		}
	}

	void convert_to_101_fixed_102_bt(String name_schema, String name) throws SQLException {
		for (int j = 2013; j < 2017; j++) {

			for (int i = 1; i < 13; i++) {

				String a = "insert ignore into " + name_schema + "." + name
						+ "(`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type_for_gemel)"
						+ "                             SELECT  fullname, cid ,`dyear`,`id`, 1000 as Symbol ,concat(yazran_name,' ',type_polisa,' ' ,'ברוטו לפני') as SymbolName ,bruto_before ,concat(';yazran','_',yazran_id,';')\n"
						+ "                              FROM  diff_taxes_reports.fixed_102_bt  where month=" + i
						+ " and dyear=" + j + "                               on duplicate key update m" + i
						+ "=values(m" + i + ");";
				tr.Insertintodb1(a);

				String b = "insert ignore into " + name_schema + "." + name
						+ "(`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type_for_gemel)"
						+ "                             SELECT  fullname, cid ,`dyear`,`id`, 2000 as Symbol ,concat(yazran_name,' ',type_polisa,' ' ,'ברוטו אחרי') as SymbolName ,bruto_after ,concat(';yazran','_',yazran_id,';')\n"
						+ "                              FROM  diff_taxes_reports.fixed_102_bt  where month=" + i
						+ " and dyear=" + j + "                               on duplicate key update m" + i
						+ "=values(m" + i + ");";
				tr.Insertintodb1(b);

				String c = "insert ignore into " + name_schema + "." + name
						+ "(`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type_for_gemel)"
						+ "                             SELECT  fullname, cid ,`dyear`,`id`, 3000 as Symbol ,concat(yazran_name,' ',type_polisa,' ' ,'ברוטו הפרש') as SymbolName ,brouto_diff ,concat(';yazran','_',yazran_id,';')\n"
						+ "                              FROM  diff_taxes_reports.fixed_102_bt  where month=" + i
						+ " and dyear=" + j + "                               on duplicate key update m" + i
						+ "=values(m" + i + ");";
				tr.Insertintodb1(c);

				String d = "insert ignore into " + name_schema + "." + name
						+ "(`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type_for_gemel)"
						+ "                             SELECT  fullname, cid ,`dyear`,`id`, 4000 as Symbol ,concat(yazran_name,' ',type_polisa,' ' ,'אחוז עובד') as SymbolName ,pre_oved ,concat(';yazran','_',yazran_id,';')\n"
						+ "                              FROM  diff_taxes_reports.fixed_102_bt  where month=" + i
						+ " and dyear=" + j + "                               on duplicate key update m" + i
						+ "=values(m" + i + ");";
				tr.Insertintodb1(d);

				String e = "insert ignore into " + name_schema + "." + name
						+ "(`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type_for_gemel)"
						+ "                             SELECT  fullname, cid ,`dyear`,`id`, 5000 as Symbol ,concat(yazran_name,' ',type_polisa,' ' ,'אחוז מעביד') as SymbolName ,pre_maavid ,concat(';yazran','_',yazran_id,';')\n"
						+ "                              FROM  diff_taxes_reports.fixed_102_bt  where month=" + i
						+ " and dyear=" + j + "                               on duplicate key update m" + i
						+ "=values(m" + i + ");";
				tr.Insertintodb1(e);

				String f = "insert ignore into " + name_schema + "." + name
						+ "(`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type_for_gemel)"
						+ "                             SELECT  fullname, cid ,`dyear`,`id`, 6000 as Symbol ,concat(yazran_name,' ',type_polisa,' ' ,'אחוז פיצוים') as SymbolName ,pre_pizuim ,concat(';yazran','_',yazran_id,';')\n"
						+ "                              FROM  diff_taxes_reports.fixed_102_bt  where month=" + i
						+ " and dyear=" + j + "                               on duplicate key update m" + i
						+ "=values(m" + i + ");";
				tr.Insertintodb1(f);

				String g = "insert ignore into " + name_schema + "." + name
						+ "(`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type_for_gemel)"
						+ "                             SELECT  fullname, cid ,`dyear`,`id`, 7000 as Symbol ,concat(yazran_name,' ',type_polisa,' ' ,'עובד חדש') as SymbolName ,new_oved,concat(';yazran','_',yazran_id,';')\n"
						+ "                              FROM  diff_taxes_reports.fixed_102_bt  where month=" + i
						+ " and dyear=" + j + "                               on duplicate key update m" + i
						+ "=values(m" + i + ");";
				tr.Insertintodb1(g);

				String h = "insert ignore into " + name_schema + "." + name
						+ "(`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type_for_gemel)"
						+ "                             SELECT  fullname, cid ,`dyear`,`id`, 8000 as Symbol ,concat(yazran_name,' ',type_polisa,' ' , 'מעביד חדש') as SymbolName ,new_maavid,concat(';yazran','_',yazran_id,';')\n"
						+ "                              FROM  diff_taxes_reports.fixed_102_bt  where month=" + i
						+ " and dyear=" + j + "                               on duplicate key update m" + i
						+ "=values(m" + i + ");";
				tr.Insertintodb1(h);

				String k = "insert ignore into " + name_schema + "." + name
						+ "(`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type_for_gemel)"
						+ "                             SELECT  fullname, cid ,`dyear`,`id`, 9000 as Symbol , concat(yazran_name,' ',type_polisa,' ' ,  'פיצוים חדש' )as SymbolName ,new_pizuim,concat(';yazran','_',yazran_id,';')\n"
						+ "                              FROM  diff_taxes_reports.fixed_102_bt  where month=" + i
						+ " and dyear=" + j + "                               on duplicate key update m" + i
						+ "=values(m" + i + ");";
				tr.Insertintodb1(k);

				String l = "insert ignore into " + name_schema + "." + name
						+ "(`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type_for_gemel)"
						+ "                             SELECT  fullname, cid ,`dyear`,`id`, 10000 as Symbol ,concat(yazran_name,' ',type_polisa,' ' ,'סכום חדש') as SymbolName ,sum_new ,concat(';yazran','_',yazran_id,';')\n"
						+ "                              FROM  diff_taxes_reports.fixed_102_bt  where month=" + i
						+ " and dyear=" + j + "                               on duplicate key update m" + i
						+ "=values(m" + i + ");";
				tr.Insertintodb1(l);

			}

		}

	}

	public void insert_into_101_avidar_09() throws SQLException {

		for (int i = 1; i < 13; i++) {

			String a = "insert ignore into shoshana.tbl_101_avidar_girsa\n"
					+ "(cid, dyear, id, FullName, Symbol, SymbolName, m" + i + ", source)\n"
					+ "SELECT '924123740' as cid,dyear,id,full_name ,'1111' as symbol,concat('ברוטו',' ',sug_hafrasha_desc,' ',name_kupa),salary,num_file as source \n"
					+ "FROM shoshana.tbl_sum_avidar_09_2 where   chodesh=" + i + "\n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			t.Insertintodb(a);
			String b = "insert ignore into shoshana.tbl_101_avidar_girsa\n"
					+ "(cid, dyear, id, FullName, Symbol, SymbolName, m" + i + ", source)\n"
					+ "SELECT '924123740' as cid,dyear,id,full_name ,'2222' as symbol,concat('הפרשה חודשית',' ',sug_hafrasha_desc,' ',name_kupa),hafrash_codshit,num_file as source \n"
					+ "FROM shoshana.tbl_sum_avidar_09_2 where   chodesh=" + i + "\n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			t.Insertintodb(b);
			String c = "insert ignore into shoshana.tbl_101_avidar_girsa\n"
					+ "(cid, dyear, id, FullName, Symbol, SymbolName, m" + i + ", source)\n"
					+ "SELECT '924123740' as cid,dyear,id,full_name ,'3333' as symbol,concat('סכום פיצולים',' ',sug_hafrasha_desc,' ',name_kupa),scum_pitzulim,num_file as source \n"
					+ "FROM shoshana.tbl_sum_avidar_09_2 where   chodesh=" + i + "\n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			t.Insertintodb(c);
			String d = "insert ignore into shoshana.tbl_101_avidar_girsa\n"
					+ "(cid, dyear, id, FullName, Symbol, SymbolName, m" + i + ", source)\n"
					+ "SELECT '924123740' as cid,dyear,id,full_name ,'4444' as symbol,concat('שעור  הפרשה',' ',sug_hafrasha_desc,' ',name_kupa),shiur_hafrasha,num_file as source \n"
					+ "FROM shoshana.tbl_sum_avidar_09_2 where   chodesh=" + i + "\n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			t.Insertintodb(d);
		}

	}

	public void change_yatzran() throws SQLException {

		String a = "select in_id, type_for_gemel from `diff_taxes_reports`.`tbl_101`  where cid=924123740 and dyear=2016 and type_for_gemel like '%yazran%'; ";
		ResultSet rs = tr.gettabledb(a);
		while (rs.next()) {
			int id = rs.getInt(1);
			String b = rs.getString(2);

			int dd = b.lastIndexOf("_");

			String d = b.substring(dd + 1, b.length() - 1);

			System.out.println(d);
			String s = "update `diff_taxes_reports`.`tbl_101`  set name_yatzran=" + d
					+ " where cid=924123740 and dyear=2016 and type_for_gemel like '%yazran%' and in_id=" + id;
			tr1.Insertintodb1(s);

		}
	}

	public void change_yatzran1() throws SQLException {

		String a = "select in_id, type_for_gemel from `diff_taxes_reports`.`tbl_101`  where cid=924123740 and dyear=2016 and type_for_gemel like '%yazran%' and name_yatzran is null; ";
		ResultSet rs = tr.gettabledb(a);
		while (rs.next()) {
			int id = rs.getInt(1);
			String b = rs.getString(2);

			int dd = b.indexOf("_");
			int ddd = b.indexOf(";;");
			String d = b.substring(dd + 1, ddd);

			System.out.println(d);
			String s = "update `diff_taxes_reports`.`tbl_101`  set name_yatzran=" + d
					+ " where cid=924123740 and dyear=2016 and type_for_gemel like '%yazran%' and in_id=" + id;
			tr1.Insertintodb1(s);

		}
	}

	public void change_yatzran2() throws SQLException {

		String a = "select in_id, type_for_gemel from Upload_file.tbl_101_for_xml; ";
		ResultSet rs = tr.gettabledb(a);
		while (rs.next()) {
			int id = rs.getInt(1);
			String b = rs.getString(2);

			int dd = b.lastIndexOf("_");

			String d = b.substring(dd + 1, b.length() - 1);

			System.out.println(d);
			String s = "update Upload_file.tbl_101_for_xml  set num_yatzran=" + d + " where  in_id=" + id;
			tr1.Insertintodb1(s);

		}
	}

	public void up_date_cheshbon_bank() throws SQLException {
		String a = " select in_id,  substring(a,8) as mis_heshbon , substring(a,1,2)  as bank,substring(a,4,3) as snif\n"
				+ " from \n" + " (select   replace(replace(hechbon_bank,'  ',' '),'-',' ') as a ,in_id as in_id\n"
				+ " FROM diff_taxes_reports.tbl_yazranim_new) as b  where LENGTH(a)<21;";
		ResultSet rs = tr.gettabledb(a);
		while (rs.next()) {
			int s = rs.getInt(1);
			int c = rs.getInt(2);
			int v = rs.getInt(3);
			int m = rs.getInt(4);
			String h = "update diff_taxes_reports.tbl_yazranim_new set  bank=" + v + " , snif=" + m + " , mis_chechbon="
					+ c + " where in_id =" + s;
			tr1.Insertintodb1(h);
		}
	}

	public void up_date_cheshbon_bank_1() throws SQLException {
		String a = " select in_id,  substring(a,8) as mis_heshbon , substring(a,1,2)  as bank,substring(a,4,3) as snif\n"
				+ " from \n" + " (select   replace(replace(hechbon_bank,'  ',' '),'-',' ') as a ,in_id as in_id\n"
				+ " FROM naomi.tbl_yazranim_new_2) as b  where LENGTH(a)<21;";
		ResultSet rs = tr.gettabledb(a);
		while (rs.next()) {
			int s = rs.getInt(1);
			int c = rs.getInt(2);
			int v = rs.getInt(3);
			int m = rs.getInt(4);
			String h = "update naomi.tbl_yazranim_new_2 set  bank=" + v + " , snif=" + m + " , mis_chechbon=" + c
					+ " where in_id =" + s;
			tr1.Insertintodb1(h);
		}
	}

	void load_data_avidar(String name_schema, String name_table) throws SQLException {

		//
		for (int y = 2009; y < 2017; y++) {
			for (int i = 1; i < 13; i++) {
				if (y != 2016 && i != 12)

				{
					String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/moke_emon_nikayon/" + y + "/" + i
							+ ".csv'" + "   INTO TABLE  " + name_schema + "." + name_table + ""
							+ "  charset hebrew  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
							+ "  ( kod_kupa, name_kupa, ofi, sug_kupa, kvutza, tear_kvutza, kod_tat_mifal, tear_tat_mifal, kod_machlaka, tear_machlaka, kod_agaf, tear_agaf, mis_oved, id, tarich_hatchalat_avoda, mispar_amit, tarich_hitzarfut, achoz, shaot_yme_avoda, nicoy, hafrasha, pitzuim, achoz_nicoy_befoal, achoz_hafrash_befoal, achoz_pitzuim_befoal, chodesh_sacar, sug_divoch, avor_chodesh, sug_chishov, broto_lechishov, nicoy_oved, hafrashat_mavid, hafrasha_lepitzuim, ovdan, kupa, sach_lekupa_a_cosher, sach_lekupa, zkifa, mimshak, tear_mimmshak, sochan, tear_sochen, kupa_baotzar, name_kupa_baotzar, sug_niman, hp_niman, shem_niman, girsa, dyear, m)"
							+ "set dyear=" + y + ",m=" + i + ";";

					tr.Insertintodb1(a);

				} else {

					String aa = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/moke_emon_nikayon/" + y + "/"
							+ i + ".csv'" + "   INTO TABLE  " + name_schema + "." + name_table + ""
							+ "  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
							+ "  ( kod_kupa, name_kupa, ofi, sug_kupa, kvutza, tear_kvutza, kod_tat_mifal, tear_tat_mifal, kod_machlaka, tear_machlaka, kod_agaf, tear_agaf, mis_oved, id,  tarich_hatchalat_avoda, mispar_amit, tarich_hitzarfut, achoz, shaot_yme_avoda, nicoy, hafrasha, pitzuim, achoz_nicoy_befoal, achoz_hafrash_befoal, achoz_pitzuim_befoal, chodesh_sacar, sug_divoch, avor_chodesh, sug_chishov, broto_lechishov, nicoy_oved, hafrashat_mavid, hafrasha_lepitzuim, ovdan, kupa, sach_lekupa_a_cosher, sach_lekupa, zkifa, mimshak, tear_mimmshak, sochan, tear_sochen, kupa_baotzar, name_kupa_baotzar, sug_niman, hp_niman, shem_niman, girsa, dyear, m)"
							+ "set dyear=" + y + ",m=" + i + ";";

					tr.Insertintodb1(aa);
				}
			}
		}

	}

	void load_data_pi(String name_schema, String name_table) throws SQLException {

		for (int y = 2013; y < 2016; y++) {
			for (int i = 1; i < 13; i++) {

				String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/avidar1/" + y + "/" + i + ".csv'"
						+ "   INTO TABLE  " + name_schema + "." + name_table + ""
						+ "  charset hebrew  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
						+ "  ( kod_kupa, name_kupa, ofi, sug_kupa, kvutza, tear_kvutza, kod_tat_mifal, tear_tat_mifal, kod_machlaka, tear_machlaka, kod_agaf, tear_agaf, mis_oved, id, l_name, f_name, tarich_hatchalat_avoda, mispar_amit, tarich_hitzarfut, achoz, shaot_yme_avoda, nicoy, hafrasha, pitzuim, achoz_nicoy_befoal, achoz_hafrash_befoal, achoz_pitzuim_befoal, chodesh_sacar, sug_divoch, avor_chodesh, sug_chishov, broto_lechishov, nicoy_oved, hafrashat_mavid, hafrasha_lepitzuim, ovdan, kupa, sach_lekupa_a_cosher, sach_lekupa, zkifa, mimshak, tear_mimmshak, sochan, tear_sochen, kupa_baotzar, name_kupa_baotzar, sug_niman, hp_niman, shem_niman, girsa, dyear, m)"
						+ "set dyear=" + y + ",m=" + i + ";";

				tr.Insertintodb1(a);
			}
		}

	}

	public void insert_into_101_avidara(String name_schema, String name_schema1, String name_table, String name,
			int cid) throws SQLException

	{
		for (int i = 1; i < 13; i++) {

			String a = "insert into " + name_schema + "." + name_table
					+ "  (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source,date_value,name_yatzran)\n" + " SELECT  concat(f_name,' ',l_name)," + cid
					+ ",dyear,id,99,concat(  'ברוטו לחישוב',':',name_kupa,' ', sug_kupa  ),\n"
					+ "  if (`broto_lechishov`=0,0,`broto_lechishov`) ,'excel',sug_kupa ,num_yatzran FROM "
					+ name_schema1 + "." + name + "  as a  where m=" + i + " \n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			t.Insertintodb(a);

			String b = "insert into  " + name_schema + "." + name_table
					+ "  (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source,date_value,name_yatzran)\n" + " SELECT  concat(f_name,' ',l_name)," + cid
					+ ",dyear,id,88,concat(  'ניכוי עובד',':',name_kupa,' ', sug_kupa  ),\n"
					+ " if (`nicoy_oved`=0,0,`nicoy_oved`)    ,'excel',sug_kupa ,num_yatzran FROM " + name_schema1 + "."
					+ name + "  as a  where m=" + i + " \n" + "on duplicate key update m" + i + "=values(m" + i + ")";
			t.Insertintodb(b);

			String c = "insert into  " + name_schema + "." + name_table
					+ "  (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source,date_value,name_yatzran)\n" + " SELECT  concat(f_name,' ',l_name)," + cid
					+ ",dyear,id,77,concat(  'הפרשת מעביד',':',name_kupa,' ', sug_kupa  ),\n"
					+ "  if (`hafrashat_mavid`=0,0,`hafrashat_mavid`)   ,'excel' ,sug_kupa ,num_yatzran FROM "
					+ name_schema1 + "." + name + "  as a  where m=" + i + " \n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			t.Insertintodb(c);

			String d = "insert into  " + name_schema + "." + name_table
					+ "  (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source,date_value,name_yatzran)\n" + " SELECT  concat(f_name,' ',l_name)," + cid
					+ ",dyear,id,66,concat(  'הפרשה לפיצוים',':',name_kupa,' ', sug_kupa  ),\n"
					+ "   if (`hafrasha_lepitzuim`=0,0,`hafrasha_lepitzuim`)   ,'excel' ,sug_kupa ,num_yatzran FROM "
					+ name_schema1 + "." + name + "  as a  where m=" + i + " \n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			t.Insertintodb(d);

			String e = "insert into  " + name_schema + "." + name_table
					+ "  (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source,date_value,name_yatzran)\n" + " SELECT  concat(f_name,' ',l_name)," + cid
					+ ", dyear,id,55,concat(  'אחוז ניכוי בטבלה',':',name_kupa,' ', sug_kupa  ),\n"
					+ "   if (`nicoy`=0,0,`nicoy`)  ,'excel',sug_kupa ,num_yatzran FROM " + name_schema1 + "." + name
					+ "  as a  where m=" + i + " \n" + "on duplicate key update m" + i + "=values(m" + i + ")";
			t.Insertintodb(e);

			String f = "insert into  " + name_schema + "." + name_table
					+ "  (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source,date_value,name_yatzran)\n" + " SELECT  concat(f_name,' ',l_name)," + cid
					+ ", dyear,id,44,concat(  'אחוז הפרשה בטבלה',':',name_kupa,' ', sug_kupa  ),\n"
					+ "     if (`hafrasha`=0,0,`hafrasha`),'excel',sug_kupa ,num_yatzran FROM " + name_schema1 + "."
					+ name + "  as a  where m=" + i + " \n" + "on duplicate key update m" + i + "=values(m" + i + ")";
			t.Insertintodb(f);

			String g = "insert into  " + name_schema + "." + name_table
					+ " (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source,date_value,name_yatzran)\n" + " SELECT  concat(f_name,' ',l_name), " + cid
					+ ", dyear,id,33,concat(  'אחוז פיצוים בטבלה',':',name_kupa,' ', sug_kupa  ),\n"
					+ "  if (`pitzuim` =0,0,`pitzuim` ),'excel',sug_kupa ,num_yatzran FROM " + name_schema1 + "." + name
					+ "  as a  where m=" + i + " \n" + "on duplicate key update m" + i + "=values(m" + i + ")";
			t.Insertintodb(g);

			String h = "insert into  " + name_schema + "." + name_table
					+ "  (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source,date_value,name_yatzran)\n" + " SELECT  concat(f_name,' ',l_name)," + cid
					+ ",dyear,id,22,concat(  'אחוז ניכוי בפועל',':',name_kupa,' ', sug_kupa  ),\n"
					+ "   if (`achoz_nicoy_befoal`  =0,0,`achoz_nicoy_befoal`  ) ,'excel' ,sug_kupa ,num_yatzran FROM "
					+ name_schema1 + "." + name + "  as a  where m=" + i + " \n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			t.Insertintodb(h);

			String k = "insert into  " + name_schema + "." + name_table
					+ "  (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source,date_value,name_yatzran)\n" + " SELECT  concat(f_name,' ',l_name)," + cid
					+ ",dyear,id,11,concat(  'אחוז הפרשה בפועל',':',name_kupa,' ', sug_kupa  ),\n"
					+ "   if (`achoz_hafrash_befoal`  =0,0,`achoz_hafrash_befoal`  ),'excel' ,sug_kupa ,num_yatzran FROM "
					+ name_schema1 + "." + name + "  as a  where m=" + i + " \n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			t.Insertintodb(k);

			String l = "insert into  " + name_schema + "." + name_table
					+ "  (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i
					+ ",source,date_value,name_yatzran)\n" + " SELECT  concat(f_name,' ',l_name)," + cid
					+ ",dyear,id,12,concat(  'אחוז פיצוים בפועל',':',name_kupa,' ', sug_kupa  ),\n"
					+ "   if (`achoz_pitzuim_befoal`  =0,0,`achoz_pitzuim_befoal`  ) ,'excel' ,sug_kupa,num_yatzran FROM "
					+ name_schema1 + "." + name + "  as a  where m=" + i + " \n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";

			t.Insertintodb(l);

			// String b="insert into shoshana.tbl_101_ikea2
			// (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"+i+",source)\n"
			// +
			// " SELECT full_name,430,dyear,id,88,concat( 'ברוטו',' : ',
			// sug_hafrasha_desc ),\n" +
			// " `salary` ,`num_file` FROM shoshana.tbl_sum_ikea_sofi as a where
			// chodesh="+i+" \n" +
			// "on duplicate key update m"+i+"=values(m"+i+")";
			// t.Insertintodb(b);
			//
			// String c="insert into shoshana.tbl_101_ikea2
			// (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"+i+",source)\n"
			// +
			// " SELECT full_name,430,dyear,id,77,concat( 'סכום פיצולים' ,' : ',
			// sug_hafrasha_desc ),\n" +
			// " `scum_pitzulim` ,`num_file` FROM shoshana.tbl_sum_ikea_sofi as
			// a where chodesh="+i+" \n" +
			// "on duplicate key update m"+i+"=values(m"+i+")";
			// t.Insertintodb(c);
			// String d=" insert into shoshana.tbl_101_ikea2
			// (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m"+i+",source)\n"
			// +
			// " SELECT full_name,430,dyear,id,66,concat('שעור הפרשה' ,' : ',
			// sug_hafrasha_desc ),\n" +
			// " `shiur_hafrasha` ,`num_file` FROM shoshana.tbl_sum_ikea_sofi as
			// a where chodesh="+i+" \n" +
			// "on duplicate key update m"+i+"=values(m"+i+")";
			// t.Insertintodb(d);

		}
	}

	public void update_broto1(String table) throws SQLException {

		String a = " update diff_taxes_reports." + table + " \n" + "set type_for_gemel=';bruto_pensya;'\n"
				+ "where Symbol =99 and date_value='פנסיונית'  ;";
		t.Insertintodb(a);
		String b = "update diff_taxes_reports." + table + " \n" + "set type_for_gemel=';bruto_gemel;'\n"
				+ "where Symbol =99 and date_value='תגמולים'  ;";
		t.Insertintodb(b);
		String c = "update diff_taxes_reports." + table + " \n" + "set type_for_gemel=';bruto_k_hish;'\n"
				+ "where Symbol =99 and date_value='ק.השתלמות'  ;";
		t.Insertintodb(c);

		String d = "update diff_taxes_reports." + table + " \n" + "set type_for_gemel=';bruto_gemel;'\n"
				+ "where Symbol =99 and date_value=1  ;";
		t.Insertintodb(d);

	}

	public void update_achoz_pensai(String table) throws SQLException {

		String a = "update\n" + "diff_taxes_reports." + table + "\n" + "set type_for_gemel=';pre_pensya_maavid;'\n"
				+ "where   Symbol =11 and date_value='פנסיונית';";
		t.Insertintodb(a);
		String b = "update diff_taxes_reports.`" + table + "`\n" + "set type_for_gemel=';pre_pensya_oved;'\n"
				+ " where   Symbol =22 and date_value='פנסיונית';";
		t.Insertintodb(b);
		String c = "update diff_taxes_reports.`" + table + "`\n" + "set type_for_gemel=';pre_pensya_pizoim;'\n"
				+ "where   Symbol =12 and date_value='פנסיונית';";
		t.Insertintodb(c);

	}

	public void update_achoz_keren_hishtalmut(String table) throws SQLException {

		String a = "update\n" + "diff_taxes_reports." + table + "\n" + "set type_for_gemel=';pre_k_hish_maavid;'\n"
				+ "where   Symbol =11 and date_value='ק.השתלמות';";
		t.Insertintodb(a);
		String b = "update diff_taxes_reports.`" + table + "`\n" + "set type_for_gemel=';pre_k_hish_oved;'\n"
				+ "where   Symbol =22 and date_value='ק.השתלמות';";
		t.Insertintodb(b);
		// String c="update shoshana.`"+table+"`\n" +
		// "set type=';pre_pensya_pizoim;'\n" +
		// "where SymbolName like '%אחוז הפרשה : פיצוים%' and permission=4;";
		// t.Insertintodb(c);

	}

	public void update_achoz_gemel(String table) throws SQLException {

		String a = "update\n" + "diff_taxes_reports." + table + "\n" + "set type_for_gemel=';pre_gemel_maavid;'\n"
				+ "where   Symbol =11 and date_value='תגמולים';";
		t.Insertintodb(a);
		String b = "update diff_taxes_reports.`" + table + "`\n" + "set type_for_gemel=';pre_gemel_oved;'\n"
				+ " where   Symbol =22 and date_value='תגמולים';";
		t.Insertintodb(b);
		String c = "update diff_taxes_reports.`" + table + "`\n" + "set type_for_gemel=';pre_gemel_pizoim;'\n"
				+ "where   Symbol =12 and date_value='תגמולים';";
		t.Insertintodb(c);

	}

	public void update_scom_pitzulim_pensia(String table) throws SQLException {

		String a = "update  diff_taxes_reports." + table + "\n" + "\n" + "set type_for_gemel=';shulam_maavid;gemel;'\n"
				+ "\n" + " where   Symbol =77  and date_value='תגמולים';";
		t.Insertintodb(a);

		String b = "update  diff_taxes_reports." + table + "\n"
				+ "set type_for_gemel=';shulam_maavid;pensya;sum_shulam;'\n"
				+ " where  Symbol =77 and date_value='פנסיונית';";
		t.Insertintodb(b);

		String c = "update  diff_taxes_reports." + table + "\n"
				+ "set type_for_gemel=';shulam_maavid;k_hish;sum_shulam;'\n" + "\n"
				+ "where  Symbol =77 and date_value='ק.השתלמות';\n";

		t.Insertintodb(c);

		String d = "update  diff_taxes_reports." + table + "\n"
				+ "set type_for_gemel=';shulam_oved;gemel;sum_shulam;'\n"
				+ " where  Symbol =88 and date_value='תגמולים';";
		t.Insertintodb(d);
		String e = "update  diff_taxes_reports." + table + "\n"
				+ "set type_for_gemel=';shulam_oved;pensya;sum_shulam;'\n"
				+ " where   Symbol =88 and date_value='פנסיונית';\n";
		t.Insertintodb(e);
		String f = "update  diff_taxes_reports." + table + "\n"
				+ "set type_for_gemel=';shulam_oved;k_hish;sum_shulam;'\n"
				+ "where   Symbol =88 and date_value='ק.השתלמות';";

		t.Insertintodb(f);

		String aa = "update  diff_taxes_reports." + table + "\n" + "\n" + "set type_for_gemel=';shulam_maavid;gemel;'\n"
				+ "\n" + " where   Symbol =66  and date_value='תגמולים';";
		t.Insertintodb(aa);

		String bb = "update  diff_taxes_reports." + table + "\n"
				+ "set type_for_gemel=';shulam_maavid;pensya;sum_shulam;'\n"
				+ " where  Symbol =66 and date_value='פנסיונית';";
		t.Insertintodb(bb);

		String cc = "update  diff_taxes_reports." + table + "\n"
				+ "set type_for_gemel=';shulam_maavid;k_hish;sum_shulam;'\n" + "\n"
				+ "where  Symbol =66 and date_value='ק.השתלמות';\n";

		t.Insertintodb(cc);

	}

	void update_yatzran(String name) throws SQLException {

		String a = "update diff_taxes_reports." + name + "\n"
				+ " set type_for_gemel=concat(type_for_gemel ,';','yazran_','',name_yatzran,';');";
		tr.Insertintodb(a);
		String b = "update  diff_taxes_reports." + name
				+ " set type_for_gemel=concat(';','yazran_','',name_yatzran,';')  where type_for_gemel is null;";
		tr.Insertintodb(b);

	}

	void update_num_yatzran(String name_schema, String tbl_avidar_next) throws SQLException {
		String a = "update \n" + "" + name_schema + "." + tbl_avidar_next
				+ " as a left join diff_taxes_reports.tbl_yazranim_new as b\n"
				+ "on a.kupa_baotzar COLLATE utf8_unicode_ci =b.mis_h_p_hevra_menaelet \n"
				+ "or a.kupa_baotzar COLLATE utf8_unicode_ci=b.mis_ishur \n" + " set a.num_yatzran=b.in_id;";
		tr.Insertintodb(a);
	}

	public void Malam_load_data_fanitzia1(String name_schema, int year, String name_table, int name_file, String kidod)
			throws SQLException {
		/// home/shoshana/NetBeansProjects/JavaAppli
		String a = "LOAD DATA  LOCAL INFILE " + " '/home/shoshana/Downloads/bikor_rofe/" + name_file + ".csv'"
				+ "    INTO TABLE " + name_schema + ".`" + name_table + "` \n" + kidod
				+ "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n" + "	LINES TERMINATED BY '\\n' \n"
				+ "     IGNORE 1 LINES\n"
				+ "(  num_oved, id, seif_taktzivi, Symbol, Symbol_name, m1_s, m1_c, m2_s, m2_c, m3_s, m3_c, m4_s, m4_c, m5_s, m5_c, m6_s, m6_c, m7_s, m7_c, m8_s, m8_c, m9_s, m9_c, m10_s, m10_c, m11_s, m11_c, m12_s, m12_c,dyear)"
				// + " ( office_num, office_name, `name`, numWorker, idWorker,
				// m_n, derug, darga, start_date, vetek, department, unit,
				// symbol_type, symbolName, m1, m1_amount, m2, m2_amount, m3,
				// m3_amount, m4, m4_amount, m5, m5_amount, m6, m6_amount, m7,
				// m7_amount, m8, m8_amount, m9, m9_amount, m10, m10_amount,
				// m11, m11_amount, m12, m12_amount, all_sum, all_amount)\n"
				+ "     set dyear=" + year;
		tr.Insertintodb1(a);

	}

	public void Malam_insert_into_temp_isrotel1(String name_schema, String name_table, String name_table1, int year,
			int cid) throws SQLException {

		String a = "INSERT  INTO `" + name_schema + "`.`" + name_table + "`\n" + "(\n" + "`cid`,\n" + "`dyear`,\n"
				+ "`id`,\n" + "`FullName`,\n" + "`Symbol`,\n" + "`SymbolName`,\n"

				+ "`m1`,\n" + "`m2`,\n" + "`m3`,\n" + "`m4`,\n" + "`m5`,\n" + "`m6`,\n" + "`m7`,\n" + "`m8`,\n"
				+ "`m9`,\n" + "`m10`,\n" + "`m11`,\n" + "`m12`\n" + ")SELECT \n" + cid + "," + year
				+ ", `id`,  concat(f_name,'',l_name),\n"
				+ "   `Symbol`,`Symbol_name`,`m1_s`,`m2_s`,`m3_s`,`m4_s`,`m5_s`,`m6_s`,`m7_s`,`m8_s`,`m9_s`,`m10_s`,`m11_s`,`m12_s`\n"
				+ "  \n" + "FROM  `" + name_schema + "`.`" + name_table1 + "`;";
		// + "select\n"
		// + "dyear,idWorker, `name`,\n"
		// + "SUBSTRING_INDEX(SymbolName,'-',1),\n"
		// +
		// "SUBSTR(SymbolName,instr(SymbolName,'-')+1,LENGTH(SymbolName)),`symbol_type`,\n"
		// + "sum(m1) as m1, sum(m2) as m2, sum(m3) as m3, sum(m4) as m4,
		// sum(m5) as m5, sum(m6) as m6, sum(m7) as m7, sum(m8) as m8,\n"
		// + "sum(m9) as m9, sum(m10) as m10, sum(m11) as m11, sum(m12) as
		// m12\n"
		// + "from " + name_schema + "." + name_table1 + "\n"
		// + "group by dyear,idWorker,SymbolName,`symbol_type` \n"
		// + " on duplicate key update
		// m1=values(m1),m2=values(m2),m3=values(m3),m4=values(m4),m5=values(m5),m6=values(m6),m7=values(m7),m8=values(m8),m9=values(m9),m10=values(m10),m11=values(m11),m12=values(m12);";
		tr.Insertintodb1(a);

	}

	void create_zol_vbegadol_10_2016(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists  `Upload_file`.`tbl_bituch_lemui_mavid` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `full_name` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n" + "  `min` VARCHAR(45) NULL,\n"
				+ "  `m1` VARCHAR(45) NULL,\n" + "  `m2` VARCHAR(45) NULL,\n" + "  `m3` VARCHAR(45) NULL,\n"
				+ "  `m4` VARCHAR(45) NULL,\n" + "  `m5` VARCHAR(45) NULL,\n" + "  `m6` VARCHAR(45) NULL,\n"
				+ "  `m7` VARCHAR(45) NULL,\n" + "  `m8` VARCHAR(45) NULL,\n" + "  `m9` VARCHAR(45) NULL,\n"
				+ "  `m10` VARCHAR(45) NULL,\n" + "  `m11` VARCHAR(45) NULL,\n" + "  `m12` VARCHAR(45) NULL,\n"
				+ "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(a);

	}

	void create_tbl_bituch_lemi_ma(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists  `Upload_file`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" +

				"  `mis_oved` VARCHAR(45) NULL,\n" + "  `m1` VARCHAR(45) NULL,\n" + "  `m2` VARCHAR(45) NULL,\n"
				+ "  `m3` VARCHAR(45) NULL,\n" + "  `m4` VARCHAR(45) NULL,\n" + "  `m5` VARCHAR(45) NULL,\n"
				+ "  `m6` VARCHAR(45) NULL,\n" + "  `m7` VARCHAR(45) NULL,\n" + "  `m8` VARCHAR(45) NULL,\n"
				+ "  `m9` VARCHAR(45) NULL,\n" + "  `m10` VARCHAR(45) NULL,\n" + "  `m11` VARCHAR(45) NULL,\n"
				+ "  `m12` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(a);

	}

	// /home/shoshana/Downloads/traklin/bituch_leomi_m
	// /home/shoshana/Downloads/paltrin/bituch_leomi_m

	public void load_data1(String name_schema, String name_tabel, int dyear) throws SQLException {
		// name_tabel = name_tabel;

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/magen_avraham_bil/" + dyear + ".csv'"
				+ " INTO TABLE  `" + name_schema + "`." + name_tabel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 3 LINES "
				+ "  (  mis_oved,  m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12)" + "set dyear=" + dyear;

		tr.Insertintodb1(a);

	}

	public void insert_bl(String name_schema, String name_tabel, String name_tabel_101, int cid) throws SQLException {

		String k = "insert into Upload_file." + name_tabel_101
				+ "(`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12)\n"
				+ " SELECT 'a'," + cid + ",dyear,mis_oved,11,'ביטוח לאומי מעביד'\n"
				+ " ,m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12 FROM  Upload_file." + name_tabel + "";
		tr.Insertintodb1(k);

	}

	public void Malam_load_data_fanitziaa(String name_schema, int year, String name_table, int name_file, String kidod,
			int cid) throws SQLException {
		/// home/shoshana/NetBeansProjects/JavaAppli
		String a = "LOAD DATA  LOCAL INFILE " + " '/home/shoshana/Downloads/" + name_file + ".csv'" + "    INTO TABLE "
				+ name_schema + ".`" + name_table + "` \n" + kidod + "    FIELDS TERMINATED BY ','  ENCLOSED BY '\"'\n"
				+ "	LINES TERMINATED BY '\\n' \n" + "     IGNORE 1 LINES\n"
				+ "( F_name, L_name, num_oved, id, seif_taktzivi, Symbol, Symbol_name, m1_s, m1_c, m2_s, m2_c, m3_s, m3_c, m4_s, m4_c, m5_s, m5_c, m6_s, m6_c, m7_s, m7_c, m8_s, m8_c, m9_s, m9_c, m10_s, m10_c, m11_s, m11_c, m12_s, m12_c, dyear, cid)"
				// + " ( office_num, office_name, `name`, numWorker, idWorker,
				// m_n, derug, darga, start_date, vetek, department, unit,
				// symbol_type, symbolName, m1, m1_amount, m2, m2_amount, m3,
				// m3_amount, m4, m4_amount, m5, m5_amount, m6, m6_amount, m7,
				// m7_amount, m8, m8_amount, m9, m9_amount, m10, m10_amount,
				// m11, m11_amount, m12, m12_amount, all_sum, all_amount)\n"
				+ "     set dyear=" + year + " , cid=" + cid;
		tr.Insertintodb1(a);

	}

	void update_pitzulim(String name_schema, String table_101) throws SQLException {
		String a = " update  " + name_schema + "." + table_101
				+ " set type_for_gemel=concat(ifnull(type_for_gemel,''),'',';sikum_pitzulim;') where Symbol in (77,66,88) ;";
		t.Insertintodb(a);
	}

	void a() {

	}

	void insert_into_101_isrotel1(String name_scema, String name_table_101, String name_table_temp)
			throws SQLException {
		String a = "INSERT INTO  `" + name_scema + "`.`" + name_table_101 + "`\n" + "(\n" + "`cid`,\n" + "`dyear`,\n"
				+ "`id`,\n" +

				"`FullName`,\n" + "`Symbol`,\n" + "`SymbolName`,\n" + "`m1`,\n" + "`m2`,\n" + "`m3`,\n" + "`m4`,\n"
				+ "`m5`,\n" + "`m6`,\n" + "`m7`,\n" + "`m8`,\n" + "`m9`,\n" + "`m10`,\n" + "`m11`)\n" +

				"SELECT \n" + "  `cid`\n" + "  ,`dyear`,\n" + "    `id`,\n" + "    `FullName`,\n" + "  `Symbol`,\n"
				+ "   `SymbolName`,\n"
				+ "sum(m1) as m1, sum(m2) as m2, sum(m3) as m3, sum(m4) as m4, sum(m5) as m5, sum(m6) as m6, sum(m7) as m7, sum(m8) as m8,\n"
				+ "sum(m9) as m9, sum(m10) as m10, sum(m11) as m11\n" + "FROM  `" + name_scema + "`.`" + name_table_temp
				+ "`" + "group by  cid,  dyear, id, Symbol, SymbolName \n";

		tr.Insertintodb1(a);

	}

	String name_kupa = "", name_kupaa = "פנסיה";

	int mis_oved = 0, month = 0, dyear = 0;
	double nicoy_oved = 0, hafrashat_mavid = 0, hfrashat_pitzuim = 0, bruto = 0, scum_nicoy_oved = 0,
			scum_hafrashat_mavid = 0, scum_hfrashat_pitzuim = 0;

	String id = "no", name = "no";

	public void getAll() throws IOException, SQLException {
		for (int j = 2009; j < 2016; j++) {

			System.out.println("f");

			for (int i = 1; i < 13; i++) {

				String nameFile = "/home/shoshana/Downloads/zol_vbegadol_bit/" + j + "/" + i + "_" + i + ".csv";

				BufferedReader br = csvFileHelper.readFileutf(nameFile);
				String[] part;
				// br.readLine();

				for (String line = br.readLine(); line != null; line = br.readLine()) {

					part = line.split(",");
					if (!line.contains("דוח קופות גמל")) {
						if (line.contains("מספר עמית") || line.equals(",,,,,,,,,,,,,,") || line.contains("סה")) {
							continue;
						}

						name = part[3];
						id = part[0].replace("-", "");
						if (id.equals("") || id.equals(null) || id == null) {

							id = "0";

						}
						if (name.contains("אוהב")) {

							id = "0";
						}

						mis_oved = Integer.parseInt(part[2]);

						bruto = ParseDouble(part[7]);
						if (bruto != 0) {
							scum_nicoy_oved = ParseDouble(part[8]);
							scum_hafrashat_mavid = ParseDouble(part[10]);

							scum_hfrashat_pitzuim = ParseDouble(part[11]);

							nicoy_oved = ParseDouble(part[8]) / ParseDouble(part[7]);
							hafrashat_mavid = ParseDouble(part[10]) / ParseDouble(part[7]);

							hfrashat_pitzuim = ParseDouble(part[11]) / ParseDouble(part[7]);
						}
						month = i;
						dyear = j;
						name_kupaa = name_kupa;

						String a = " INSERT INTO `Upload_file`.`tbl_zol_vbegadol_betuch`\n" + "(\n" + "`id`,\n"
								+ "`mis_oved`,\n" + "`name`,\n" + "`bruto`,\n" + "`name_kupaa`,\n" + "`nicoy_oved`,\n"
								+ "`hafrashat_mavid`,\n" + "`hfrashat_pitzuim`,\n" + "`scum_nicoy_oved`,\n"
								+ "`scum_hafrashat_mavid`,\n" + "`scum_hfrashat_pitzuim`,\n" + "`month`,\n"
								+ "`dyear`)\n" + "VALUES  ( " + id + "," + mis_oved + ",'" + escapeSQL(name) + "',"
								+ bruto + ",'" + escapeSQL(name_kupaa) + "'," + nicoy_oved + "," + hafrashat_mavid + ","
								+ hfrashat_pitzuim + "," + scum_nicoy_oved + "," + scum_hafrashat_mavid + ","
								+ scum_hfrashat_pitzuim + "," + month + "," + dyear + " )";

						tr.Insertintodb1(a);

					} else {
						int xx = line.indexOf("/");
						String get3 = line.substring(xx + 4, xx + 5);
						String get2 = line.substring(xx + 5, xx + 6);
						String get1 = line.substring(xx + 3, xx + 4);
						String get11 = line.substring(xx + 2, xx + 3);
						String x = line.replace("דוח קופות גמל", "");
						String get = x.replaceAll("[0-9]", "");
						name_kupa = get.trim().replace("/", "").replace("-", "").trim().replace(",,,,,,,,,,,,,,", "");
						// name_kupa=get.trim().replace(",,,,,,,,,,,,,,",
						// "").replace("/", "").replace("-", " ").trim();
						// month=get1;

					}

				}

			}

		}
	}

	double ParseDouble(String strNumber) {
		if (strNumber != null && strNumber.length() > 0) {
			try {
				return Double.parseDouble(strNumber);
			} catch (Exception e) {
				return -1; // or some value to mark this field is wrong. or make
							// a function validates field first ...
			}
		} else
			return 0;
	}

	Integer ParseInteger(String strNumber) {
		if (strNumber != null && strNumber.length() > 0) {
			try {
				return Integer.parseInt(strNumber);
			} catch (Exception e) {
				return -1; // or some value to mark this field is wrong. or make
							// a function validates field first ...
			}
		} else
			return 0;

	}

	public void convert_to_101_be(String name_schema, String name, String name_table, int cid) throws SQLException {
		for (int i = 1; i < 13; i++) {
			String load = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT  name," + cid
					+ ",`dyear`,`mis_oved`, '99',concat('ברוטו',' ',name_kupaa),bruto\n" + "            FROM  "
					+ name_schema + "." + name_table + "  where month=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load);

			String load1 = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT  name," + cid
					+ ",`dyear`,`mis_oved`, '55',concat('אחוז ניכוי עובד',' ',name_kupaa),nicoy_oved\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where month=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load1);

			String load2 = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT  name," + cid
					+ ",`dyear`,`mis_oved`, '44',concat('אחוז הפרשת מעביד',' ',name_kupaa),hafrashat_mavid\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where month=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load2);

			String load3 = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT  name," + cid
					+ ",`dyear`,`mis_oved`, '33',concat('אחוז הפרשת פיצוים',' ',name_kupaa),hfrashat_pitzuim\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where month=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load3);
			String load4 = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT  name," + cid
					+ ",`dyear`,`mis_oved`, '88',concat('ניכוי עובד',' ',name_kupaa),scum_nicoy_oved\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where month=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load4);

			String load5 = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT  name," + cid
					+ ",`dyear`,`mis_oved`, '77',concat('הפרשת מעביד',' ',name_kupaa),scum_hafrashat_mavid\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where month=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load5);

			String load6 = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT  name," + cid
					+ ",`dyear`,`mis_oved`, '66',concat('הפרשת פיצוים',' ',name_kupaa),scum_hfrashat_pitzuim\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where month=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load6);
		}

	}

	public void getAll_ni() throws IOException, SQLException

	{
		for (int j = 2009; j < 2016; j++) {

			System.out.println("f");

			for (int i = 1; i < 13; i++) {

				String nameFile = "/home/shoshana/Downloads/avidar_nifraim/ayalon/5.csv";

				// BufferedReader br = csvFileHelper.readFile(nameFile);
				BufferedReader br = csvFileHelper.readFileutf(nameFile);
				String[] part;
				// br.readLine();

				for (String line = br.readLine(); line != null; line = br.readLine()) {

					part = line.split(",");
					if (part.length == 0)
						continue;
					if (!line.contains("דוח קופות גמל")) {
						if (line.contains("מספר עמית") || line.equals(",,,,,,,,,,,,,,") || line.contains("סה")) {
							continue;
						}

						name = part[3];
						id = part[0].replace("-", "");
						if (id.equals("") || id.equals(null) || id == null) {

							id = "0";

						}
						if (name.contains("אוהב")) {

							id = "0";
						}

						mis_oved = Integer.parseInt(part[2]);

						bruto = ParseDouble(part[7]);
						if (bruto != 0) {
							scum_nicoy_oved = ParseDouble(part[8]);
							scum_hafrashat_mavid = ParseDouble(part[10]);

							scum_hfrashat_pitzuim = ParseDouble(part[11]);

							nicoy_oved = ParseDouble(part[8]) / ParseDouble(part[7]);
							hafrashat_mavid = ParseDouble(part[10]) / ParseDouble(part[7]);

							hfrashat_pitzuim = ParseDouble(part[11]) / ParseDouble(part[7]);
						}
						month = i;
						dyear = j;
						name_kupaa = name_kupa;

						String a = " INSERT INTO `Upload_file`.`tbl_zol_vbegadol_betuch`\n" + "(\n" + "`id`,\n"
								+ "`mis_oved`,\n" + "`name`,\n" + "`bruto`,\n" + "`name_kupaa`,\n" + "`nicoy_oved`,\n"
								+ "`hafrashat_mavid`,\n" + "`hfrashat_pitzuim`,\n" + "`scum_nicoy_oved`,\n"
								+ "`scum_hafrashat_mavid`,\n" + "`scum_hfrashat_pitzuim`,\n" + "`month`,\n"
								+ "`dyear`)\n" + "VALUES  ( " + id + "," + mis_oved + ",'" + escapeSQL(name) + "',"
								+ bruto + ",'" + escapeSQL(name_kupaa) + "'," + nicoy_oved + "," + hafrashat_mavid + ","
								+ hfrashat_pitzuim + "," + scum_nicoy_oved + "," + scum_hafrashat_mavid + ","
								+ scum_hfrashat_pitzuim + "," + month + "," + dyear + " )";

						tr.Insertintodb1(a);

					} else {
						int xx = line.indexOf("/");
						String get3 = line.substring(xx + 4, xx + 5);
						String get2 = line.substring(xx + 5, xx + 6);
						String get1 = line.substring(xx + 3, xx + 4);
						String get11 = line.substring(xx + 2, xx + 3);
						String x = line.replace("דוח קופות גמל", "");
						String get = x.replaceAll("[0-9]", "");
						name_kupa = get.trim().replace("/", "").replace("-", "").trim().replace(",,,,,,,,,,,,,,", "");
						// name_kupa=get.trim().replace(",,,,,,,,,,,,,,",
						// "").replace("/", "").replace("-", " ").trim();
						// month=get1;

					}

				}

			}

		}
	}

	public void load_data2(String name_schema, String name_tabel, int dyear) throws SQLException {
		// name_tabel = name_tabel;

		String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/avidar_nifraim/migdal/5.CSV'"
				+ " INTO TABLE  `" + name_schema + "`." + name_tabel
				+ " CHARACTER SET hebrew  FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 4 LINES "
				+ "  (   id, full_name, zman_peraon, pitzuim, mavid, shalva, nosaf_shonot_mavid, manak_ptira, seif_45, seif_47, nosaf_shonot_oved, sach_chelek_oved, sach_chelek_mavid, sach)";

		tr.Insertintodb1(a);

	}

	public void create_table_neto(String name_schema, String name_tabel) {

		String a = "CREATE TABLE  if not exists Upload_file.`" + name_tabel + "` (\n"
				+ "  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n"
				+ "  `name_tz` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `a` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `aa` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `id` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `aaa` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `aaaa` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `aaaaa` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `neto` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `dyear` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `num_oved` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `full_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" + "  PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";

	}

	public void load_data_neto(String name_schema, String name_tabel) throws SQLException {
		// name_tabel = name_tabel;
		for (int l = 2009; l < 2015; l++) {
			// /home/shoshana/Downloads/itali_gold/sacar/2008/neto

			for (int i = 1; i < 13; i++) {

				String a = "LOAD DATA  LOCAL INFILE" + " '/home/shoshana/Downloads/onot/" + l + "/neto/" + i + ".csv'"
						+ " INTO TABLE  `" + name_schema + "`." + name_tabel
						+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 14 LINES "
						+ "  (  name_tz, a, aa, id, aaa, aaaa, aaaaa, neto, m, dyear)" + "set dyear=" + l + ",m=" + i
						+ ";";

				tr.Insertintodb1(a);
			}
		}
		String z = "update  Upload_file." + name_tabel + "\n" + "set neto=replace(neto,',','');";
		tr.Insertintodb1(z);

		String x = "update Upload_file. " + name_tabel
				+ "  as t2 set num_oved=substring(t2.name_tz,1,position(' ' in t2.name_tz)-1)";
		tr.Insertintodb1(x);
		String h = "update Upload_file. " + name_tabel
				+ "  as t2 set full_name=substring(t2.name_tz,position(' ' in t2.name_tz)) ;";
		tr.Insertintodb1(h);
		String xx = "delete from Upload_file.tbl_neto_onot where neto=''";
		tr.Insertintodb1(xx);
	}

	public void convert_to_101_neto(String name_schema, String name, String name_table, int cid) throws SQLException {
		for (int i = 1; i < 13; i++) {
			String load = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT   full_name," + cid + ",`dyear`,`num_oved`, 3000,'נטו',neto\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where m=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load);
		}

	}

	public void convert_to_101_kupot_gemel(String name_schema, String name, String name_table, int cid)
			throws SQLException {
		for (int i = 1; i < 13; i++) {
			String load = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT   shem_oved," + cid
					+ ",`dyear`,`mis_oved`, 3020, concat( shem_kupa,'  ','גמל עובד'),ifnull(tigmulim_oved,0)\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load);

			String load1 = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT   shem_oved," + cid
					+ ",`dyear`,`mis_oved`, 3021,concat( shem_kupa,'  ', 'גמל מעביד'),ifnull(tigmulim_maavid,0)\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load1);
		}

	}

	public void create_table_emon(String name_schema, String name_table) throws SQLException {
		String a = "CREATE TABLE   if not exists `" + name_schema + "`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n" +
				// " `f_name` VARCHAR(45) NULL,\n" +
				// " `l_name` VARCHAR(45) NULL,\n" +
				"  `tat_mifal` VARCHAR(60) NULL,\n" + "  `agaf` VARCHAR(45) NULL,\n"
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
				+ "  PRIMARY KEY (`in_id`));";

		tr.Insertintodb1(a);

	}

	public void load_data_emon(String name_schema, String name_tabel, String year) throws SQLException {

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
		// + " '/home/shoshana/Downloads/mishcan_htcelet/"+year+".csv'"+
		// "INTO TABLE `Upload_file`."+name_tabel+" CHARSET hebrew FIELDS
		// TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES\n" +
		// " (mis_oved,f_name,l_name, tat_mifal, agaf, machlaka, name_machlaka,
		// derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok,
		// tear_isok, symbol, symbol_name,\n" +
		// " m1, m2, m3, m4, m5, m6, m7, m8, m9, \n" +
		// " m10, m11, m12, total, avg, avg_p, sub,dyear) set dyear="+year;
		// tr.Insertintodb1(b);

		// String b="LOAD DATA LOCAL INFILE "
		// + " '/home/shoshana/Downloads/amal/"+year+".csv'"+
		// "INTO TABLE `Upload_file`."+name_tabel+" FIELDS TERMINATED BY ','
		// LINES TERMINATED BY '\n' IGNORE 2 LINES\n" +
		// " (mis_oved,f_name,l_name, tat_mifal, agaf, machlaka, name_machlaka,
		// derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok,
		// tear_isok, symbol, symbol_name,\n" +
		// " m1, m2, m3, m4, m5, m6, m7, m8, m9, \n" +
		// " m10, m11, m12, total, avg, avg_p, sub,dyear) set dyear="+year;

		String b = "LOAD DATA LOCAL INFILE " + " '/home/shoshana/Downloads/" + year + ".csv'"
				+ "INTO TABLE `Upload_file`." + name_tabel
				+ "   FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n' IGNORE 2 LINES\n"
				+ " (mis_oved,  tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,\n"
				+ " m1, m2, m3, m4, m5, m6, m7, m8, m9, \n"
				+ " m10, m11, m12, total, avg, avg_p, sub,dyear) set dyear=2016";
		tr.Insertintodb1(b);

	}

	void update_symbol(String name_schema, String name_tabel) throws SQLException {
		String a = "  UPDATE " + name_schema + "." + name_tabel + "\n"
				+ "SET SymbolName = substring(SymbolName,1,position(':' in SymbolName)-1 )\n"
				+ "where  type not like '%כמויות%' ;";

		tr.Insertintodb1(a);

	}

	void create_table_neto1(String name_schema, String name_tabel) throws SQLException {
		String a = "CREATE TABLE " + name_schema + "." + name_tabel + " (\n"
				+ "  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n"
				+ "  `name_tz` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `a` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `aa` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `id` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `aaa` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `aaaa` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `aaaaa` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `neto` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `m` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `dyear` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `num_oved` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `full_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" + "  PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";

		tr.Insertintodb1(a);
	}

	void read_file() throws IOException {

		// Fixed102Btjava result = new Fixed102Btjava();
		// List<Fixed102Btjava> listRes = new ArrayList<Fixed102Btjava>();
		EntityManagerFactory emf = Persistence.createEntityManagerFactory("JavaApplication1PU");

		TblTiconJpaController jpa = new TblTiconJpaController(emf);

		TblTicon g = new TblTicon();

		for (int i = 2012; i < 2016; i++) {

			for (int j = 1; j < 13; j++) {
				String a = "/home/shoshana/Downloads/ticon/" + i + "/" + j + ".csv";

				BufferedReader br = csvFileHelper.readFile(a);
				// List<koupatGuimel> list;
				String[] part;
				br.readLine();
				br.readLine();
				br.readLine();
				// int i =0;
				for (String line = br.readLine(); line != null; line = br.readLine()) {

					part = line.split("\\|", 10);
					String mifal = part[0];
					String tkufa = part[1];
					String hanhalach = part[2];
					String Symbol = part[3];
					String tear = part[4];
					String machlaka = part[5];
					String id = part[6];
					String l_name = part[7];
					String f_name = part[8];
					String value = part[9];

					g.setMifal(mifal);
					g.setTkufa(tkufa);
					g.setHanalach(hanhalach);
					g.setSemel(Symbol);
					g.setTear(tear);
					g.setMachlaka(machlaka);
					g.setId(id);
					g.setLName(l_name);
					g.setFName(f_name);
					g.setScumNicoy(value);
					g.setDyear(i);
					g.setMonth(j);
					jpa.create(g);
				}
			}

		}
	}

	void convert_to_101_dmei(String name_schema, String name_table_101, String name_table, int cid)
			throws SQLException {
		for (int i = 1; i < 13; i++) {

			String c = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "(cid, dyear, id, FullName, Symbol, SymbolName, m" + i + ")\n" + "SELECT " + cid
					+ " as cid,dyear,id,concat(l_name, f_name) ,semel as symbol,tear,scum_nicoy \n" + "FROM "
					+ name_schema + "." + name_table + " where   month=" + i + "\n" + "on duplicate key update m" + i
					+ "=values(m" + i + ")";
			t.Insertintodb(c);
		}
	}

	void create_table_bitoc_leimi(String name_schema, String name_table) throws SQLException {

		String a = "CREATE TABLE  if not exists  `Upload_file`.`" + name_table + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n" + "  `mis_oved` VARCHAR(45) NULL,\n"
				+ "  `l_name` VARCHAR(45) NULL,\n" + "  `min` VARCHAR(45) NULL,\n" + "  `f_name` VARCHAR(45) NULL,\n"
				+ "  `full_name` VARCHAR(45) NULL,\n" + "  `id` VARCHAR(45) NULL,\n"
				+ "  `tarich_leda` VARCHAR(45) NULL,\n" + "  `m1` VARCHAR(45) NULL,\n" + "  `m2` VARCHAR(45) NULL,\n"
				+ "  `m3` VARCHAR(45) NULL,\n" + "  `m4` VARCHAR(45) NULL,\n" + "  `m5` VARCHAR(45) NULL,\n"
				+ "  `m6` VARCHAR(45) NULL,\n" + "  `m7` VARCHAR(45) NULL,\n" + "  `m8` VARCHAR(45) NULL,\n"
				+ "  `m9` VARCHAR(45) NULL,\n" + "  `m10` VARCHAR(45) NULL,\n" + "  `m11` VARCHAR(45) NULL,\n"
				+ "  `m12` VARCHAR(45) NULL,\n" + "  `dyear` VARCHAR(45) NULL,\n" + "  PRIMARY KEY (`in_id`));";
		tr.Insertintodb1(a);

	}

	public void convert_to_101_bituchim(String name_schema, String name, String name_table, int cid)
			throws SQLException {
		for (int i = 1; i < 13; i++) {
			String load = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT   shem_amit, cid,dyear,mispar_mezhe,'11111', concat('עובד',' ',sug,' ', keren) ,oved \n"
					+ "            FROM  " + name_schema + "." + name_table + "  where month=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load);

			String load1 = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT   shem_amit, cid,dyear,mispar_mezhe,'22222', concat('מעביד',' ',sug,' ', keren) ,masik \n"
					+ "            FROM  " + name_schema + "." + name_table + "  where month=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load1);

			String load2 = "insert ignore into " + name_schema + "." + name + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ") \n"
					+ "            SELECT   shem_amit, cid,dyear,mispar_mezhe,'33333', concat('פיצוים',' ',sug,' ', keren) ,pitzuim \n"
					+ "            FROM  " + name_schema + "." + name_table + "  where month=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			tr.Insertintodb1(load2);
		}

	}

}//

//
//
//
//
//