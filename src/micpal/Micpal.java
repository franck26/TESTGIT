package micpal;

import mySQL.Trysql;

import java.io.File;
import java.sql.*;
import java.sql.SQLException;
import com.sun.xml.internal.bind.v2.runtime.Name;

public class Micpal{

	private static Trysql t;

	private static Micpal instance = null;


	private  Micpal() throws SQLException, ClassNotFoundException{
		this.t = Trysql.getInstance();

	}


	static Micpal getInstance() throws SQLException, ClassNotFoundException{
		if (instance == null){
			instance = new Micpal();
			return instance;
		} else {
			return instance;
		}
	}

	public void create_tabel_micpal_tashlumim(String name_schema,String name_table) throws SQLException, ClassNotFoundException {
		System.out.println("create tbl_tashlumim.");
		String create = "CREATE TABLE  if not exists `"+name_schema+"`.`" + name_table +"` (\n"
				+ "                `in_id` INT NOT NULL AUTO_INCREMENT,\n"

                + "                `name_company` VARCHAR(200) NULL,\n"
                + "                `tax_year` VARCHAR(200) NULL,\n"
                + "                `id_Worker` VARCHAR(200) NULL,\n"
                + "                `full_name` VARCHAR(200) NULL,\n"
                + "               `m` VARCHAR(200) NULL,\n"
                + "               `symbol` VARCHAR(200) NULL,\n"
                + "                `symbolName` VARCHAR(200) NULL,\n"
                + "               `value` VARCHAR(200) NULL,\n"
                + "                 `sicom_camot` VARCHAR(200) NULL,\n"
                + "                `dyear` VARCHAR(200) NULL,\n"
                + "                 `name_machlaka` VARCHAR(200) NULL,\n"
                + "               PRIMARY KEY (`in_id`));";

		t.Insertintodb1(create);

		truncate(name_schema, name_table);

	}

	public void create_tabel_micpal_koupot_gemel(String name_schema,String name_table) throws SQLException {

		System.out.println("create tbl_guemel.");
		String kupot_gemel = "CREATE TABLE if not exists "+ name_schema+".`" + name_table + "` (\n"
				+ "  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n"
				+ "  `shem_hevra` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `dyear` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  `id` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
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
				+ "  `cid` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"

				+ "  `ksk` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"

				+ "  `soug` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		t.Insertintodb1(kupot_gemel);

		truncate(name_schema, name_table);

	}

	public void create_tabel_micpal_nikuy_hova(String name_schema,String name_table) throws SQLException {

		System.out.println("create tbl_hova.");
		String nikuy = "CREATE TABLE if not exists "+name_schema+".`" + name_table + "` (\n"
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
				+ "  `mas_hahnasa` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		t.Insertintodb1(nikuy);

		truncate(name_schema, name_table);


	}

	public void create_tabel_micpal_nikuy_reshut(String name_schema,String name_table) throws SQLException {

		System.out.println("create tbl_reshut.");
		String reshut = "CREATE TABLE if not exists "+name_schema+".`" + name_table + "` (\n"
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
				+ "  `symbol` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		t.Insertintodb1(reshut);

		truncate(name_schema, name_table);



	}

	public void create_tabel_micpal_tamhir_ovdim(String name_schema,String name_table) throws SQLException {
		System.out.println("create tbl_ovdim.");

		String tamchir = "CREATE TABLE if not exists "+name_schema+".`" + name_table + "` (\n"
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
				+ "  `mis_hodesh` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n"
				+ "  PRIMARY KEY (`in_id`)\n"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		t.Insertintodb1(tamchir);

		truncate(name_schema, name_table);

	}



	public void truncate(String name_schema, String name_table) throws SQLException {
		System.out.println("truncate " + name_table);
		String a="    TRUNCATE `"+name_schema+"`.`"+name_table+"`";
		t.Insertintodb1(a);
	}



	public void load_data_micpal_tashlumim(String path_file, String name_schema, String name_table_tashlumim, int year) throws SQLException {
	
			String path = path_file+ "/" + year+"/tashlumim.csv";
			System.out.println(path);

			String a = "LOAD DATA  LOCAL INFILE "
					+ "'" + path + "'"
					+ " INTO TABLE `"+name_schema+"`." + name_table_tashlumim
					+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  "
					+ " (name_company ,  dyear , id_Worker , full_name, m ,symbol, symbolName ,value, sicom_camot,   name_machlaka)";
			t.Insertintodb1(a);
	}

	public void load_data_micpal_kupot_gemel(String path_file, String name_schema, String name_table_koupot_gemel, int year, int cid) throws SQLException {
		System.out.println(path_file+ "/" + year+"/koupot.csv'");

		String a = "LOAD DATA  LOCAL INFILE '"
				+ path_file+ "/" + year+"/koupot.csv'"
				+ " INTO TABLE `"+name_schema+"`." + name_table_koupot_gemel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
				+ "  (shem_hevra, dyear, id, mis_oved, shem_oved, shem_mahlaka, mis_hodesh, "
				+ "zihoy_1, zihoy_2, "
				+ "mis_kupa, "
				+ "ksk, "
				+ "shem_kupa, "
				+ "soug, "
				+ "mis_haver, tigmulim_oved, "
				+ "tigmulim_maavid, pitzuim, shonot, zkifa, sachar_mafkidim_lakupa, min, taarih_leida, matzav_mishpaha, yeshuv, ktovet, mis_bait, mikud, telephon, symbol) "
				+ "set cid = " + cid + " ;"
				;
		t.Insertintodb1(a);

	}

	public void load_data_micpal_nikuy_hova(String path_file, String name_schema, String name_table_nikuy_hova, int year) throws SQLException{
		System.out.println("'" +path_file+  "/"+year+"/chova.csv'");

		String a = "LOAD DATA  LOCAL INFILE "
				+ "'" +path_file+"/"+year+"/chova.csv'"
				+ " INTO TABLE `"+name_schema+"`." + name_table_nikuy_hova
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  "
				+ " (shem_hevra, shnat_mas, mis_oved, shem_oved, shem_mahlaka, mis_hodesh, bituah_leumi, mas_irgun, sah_bituah_leumi, mas_briut, mas_hahnasa)";

		t.Insertintodb1(a);
	}

	public void load_data_micpal_nikuy_reshut(String path_file, String name_schema, String name_table_nikuy_reshut, int year) throws SQLException{
		System.out.println(path_file+year+"/reshut.csv'");

		String a = "LOAD DATA  LOCAL INFILE '"
				+ path_file+ "/" + year+"/reshut.csv'"
				+ " INTO TABLE `"+name_schema+"`." + name_table_nikuy_reshut
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
				+ "  ( shem_hevra, shnat_mas, mis_oved, shem_oved, shem_mahlaka, mis_hodesh, mis_shura, mis_nikuy, kod_sug_nikuy, shem_nikuy, schum_nikuy, symbol)"
				;

		t.Insertintodb1(a);
	}

	public void load_data_micpal_tamhir_ovdim(String path_file, String name_schema, String name_table_tamhir_ovdim, int year) throws SQLException{
		
		File f = new File(path_file+ "/" +year+"/tamhir");

		File[] listefichier = f.listFiles();

		int i = 0;

		for (int a = 0; a < listefichier.length; a++){
			if (listefichier[a].getAbsolutePath().endsWith(".csv")) {
				i++;
			}
		}
		
		for(int k = 1; k <= i ; k++){

			System.out.println(path_file+ "/" +year+"/tamhir/" +k + ".csv'");
			String a = "LOAD DATA  LOCAL INFILE '"
					+ path_file+ "/" +year+"/tamhir/" + k + ".csv'"
					+ " INTO TABLE `"+name_schema+"`." + name_table_tamhir_ovdim
					+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
					+ "  (shem_hevra, shnat_mas, mis_oved, shem_oved, alut, sikum_bruto, sikum_tigmulim, sikum_pitzuim, "
					+ "sikum_shonot, sikum_bituah_leumi_maavid, sikum_mas_maasikim, sikum_mas_sachar, sikum_hetel_oved_zar, symbol) set mis_hodesh = " + k;
					;
			t.Insertintodb1(a);

		}

	}



	public void create_101_micpal_koupot_gemel(String name_schema, String kupot_gemel_micpal) throws SQLException {

		System.out.println("create " + kupot_gemel_micpal);

		String a = "CREATE TABLE if not exists  "+name_schema+".`" + kupot_gemel_micpal + "` (\n"
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

		t.Insertintodb1(a);

		truncate(name_schema, kupot_gemel_micpal);


	}

	public void create_101_micpal_nikuy_hova(String name_schema, String nikuy_hova_micpal) throws SQLException {

		System.out.println("create " + nikuy_hova_micpal);

		String a = "CREATE TABLE if not exists "+name_schema+".`" + nikuy_hova_micpal + "` (\n"
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

		t.Insertintodb1(a);

		truncate(name_schema, nikuy_hova_micpal);

	}

	public void create_101_micpal_nikuy_reshut(String name_schema, String nikuy_reshut_micpal) throws SQLException {

		System.out.println("create " + nikuy_reshut_micpal);

		String a = "CREATE TABLE if not exists "+name_schema+".`" + nikuy_reshut_micpal + "` (\n"
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

		t.Insertintodb1(a);

		truncate(name_schema, nikuy_reshut_micpal);
	}

	public void create_101_micpal_tamhir_ovdim(String name_schema, String tamhir_ovdim_micpal) throws SQLException {

		System.out.println("create " + tamhir_ovdim_micpal);

		String a = "CREATE TABLE  if not exists "+name_schema+".`" + tamhir_ovdim_micpal + "` (\n"
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

		t.Insertintodb1(a);

		truncate(name_schema, tamhir_ovdim_micpal);
	}

	public void create_101_micpal_tashlumim(String name_schema, String tashlumim_micpal) throws SQLException {

		System.out.println("create " + tashlumim_micpal);
		String a = "CREATE TABLE if not exists "+name_schema+".`" + tashlumim_micpal + "` (\n"
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

		t.Insertintodb1(a);

		truncate(name_schema, tashlumim_micpal);
	}




	public void convert_to_101_kupot_gemel(String name_schema, String name_table_101,String name_table, int cid) throws SQLException {

		System.out.println("convert koupot_guemel to 101");
		for (int i = 1; i < 13; i++) {
			//			String load = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ", `type`) \n"
			//					+ "            SELECT   shem_oved," + cid + ",`shnat_mas`,`mis_oved`, 3020, concat( shem_kupa,'  ','גמל עובד'),ifnull(tigmulim_oved,0), 'גמל'\n"
			//					+ "            FROM  "+name_schema+"."+name_table +"  where mis_hodesh=" + i + " \n"
			//					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			//			t.Insertintodb1(load);
			//
			//			String load1 = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
			//					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ", `type`) \n"
			//					+ "            SELECT   shem_oved," + cid + ",`shnat_mas`,`mis_oved`, 3021,concat( shem_kupa,'  ', 'גמל מעביד'),ifnull(tigmulim_maavid,0), 'גמל'\n"
			//					+ "            FROM  "+name_schema+"."+name_table +"  where mis_hodesh=" + i + " \n"
			//					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			//			t.Insertintodb1(load1);

			String load = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`, num_worker, `Symbol`,`SymbolName`,m" + i + ", type_for_gemel, source) \n"
					+ "            SELECT   shem_oved," + cid + ",`dyear`,`id`, `mis_oved`, 3020, concat( shem_kupa,'  ','גמל עובד'),ifnull(sum(tigmulim_oved),0), ';shulam_oved;pensya;sum_shulam;', 'original_micpal'\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " group by mis_oved,dyear,shem_kupa\n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(load);

			String load1 = "insert ignore into " + name_schema + "." + name_table_101 + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`, num_worker, `Symbol`,`SymbolName`,m" + i + ", type_for_gemel, source) \n"
					+ "            SELECT   shem_oved," + cid + ",`dyear`,`id`, mis_oved, 3021,concat( shem_kupa,'  ', 'גמל מעביד'),ifnull(sum(tigmulim_maavid),0), ';shulam_maavid;pensya;sum_shulam;' , 'original_micpal'\n"
					+ "            FROM  " + name_schema + "." + name_table + "  where mis_hodesh=" + i + " group by mis_oved,dyear,shem_kupa\n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(load1);
		}
	}

	public void Micpal_to_101_tashlumim(String name_schema,int cid, String name_table, String name_table1) throws SQLException {


		System.out.println("convert tashlumim to 101");
		for (int i = 1; i < 13; i++) {
			String load = "insert ignore into "+name_schema+"." + name_table + "\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`, num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "            SELECT   full_name," + cid + ",`dyear`,`id_Worker`, id_Worker, `symbol`,`symbolName`,`value`,';tlush_tashlum;', 'original_micpal'\n"
					+ "            FROM "+name_schema+"." + name_table1 + " where m=" + i + " \n"
					+ "            on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(load);

		}

	}

	public void Micpal_to_101_nikuy_hova(String name_schema, String name_table_101, String name_table, int year1, int year2, int cid) throws SQLException {


		for(int year = year1; year <= year2; year++){

			System.out.println("nicoye hova to 101 for year : " + year);
			for (int i = 1; i <= 12; i++) {

				String a = " insert ignore into "+name_schema+"." + name_table_101 + "\n"
						+ "            (`FullName`,`cid`,`dyear`,`id`, num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n  "
						+ " SELECT   shem_oved," + cid + " as cid," + year + ",`mis_oved`,`mis_oved`, '2500' as Symbol,'ביטוח לאומי'  as SymbolName ,bituah_leumi,'ניכוי חובה','original_micpal'"
						+ " FROM "+name_schema+"."+name_table +" where mis_hodesh=" + i + " and shnat_mas = " + year + " \n group by mis_oved,SymbolName,Symbol   on duplicate key update m" + i + "=values(m" + i + ")";
				t.Insertintodb1(a);

				String b = " insert ignore into "+name_schema+"." + name_table_101 + "\n"
						+ "            (`FullName`,`cid`,`dyear`,`id`,num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n  "
						+ " SELECT   shem_oved," + cid + " as cid," + year + ",`mis_oved`,`mis_oved`, '3000' as Symbol,'מס אירגון '  as SymbolName ,mas_irgun,'ניכוי חובה','original_micpal'"
						+ " FROM "+name_schema+"."+name_table +" where mis_hodesh=" + i + " and shnat_mas = " + year + " \n  group by mis_oved,SymbolName,Symbol  on duplicate key update m" + i + "=values(m" + i + ")";
				t.Insertintodb1(b);

				String c = "  insert ignore into "+name_schema+"." + name_table_101 + "\n"
						+ "            (`FullName`,`cid`,`dyear`,`id`,num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n  "
						+ " SELECT   shem_oved," + cid + " as cid," + year + ",`mis_oved`, `mis_oved`,'4000' as Symbol,'סך ביטוח לאומי '  as SymbolName ,sah_bituah_leumi,'ניכוי חובה','original_micpal'"
						+ " FROM "+name_schema+"."+name_table + " where mis_hodesh=" + i + " and shnat_mas = " + year + "\n  group by mis_oved,SymbolName,Symbol  on duplicate key update m" + i + "=values(m" + i + ")";
				t.Insertintodb1(c);

				String f = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
						+ "            (`FullName`,`cid`,`dyear`,`id`,num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n  "
						+ "   SELECT   shem_oved," + cid + " as cid," + year + ",`mis_oved`,`mis_oved`, '5000' as Symbol,'מס בריאות '  as SymbolName ,mas_briut,'ניכוי חובה','original_micpal'"
						+ " FROM "+name_schema+"."+name_table+" where mis_hodesh=" + i + " and shnat_mas = " + year + " \n group by mis_oved,SymbolName,Symbol   on duplicate key update m" + i + "=values(m" + i + ")";
				t.Insertintodb1(f);

				String h = "insert ignore into "+name_schema+"." + name_table_101 + "\n"
						+ "            (`FullName`,`cid`,`dyear`,`id`,num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n  "
						+ " SELECT   shem_oved," + cid + " as cid," + year + ",`mis_oved`, `mis_oved`,'6000' as Symbol,'מס הכנסה '  as SymbolName ,mas_hahnasa,'ניכוי חובה','original_micpal'"
						+ " FROM "+name_schema+"."+name_table +" where mis_hodesh=" + i + " and shnat_mas = " + year + " \n group by mis_oved,SymbolName,Symbol   on duplicate key update m" + i + "=values(m" + i + ")";
				t.Insertintodb1(h);


			}
		}

	}


	public void Micpal_to_101_tamchir_ovdim_per_month(String name_schema,int cid,String name_table_101,String name_table) throws SQLException {

		System.out.println("tamhir ovdim to 101");

		for (int i = 1; i < 13; i++) {
			String a = "insert ignore into "+name_schema+"."+name_table_101+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`, num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + "  as cid,`shnat_mas`,`mis_oved`, `mis_oved`,'1500' as Symbol,'עלות'  as SymbolName \n"
					+ "   ,alut,'תמחיר עובדים ','original_micpal'  FROM "+name_schema+"."+name_table +" where mis_hodesh=" + i + " \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(a);
			String b = "insert ignore into "+name_schema+"."+name_table_101+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, `mis_oved`,'1600' as Symbol,'סיכום ברוטו'  as SymbolName \n"
					+ "   ,sikum_bruto,'תמחיר עובדים ','original_micpal'  FROM "+name_schema+"."+name_table +" where mis_hodesh=" + i + " \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(b);
			String c = "insert ignore into "+name_schema+"."+name_table_101+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, `mis_oved`,'1700' as Symbol,'סיכום תגמולים'  as SymbolName \n"
					+ "   ,sikum_tigmulim,'תמחיר עובדים ','original_micpal'  FROM "+name_schema+"."+name_table +" where mis_hodesh=" + i + " \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(c);
			String d = "insert ignore into "+name_schema+"."+name_table_101+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, `mis_oved`,'1800' as Symbol,'סיכום פיצוים'  as SymbolName \n"
					+ "   ,sikum_pitzuim,'תמחיר עובדים ','original_micpal'  FROM "+name_schema+"."+name_table +" where mis_hodesh=" + i + " \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(d);
			String e = "insert ignore into "+name_schema+"."+name_table_101+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, `mis_oved`,'1900' as Symbol,'סיכום שונות'  as SymbolName \n"
					+ "   ,sikum_shonot,'תמחיר עובדים ','original_micpal'  FROM "+name_schema+"."+name_table +" where mis_hodesh=" + i + " \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(e);
			String f = "insert ignore into "+name_schema+"."+name_table_101+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`,`mis_oved`, '2000' as Symbol,'סיכום ביטוח לאומי מעביד'  as SymbolName \n"
					+ "   ,sikum_bituah_leumi_maavid,'תמחיר עובדים ','original_micpal'  FROM "+name_schema+"."+name_table +" where mis_hodesh=" + i + " \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(f);

//			String l = "insert ignore into "+name_schema+"."+name_table_101+"\n"
//					+ "            (`FullName`,`cid`,`dyear`,`id`,num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n"
//					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, `mis_oved`,'2100' as Symbol,'קרן השתלמות'  as SymbolName \n"
//					+ "   ,keren,'תמחיר עובדים ','original_micpal'  FROM "+name_schema+"."+name_table +" where mis_hodesh=" + i + " \n"
//					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
//			t.Insertintodb1(l);


			String g = "insert ignore into "+name_schema+"."+name_table_101+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, `mis_oved`, '2200' as Symbol,'סיכום מס שכר '  as SymbolName \n"
					+ "   ,sikum_mas_sachar,'תמחיר עובדים ','original_micpal'  FROM "+name_schema+"."+name_table +" where mis_hodesh=" + i + " \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(g);



		}
	}

	public void Micpal_to_101_tamchir_ovdim_per_year(String name_schema,int cid,String name_table,String name_table1) throws SQLException {
		for (int i = 1; i < 13; i++) {

			String a = "insert ignore into "+name_schema+"."+name_table+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + "  as cid,`shnat_mas`,`mis_oved`, '1500' as Symbol,'עלות'  as SymbolName \n"
					+ "   ,alut/12,'תמחיר עובדים ' ,'נתוני שכר'  FROM "+name_schema+". "+ name_table1 +"  \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(a);
			String b = "insert ignore into "+name_schema+"."+name_table+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, '1600' as Symbol,'סיכום ברוטו'  as SymbolName \n"
					+ "   ,sikum_bruto/12,'תמחיר עובדים ','נתוני שכר'  FROM "+name_schema+". "+ name_table1 +"  \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(b);
			String c = "insert ignore into "+name_schema+"."+name_table+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, '1700' as Symbol,'סיכום תגמולים'  as SymbolName \n"
					+ "   ,sikum_tigmulim/12,'תמחיר עובדים ','נתוני שכר'  FROM "+name_schema+"."+ name_table1 +"  \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(c);
			String d = "insert ignore into "+name_schema+"."+name_table+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, '1800' as Symbol,'סיכום פיצוים'  as SymbolName \n"
					+ "   ,sikum_pitzuim/12,'תמחיר עובדים ','נתוני שכר'  FROM "+name_schema+". "+ name_table1 +"  \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(d);
			String e = "insert ignore into "+name_schema+"."+name_table+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, '1900' as Symbol,'סיכום שונות'  as SymbolName \n"
					+ "   ,sikum_shonot/12,'תמחיר עובדים ','נתוני שכר'  FROM "+name_schema+". "+ name_table1 +"  \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(e);
			String f = "insert ignore into "+name_schema+"."+name_table+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`,`Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, '2000' as Symbol,'סיכום ביטוח לאומי מעביד'  as SymbolName \n"
					+ "   ,sikum_bituah_leumi_maavid/12,'תמחיר עובדים ','נתוני שכר'  FROM "+name_schema+". "+ name_table1 +"  \n"
					+ "    on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(f);
		}

	}

	public void create_symbels_numbers_and_insert_into_main_table_pa(String name_schema, String name_tabletemp, String name_table) throws SQLException {
		
		System.out.println("nikuye reshut to 101, create_symbels_numbers_and_insert_into_main_table_pa");
		
		String a = "CREATE TABLE  if not exists `" + name_schema + "`.`" + name_tabletemp + "` (\n"
				+ "  `in_id` INT NOT NULL AUTO_INCREMENT,\n"
				+ "  `symble` VARCHAR(200) NULL,\n"

                + "  PRIMARY KEY (`in_id`)) "
                + "ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";

		t.Insertintodb1(a);
		
		String x = "TRUNCATE `" + name_schema + "`.`" + name_tabletemp + "`;";
		t.Insertintodb1(x);
		String s = "ALTER TABLE `" + name_schema + "`.`" + name_tabletemp + "`  AUTO_INCREMENT = 2001";
		t.Insertintodb1(s);

		String j = "insert into " + name_schema + "." + name_tabletemp + "(symble)  SELECT shem_nikuy FROM " + name_schema + "." + name_table + " group by  shem_nikuy;";
		t.Insertintodb1(j);

		String insert = "UPDATE  " + name_schema + "." + name_table + "  as a inner join  " + name_schema + "." + name_tabletemp + " as b "
				+ " on a.shem_nikuy =  b.symble   set a.symbol = b.in_id  ;";
		t.Insertintodb1(insert);

	}

	public void Micpal_to_101_nicoy_reshut(String name_schema,int cid,String name_table_101,String name_table) throws SQLException {

		System.out.println("nicoye_reshut to 101");

		for (int i = 1; i <= 12; i++) {
			String a = " insert  into "+name_schema+"."+name_table_101+"\n"
					+ "            (`FullName`,`cid`,`dyear`,`id`, num_worker, `Symbol`,`SymbolName`,m" + i + ",type,source) \n"
					+ "   SELECT   shem_oved," + cid + " as cid,`shnat_mas`,`mis_oved`, `mis_oved`, symbol, shem_nikuy as SymbolName \n"
					+ "   ,sum(schum_nikuy),'ניכוי רשות','original_micpal' "
					+ "FROM "+name_schema+"."+name_table+" where mis_hodesh=" + i + "\n"
					+ " group by shnat_mas, mis_oved,SymbolName,symbol   "
					+ "on duplicate key update m" + i + "=values(m" + i + ");";
			t.Insertintodb1(a);
		}

	}





	public void create_101_micpal_sofi(String name_schema,String name_table) throws SQLException {

		System.out.println("create " + name_table);

		String a = "CREATE TABLE   if not exists "+name_schema+".`" + name_table + "` (\n"
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
				+ "  KEY `indi3` (`cid`,`dyear`,`Symbol`,`SymbolName`)\n,"
				 + " KEY `ind` (`id`,`dyear`)"
				+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";
		t.Insertintodb1(a);

		truncate(name_schema, name_table);

	}

	public void insert_total_101_micpal(String name_schema,String name_table_101_sofi ,String name_table_101_tashlumim,String name_table_101_nikuy_hova, String name_table_101_nikuy_reshut,String name_table_101_tamhir_ovdim, String name_table_101_guemel) throws SQLException {
		System.out.println("insert all in " + name_table_101_sofi);

		String a = "insert ignore into " + name_schema + "."+name_table_101_sofi+" \n"
				+ "            (cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) \n"
				+ "   SELECT   \n"
				+ "   cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel\n"
				+ "   \n"
				+ "   \n"
				+ "   FROM "+name_schema+"."+name_table_101_nikuy_hova ;
		t.Insertintodb1(a);

		String b = "insert ignore into " + name_schema + "."+name_table_101_sofi+" \n"
				+ "            (cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) \n"
				+ "   SELECT   \n"
				+ "   cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel\n"
				+ "   \n"
				+ "   \n"
				+ "   FROM "+name_schema+"."+name_table_101_nikuy_reshut ;
		t.Insertintodb1(b);

		String c = "insert ignore into " + name_schema + "."+name_table_101_sofi+" \n"
				+ "            (cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) \n"
				+ "   SELECT   \n"
				+ "   cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel\n"
				+ "   \n"
				+ "   \n"
				+ "   FROM "+name_schema+"."+name_table_101_tamhir_ovdim;
		t.Insertintodb1(c);

		String d = "insert ignore into " + name_schema + "."+name_table_101_sofi+" \n"
				+ "            (cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) \n"
				+ "   SELECT   \n"
				+ "   cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel\n"
				+ "   \n"
				+ "   \n"
				+ "   FROM "+name_schema+"."+name_table_101_tashlumim;
		t.Insertintodb1(d);

		String e = "insert ignore into " + name_schema + "."+name_table_101_sofi+" \n"
				+ "            (cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel) \n"
				+ "   SELECT   \n"
				+ "   cid, dyear, id, original_id, FullName, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, total, division, run_version, date_value, source, type, num_worker, permission, type_for_gemel\n"
				+ "   \n"
				+ "   \n"
				+ "   FROM "+name_schema+"."+name_table_101_guemel;
		t.Insertintodb1(e);

	}

	public void update_total(String name_schema, String name_table) throws SQLException {



		String a = "update " + name_schema + "." + name_table + " set total=m1+m2+m3+m4+m5+m6+m7+m8+m9+m10+m11+m12";

		System.out.println(a);

		t.Insertintodb1(a);

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
		t.Insertintodb1(a);
	}




	// quand il n'y a pas de fichier sifferents pour chaque annee
	public void load_data_micpal_tashlumim1(String path_file, String name_schema, String name_table_tashlumim) throws SQLException {
		System.out.println(path_file + "/tashlumim.csv'");

		String a = "LOAD DATA  LOCAL INFILE '"
				+ path_file + "/tashlumim.csv'"
				+ " INTO TABLE `"+name_schema+"`." + name_table_tashlumim
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  "
				+ " (name_company ,  dyear , id_Worker , full_name, m ,symbol, symbolName ,value, sicom_camot,   name_machlaka)"
				;
		t.Insertintodb1(a);
		
		ResultSet result = t.gettabledb("select * from `"+name_schema+"`." + name_table_tashlumim );
		
	      ResultSetMetaData resultMeta = result.getMetaData();
	      
	      for(int i = 1; i <= resultMeta.getColumnCount(); i++){
	    	  t.Insertintodb("update `"+name_schema+"`." + name_table_tashlumim + " set " + resultMeta.getColumnName(i) + " = replace(" + resultMeta.getColumnName(i) + ", ',', '')");
	    	  System.out.println(resultMeta.getColumnName(i));
	      }
	}

	public void load_data_micpal_kupot_gemel(String path_file, String name_schema, String name_table_koupot_gemel, int cid) throws SQLException {
		System.out.println(path_file+"/koupot.csv'");

		String a = "LOAD DATA  LOCAL INFILE '"
				+ path_file+"/koupot.csv'"
				+ " INTO TABLE `"+name_schema+"`." + name_table_koupot_gemel
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
				+ "  (shem_hevra, shnat_mas, TZ, mis_oved, shem_oved, shem_mahlaka, mis_hodesh,  "
//				+ "zihoy_1, zihoy_2, "
				+ "mis_kupa, shem_kupa, mis_haver, tigmulim_oved, tigmulim_maavid, pitzuim, shonot, "
				+ "zkifa, sachar_mafkidim_lakupa, min, taarih_leida, matzav_mishpaha, yeshuv, ktovet, mis_bait, mikud, telephon, symbol) "
				+ "set cid = " + cid + " ;";
		;

		t.Insertintodb1(a);
		
		ResultSet result = t.gettabledb("select * from `"+name_schema+"`." + name_table_koupot_gemel);
		
	      ResultSetMetaData resultMeta = result.getMetaData();
	      
	      for(int i = 1; i <= resultMeta.getColumnCount(); i++){
	    	  t.Insertintodb("update `"+name_schema+"`." + name_table_koupot_gemel + " set " + resultMeta.getColumnName(i) + " = replace(" + resultMeta.getColumnName(i) + ", ',', '')");
	    	  System.out.println(resultMeta.getColumnName(i));
	      }
	}

	public void load_data_micpal_nikouy_hova(String path_file, String name_schema, String name_table_nikuy_hova) throws SQLException{
		System.out.println(path_file+"/nicoye_chova.csv'");

		String a = "LOAD DATA  LOCAL INFILE '"
				+ path_file+"/chova.csv'"
				+ " INTO TABLE `"+name_schema+"`." + name_table_nikuy_hova
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  "
				+ " (shem_hevra, shnat_mas, mis_oved, shem_oved, shem_mahlaka, mis_hodesh, bituah_leumi, mas_irgun, sah_bituah_leumi, mas_briut, mas_hahnasa)";

		t.Insertintodb1(a);
		
		ResultSet result = t.gettabledb("select * from `"+name_schema+"`." + name_table_nikuy_hova);
		
	      ResultSetMetaData resultMeta = result.getMetaData();
	      
	      for(int i = 1; i <= resultMeta.getColumnCount(); i++){
	    	  t.Insertintodb("update `"+name_schema+"`." + name_table_nikuy_hova + " set " + resultMeta.getColumnName(i) + " = replace(" + resultMeta.getColumnName(i) + ", ',', '')");
	    	  System.out.println(resultMeta.getColumnName(i));
	      }
	}

	public void load_data_micpal_nikuy_reshut(String path_file, String name_schema, String name_table_nikuy_reshut) throws SQLException{
		System.out.println(path_file+"/nicoye_reshut.csv'");

		String a = "LOAD DATA  LOCAL INFILE '"
				+ path_file+"/reshut.csv'"
				+ " INTO TABLE `"+name_schema+"`." + name_table_nikuy_reshut
				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
				+ "  ( shem_hevra, shnat_mas, mis_oved, shem_oved, shem_mahlaka, mis_hodesh, mis_shura, mis_nikuy, kod_sug_nikuy, shem_nikuy, schum_nikuy, symbol)"

                ;

		t.Insertintodb1(a);
		
		ResultSet result = t.gettabledb("select * from `"+name_schema+"`." + name_table_nikuy_reshut);
		
	      ResultSetMetaData resultMeta = result.getMetaData();
	      
	      for(int i = 1; i <= resultMeta.getColumnCount(); i++){
	    	  t.Insertintodb("update `"+name_schema+"`." + name_table_nikuy_reshut + " set " + resultMeta.getColumnName(i) + " = replace(" + resultMeta.getColumnName(i) + ", ',', '')");
	    	  System.out.println(resultMeta.getColumnName(i));
	      }
	}


	public void modifyTypeToExology(String name_schema, String name_table_101) throws SQLException{

		System.out.println("modifyTypeToExology");

		String s = "update " + name_schema + "." + name_table_101 + " set source = concat(source, ' - ', type), type = ';tlush_tashlum;' where type = 'תשלומים';";
		t.Insertintodb(s);

		s = "update " + name_schema + "." + name_table_101 + " set source = concat(source, ' - ', type), type = ';nikuy_chova;' where type = 'ניכוי חובה' ;";
		t.Insertintodb(s);

		s = "update " + name_schema + "." + name_table_101 + " set source = concat(source, ' - ', type), type = ';tlush_reshut;' where type = 'ניכוי רשות' ;";
		t.Insertintodb(s);

		s = "update " + name_schema + "." + name_table_101 + " set type = ';b_tlush;' where SymbolName = 'סיכום ברוטו' and symbol = 1600;";
		t.Insertintodb(s);

		s = "update " + name_schema + "." + name_table_101 + " set type = ';semelbb;nikuy_chova;' where SymbolName = 'מס בריאות ' and symbol = 5000;";
		t.Insertintodb(s);

		s = "update " + name_schema + "." + name_table_101 + " set type = ';semelbilo;nikuy_chova;' where SymbolName = 'ביטוח לאומי' and symbol = 2500;";
		t.Insertintodb(s);

		s = "update " + name_schema + "." + name_table_101 + " set type = ';semelbilm;' where SymbolName = 'סיכום ביטוח לאומי מעביד' and symbol = 2000;";
		t.Insertintodb(s);

		s = "update " + name_schema + "." + name_table_101 + " set type = ';;' where symbol = 4000 or symbolname regexp 'דמי לידה';";
		t.Insertintodb(s);

		s = "UPDATE  " + name_schema + "." + name_table_101 + " set type = ';tlush_shovy;tlush_tashlum;b_tlush;' WHERE SymbolName REGEXP '^שווי' ;";
		t.Insertintodb(s);
		
		s = "UPDATE  " + name_schema + "." + name_table_101 + " set type = ';tlush_shovy;tlush_tashlum;b_tlush;' WHERE SymbolName REGEXP '^זק.' ;";
		t.Insertintodb(s);
		
		s = "UPDATE  " + name_schema + "." + name_table_101 + " set type = ';tlush_shovy;tlush_tashlum;b_tlush;' WHERE SymbolName REGEXP '^לגמל בלידה' ;";
		t.Insertintodb(s);
		
		s = "UPDATE  " + name_schema + "." + name_table_101 + " set type = ';tlush_shovy;tlush_tashlum;b_tlush;' WHERE SymbolName REGEXP 'רכב קב' ;";
		t.Insertintodb(s);

		s = "UPDATE " + name_schema + "." + name_table_101 + " SET TYPE = ';tlush_reshut;' WHERE Symbol = 3020;";
		t.Insertintodb(s);

		s = "UPDATE " + name_schema + "." + name_table_101 + " SET type_for_gemel = ';shulam_maavid;pensya;sum_shulam;' WHERE Symbol = 1800;";
		t.Insertintodb(s);

		
		s = "UPDATE " + name_schema + "." + name_table_101 + " SET TYPE = ';tlush_tashlum;hofesh;' WHERE Symbolname regexp 'חופש' and type regexp 'tash';";
		t.Insertintodb(s);
		
		s = "UPDATE " + name_schema + "." + name_table_101 + " SET TYPE = ';tlush_tashlum;gmar_heshbon;freeBil;' WHERE Symbolname regexp 'פדיון חופש' and type regexp 'tash';";
		t.Insertintodb(s);

		s = "UPDATE " + name_schema + "." + name_table_101 + " SET TYPE = ';tlush_tashlum;avraa_gmar;' WHERE Symbolname regexp 'הבראה' and type regexp 'tash';";
		t.Insertintodb(s);
		
		s = "UPDATE " + name_schema + "." + name_table_101 + " SET TYPE = ';tlush_tashlum;mahala;' WHERE Symbolname regexp 'מחלה' and type regexp 'tash';";
		t.Insertintodb(s);
	}

	//alfon

	public void createTableAlfonMicpal(String name_schema, String name_table_alfon) throws SQLException{
		
		String s = "CREATE TABLE if not exists " + name_schema + "." + name_table_alfon + " (\n" +
				"  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `cid` int(10) NOT NULL,\n" +
				"  `dyear` int(6) NOT NULL,\n" +
				"  `id` int(11) NOT NULL,\n" +
				"  `num_worker` int(11) NOT NULL,\n" +
				"  `first_name` varchar(50) DEFAULT NULL,\n" +
				"  `last_name` varchar(50) DEFAULT NULL,\n" +
				"  `mahlaka` varchar(50) DEFAULT NULL,\n" +
				"  `birthday` varchar(50) DEFAULT NULL,\n" +
				"  `start_service_date` varchar(50) DEFAULT NULL,\n" +
				"  `finished_service_date` varchar(50) DEFAULT NULL,\n" +
				"  `is_male` varchar(50) DEFAULT NULL,\n" +
				"  `marital_status` varchar(50) DEFAULT NULL,\n" +
				"  `zikuy_points` varchar(50) DEFAULT NULL,\n" +
				"  `job_precent` varchar(50) DEFAULT NULL,\n" +
				"  `vetek` varchar(50) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`in_id`) " +
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='InnoDB free: 1508352 kB; InnoDB free: 2484224 kB; InnoDB fre';";
		
		
		t.Insertintodb(s);

		truncate(name_schema, name_table_alfon);
		
		
	}
	
	public void create101Details(String name_schema, String name_101_alfon) throws SQLException{
		String s = "CREATE TABLE if not exists " + name_schema + "." + name_101_alfon + " (\n" +
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
				"  `intra_division` varchar(45) NOT NULL DEFAULT '-12',\n" +
				"  `intra_division_exp` varchar(45) NOT NULL DEFAULT '-12' ,\n" +
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
				"  `mas_shuly_percent` double NOT NULL DEFAULT '0',"+
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

		t.Insertintodb(s);

		truncate(name_schema, name_101_alfon);
	}

	public void insertTo101Details(String name_schema, String name_table_alfon, String name_101_alfon) throws SQLException{

		System.out.println("\n\n\n micpal.Micpal.insertTo101Details() \n\n\n");

		String a = "delete FROM  " + name_schema + "." + name_table_alfon + " where id = 0 ; ";
		t.Insertintodb(a);

		String s = "INSERT INTO " + name_schema + "." + name_101_alfon + " \n" +
				"(\n" +
				"`cid`,\n" +
				"`dyear`,\n" +
				"`id`,\n" +
				"`num_worker`, " +
				"`first_name`,\n" +
				"`last_name`,\n" +
				"`birthday`,\n" +
				"`is_male`,\n" +
				"`marital_status`,\n" +
				"    zikuy_points, job_precent,    month, start_service_date, finished_service_date)\n" +
				"SELECT "
				+ "cid, "
				+ "dyear, "
				+ "id, "
				+ "num_worker, "
				+ " first_name, "
				+ " last_name , "
				+ "  concat(substring_index(substring_index(birthday, '/', 2), '/', -1), '/', substring_index(birthday, '/', 1), '/19', substring_index(birthday,  '/', -1)) as birthday , "
				+ "if(is_male regexp 'ז' , 1, 0), "
				+ "marital_status,    "
				+ "zikuy_points, "
				+ "job_precent,   "
				+ "'0', "
				+ " concat(substring_index(substring_index(start_service_date, '/', 2), '/', -1), '/', substring_index(start_service_date, '/', 1), '/', if(substring_index(start_service_date,  '/', -1) <= 17 , '20', 19),  substring_index(start_service_date,  '/', -1)) as start_service_date ,  "
				+ " if(finished_service_date = '', '', concat(substring_index(substring_index(finished_service_date, '/', 2), '/', -1), '/', substring_index(finished_service_date, '/', 1), '/', if(substring_index(finished_service_date,  '/', -1) <= 17 , '20', 19),substring_index(finished_service_date,  '/', -1))) as finished_service_date     " 
				+ "   FROM  " + name_schema + "." + name_table_alfon + " group by id, dyear ; " ;

		System.out.println(s);
		
		t.Insertintodb(s);
	}

	public void aliaAlphon(String path, String name_schema, String name_table_alfon, String name_101_alfon, int year1, int year2, int cid) throws SQLException{

		createTableAlfonMicpal(name_schema, name_table_alfon);
		
		for(int i = year1; i <= year2; i++){
			loadAlphon(name_schema, name_table_alfon, path, i, cid);
		}

		create101Details(name_schema, name_101_alfon);


		insertTo101Details(name_schema, name_table_alfon, name_101_alfon);


	}


	private void loadAlphon(String name_schema, String name_table_alfon, String path_file, int year, int cid) throws SQLException {
		
		String path = path_file+ "/" + year + "/alphon.csv";
		System.out.println(path);

		String a = ""
//				+ "LOAD DATA  LOCAL INFILE "
//				+ "'" + path + "'"
//				+ " INTO TABLE `"+name_schema+"`." + name_table_alfon
//				+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 10 LINES  "
//				+ " (num_worker, first_name, mahlaka, id, birthday, start_service_date, finished_service_date, is_male, marital_status, zikuy_points, job_precent, vetek)"
//				+ " set dyear = " + year + ", cid  =  " + cid + " ;"
+"LOAD DATA  LOCAL INFILE  '" + path + "' \n" +
" INTO TABLE `"+name_schema+"`." + name_table_alfon + " \n" +
" FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES \n" +
" (num_worker, first_name, first_name, last_name, id, id, is_male, birthday, birthday, birthday, birthday, birthday, birthday, birthday, birthday, birthday, \n" +
"  marital_status, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, \n" +
"  zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, \n" +
"  zikuy_points, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, \n" +
"  job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, \n" +
"  start_service_date, start_service_date, start_service_date, start_service_date, start_service_date, start_service_date, start_service_date, start_service_date, \n" +
"  finished_service_date)\n" +
" set dyear =  " + year + ", cid  =  " + cid + " ;"
						+ "";
		t.Insertintodb1(a);
		
	}


	public void modifyIdTZ(String name_schema, String tbl_101_sofi, String tableAlphon) throws SQLException {
		
		System.out.println("UPDATE " + name_schema + "." + tbl_101_sofi + " as t1 inner join " + name_schema + "." + tableAlphon + " as t2 "
					+ "on t1.id = t2.num_worker and t1.dyear = t2.dyear    "
					+ "set t1.id = t2.id");
			t.Insertintodb("UPDATE " + name_schema + "." + tbl_101_sofi + " as t1 inner join " + name_schema + "." + tableAlphon + " as t2 "
					+ "on t1.id = t2.num_worker and t1.dyear = t2.dyear    "
					+ "set t1.id = t2.id" );
		
	}

}