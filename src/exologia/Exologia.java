package exologia;

import java.sql.SQLException;

import mySQL.Trysql;

public class Exologia {

	Trysql t = Trysql.getInstance();

	public void createTableTempExologia(String name_schema, String name_table_temp, String fields) throws SQLException {

		System.out.println("create tbl temp");



		String s = "CREATE TABLE if not exists " + name_schema + "." + name_table_temp + "(";

		String [] array = fields.split(", ");

		for (int i = 0; i < array.length - 1; i++) {
			s += array[i] + " VARCHAR(200), \n";
		}


		s += array[array.length - 1] + " VARCHAR(200) ) DEFAULT CHARSET=utf8;";

		System.out.println(s);

		t.Insertintodb(s);

		truncate(name_schema, name_table_temp);

	}

	public void createExologia(String name_schema, String name_table_exologia) throws SQLException{

		System.out.println("create tbl exologia");

		String s = "create table if not exists " + name_schema + "." + name_table_exologia + "(" +
				"  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `cid` int(10) unsigned NOT NULL,\n" +
				"  `dyear` smallint(6) NOT NULL DEFAULT '1990',\n" +
				"  `Symbol` int(11) NOT NULL,\n" +
				"  `SymbolName` varchar(100) NOT NULL,\n" +
				"  `IsMasMaasikim` tinyint(1) NOT NULL DEFAULT '0',\n" +
				"  `IsMasSachar` tinyint(1) NOT NULL DEFAULT '0',\n" +
				"  `IsBil` tinyint(1) NOT NULL DEFAULT '0',\n" +
				"  `IsMasHachnasa` tinyint(1) NOT NULL DEFAULT '0',\n" +
				"  `IsMasBruto` tinyint(1) NOT NULL DEFAULT '0',\n" +
				"  `Special` varchar(100) DEFAULT '',\n" +
				"  `type` varchar(100) NOT NULL DEFAULT '0',\n" +
				"  `note` varchar(500) DEFAULT '',\n" +
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
				"  `run_version` int(11) DEFAULT NULL,\n" +
				"  `SymbolName___arabic` varchar(179) DEFAULT '',\n" +
				"  `SymbolName___chinese` varchar(179) DEFAULT '',\n" +
				"  `SymbolName___english` varchar(179) DEFAULT '',\n" +
				"  `SymbolName___french` varchar(179) DEFAULT '',\n" +
				"  `SymbolName___hebrew` varchar(179) DEFAULT '',\n" +
				"  `SymbolName___portuguese` varchar(179) DEFAULT '',\n" +
				"  `SymbolName___russian` varchar(179) DEFAULT '',\n" +
				"  `SymbolName___spanish` varchar(179) DEFAULT '',\n" +
				"  `SymbolName___Italiano` varchar(261) DEFAULT '',\n" +
				"  `template` varchar(300) DEFAULT 'אינפורמטיבי',\n" +
				"  `comp_name` varchar(100) DEFAULT '',\n" +
				"  `to_edit` tinyint(3) unsigned DEFAULT '1',\n" +
				"  `version` int(11) DEFAULT '0',\n" +
				"  `permission` int(11) unsigned DEFAULT NULL,\n" +
				"  PRIMARY KEY (`in_id`),\n" +
				"  UNIQUE KEY `indi1` (`cid`,`dyear`,`Symbol`,`SymbolName`),\n" +
				"  KEY `Index_2` (`version`,`cid`,`dyear`,`Symbol`,`SymbolName`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;";

		t.Insertintodb(s);

		truncate(name_schema, name_table_exologia);
	}

	public void load(String path, String name_schema, String name_table_temp, String fields, int i) throws SQLException {

		System.out.println("load year " + i);

		String s = "LOAD DATA  LOCAL INFILE '" + path + "/" + i + ".csv' "
				+ " INTO TABLE  " + name_schema + "." + name_table_temp
				+ " FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 2 LINES   \n"
				+ "("+ fields + ") set dyear = " + i + ";";

		t.Insertintodb(s);
	}

	public void insertToExologia(String name_schema, String name_table_exologia, int cid, String name_table_temp) throws SQLException{

		System.out.println("insert");

		//tlush_tashlum
		String s = "insert ignore into " + name_schema + "." + name_table_exologia
				+ "(cid, dyear, symbol, symbolName, Special)"
				+ "Select " + cid + ", dyear,  symbol, '', ';tlush_tashlum;' from " + name_schema + "." + name_table_temp
				+ "	where ofi IN (1, 2, 4, 12);";

		t.Insertintodb(s);

		//shovy
		s = "insert ignore into " + name_schema + "." + name_table_exologia
				+ "(cid, dyear, symbol, symbolName, Special)"
				+ "Select " + cid + ", dyear,  symbol, '', ';tlush_tashlum;tlush_shovy;' from " + name_schema + "." + name_table_temp
				+ "	where ofi = 5;";

		t.Insertintodb(s);


		//pensya Maavid
		s = "insert ignore into " + name_schema + "." + name_table_exologia
				+ "(cid, dyear, symbol, symbolName, type)"
				+ "Select " + cid + ", dyear,  symbol, '', ';shulam_maavid;pensya;sum_shulam;' from " + name_schema + "." + name_table_temp
				+ "	where ofi = 90;";

		t.Insertintodb(s);



		//neto
		s = "insert ignore into " + name_schema + "." + name_table_exologia
				+ "(cid, dyear, symbol, symbolName, Special)"
				+ "Select " + cid + ", dyear,  symbol, '', ';tlush_neto;' from " + name_schema + "." + name_table_temp
				+ "	where ofi = 99;";

		t.Insertintodb(s);


		//reshut
		s = "insert ignore into " + name_schema + "." + name_table_exologia
				+ "(cid, dyear, symbol, symbolName, Special)"
				+ "Select " + cid + ", dyear,  symbol, '', ';tlush_reshut;' from " + name_schema + "." + name_table_temp
				+ "	where ofi = 50;";

		t.Insertintodb(s);

	}

	public void truncate(String name_schema, String name_table_temp) throws SQLException {
		String s = "truncate table " + name_schema + "." + name_table_temp;

		t.Insertintodb(s);

	}

}
