package argal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Statement;

import mySQL.Trysql;

public class Argal {
	Trysql tr = Trysql.getInstance();
	private static Argal a = null;

	public static Argal getInstance() {
		if (a == null)
			a = new Argal();
		return a;
	}

	public void create_table(String name_schema, String name_table) throws SQLException {

		System.out.println("create table " + name_schema + "." + name_table + "....");
		String s = "CREATE TABLE  if not exists  " + name_schema + "." + name_table
				+ "(`in_id` INT NOT NULL AUTO_INCREMENT," + "`sikoum` VARCHAR(200) NULL, "
				+ "`l_name` VARCHAR(200) NULL, " + "`f_name` VARCHAR(200) NULL, " + "`id` VARCHAR(200) NULL,"
				+ "`symbol` VARCHAR(200) NULL," + "`symbolName` VARCHAR(200) NULL," + "`total` VARCHAR(200) NULL,"
				+ "`type` VARCHAR(200) NULL," + "`m1` VARCHAR(200) NULL," + "`m2` VARCHAR(200) NULL,"
				+ "`m3` VARCHAR(200) NULL," + "`m4` VARCHAR(200) NULL," + "`m5` VARCHAR(200) NULL,"
				+ "`m6` VARCHAR(200) NULL," + "`m7` VARCHAR(200) NULL," + "`m8` VARCHAR(200) NULL,"
				+ "`m9` VARCHAR(200) NULL," + "`m10` VARCHAR(200) NULL," + "`m11` VARCHAR(200) NULL,"
				+ "`m12` VARCHAR(200) NULL,"

				+ "`memoutsa` VARCHAR(200) NULL," + "`dyear` VARCHAR(200) NULL," + "PRIMARY KEY (`in_id`))";

		tr.Insertintodb1(s);

		s = "truncate table " + name_schema + "." + name_table;

		tr.Insertintodb1(s);

		
		System.out.println("create table ok !");

	}

	public void load(String name_schema, String name_table, int year) throws SQLException {
		System.out.println("load data year " + year + " in process .......");

		String s = "LOAD DATA  LOCAL INFILE '/home/user1/hevra/argal/yeshiva/" + year + ".csv' " + "INTO TABLE "
				+ name_schema + "." + name_table
				+ " FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' "
				+ "IGNORE 1 LINES   (sikoum, id, l_name, f_name, type,  symbol, symbolName, m12, m11, m10, m9, m8, m7, m6, m5, m4, m3, m2, m1) "
				+ "set dyear=" + year;

		tr.Insertintodb(s);

		s = "delete from " + name_schema + "." + name_table + " where sikoum = 'סיכום  למפעל : '";

		tr.Insertintodb1(s);

		System.out.println("load data year " + year + " ok !");

	}

	public void create_tabel_101(String name_schema, String name_table) throws SQLException {

		System.out.println("create table 101 ...");

		String a = "CREATE TABLE  if not exists " + name_schema + "." + name_table + "(\n"
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

		a = "Truncate table " + name_schema + "." + name_table;
		tr.Insertintodb1(a);

		System.out.println("create table 101 ok !");
	}

	public void insertTo101(String name_schema, String name_table_101, String name_table, int cid) throws SQLException {

		System.out.println("insert in 101");

		String s = "insert ignore into " + name_schema + "." + name_table_101
				+ "(cid, dyear, FullName, id, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, type) "
				+ "select " + cid
				+ ", dyear, concat(l_name, ' ' , f_name), id, symbol, concat(symbolName, ' - ', type) , m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, type "
				+ "from " + name_schema + "." + name_table+""
				+ " where type = 'השתתפות'";

		System.out.println(s);
		
		tr.Insertintodb1(s);
		
		s = "insert ignore into " + name_schema + "." + name_table_101
				+ "(cid, dyear, FullName, id, Symbol, SymbolName, m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, type) "
				+ "select " + cid
				+ ", dyear, concat(l_name, ' ' , f_name), id, symbol, symbolName , m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, type "
				+ "from " + name_schema + "." + name_table+""
				+ " where type <> 'השתתפות'";
		System.out.println(s);
		
		tr.Insertintodb1(s);
		
		
		
		
		s = "SELECT distinct  symbol, symbolname FROM " + name_schema + "." + name_table_101 + " where symbol = 0 ";
		ResultSet r = tr.gettabledb(s);
		int i = 1001;
		
		while(r.next()){
			s = "UPDATE " + name_schema + "." + name_table_101 + " SET symbol = " + i +  " WHERE symbolName = '" + r.getString(2) + "' and symbol = 0;\n\n";
			System.out.println(s);
			Connection conn = tr.getConnectionMySql().getConn();
			PreparedStatement statement = conn.prepareStatement(s);
			//
//			statement.setString(1, r.getString(2));

			statement.executeUpdate();
			
//			tr.Insertintodb("UPDATE " + name_schema + "." + name_table_101 + " SET symbol = " + i +  " WHERE symbolName = '" + r.getString(2) + "' and symbol = 0;");
			i++;
		} 
		
		s =  "SELECT distinct SymbolName "
				+ "FROM "
				+ name_schema + "." + name_table_101
				+ " where SymbolName = 'סיכום  לסוג נתון : השתתפות - השתתפות';";
		
		r = tr.gettabledb(s);

		while(r.next()){
			String[] parts = r.getString(1).split("-");
			
			s = "UPDATE " + name_schema + "." + name_table_101 + " SET symbolname = '" + parts[0] +  "' where SymbolName = 'סיכום  לסוג נתון : השתתפות - השתתפות';\n\n";
			System.out.println(s);
			Connection conn = tr.getConnectionMySql().getConn();
			PreparedStatement statement = conn.prepareStatement(s);

			statement.executeUpdate();
		}
		
		System.out.println("insert perfect !!!");

	}

	public void updateTotal(String name_schema, String name_table_101) throws SQLException {
		String s = "update " + name_schema + "." + name_table_101
				+ " set total = m1+m2+m3+m4+m5+m6+m7+m8+m9+m10+m11+m12";
		tr.Insertintodb1(s);

		System.out.println("update total ok !!");
	}

	public void changeSymbolSymbolname(String name_schema, String name_table) throws SQLException {
		String s = "select distinct sikoum from " + name_schema + "." + name_table + " where sikoum <> ''";
		ResultSet rs = tr.gettabledb(s);
		String a = "";
		for (int i = 1; rs.next(); i++) {
//			a = "UPDATE " + name_schema + "." + name_table + " " + "SET symbolName='" + rs.getString(1) + "', symbol = "
//					+ i * 1000 + "WHERE sikoum ='" + rs.getString(1) + "' ;\n\n";

			Connection conn = tr.getConnectionMySql().getConn();
			PreparedStatement statement = conn.prepareStatement("UPDATE " + name_schema + "." + name_table + " "
					+ "SET symbolName=?, symbol = '" + i * 1000 + "'" + "WHERE sikoum =? ;\n\n");

			statement.setString(1, rs.getString(1));
			statement.setString(2, rs.getString(1));

			statement.executeUpdate();

		}

		// s = "SELECT distinct symbol, symbolName FROM " + name_schema + "." +
		// name_table;
		// rs = tr.gettabledb(s);
		// while(rs.next()){
		//
		//// System.out.println(j + " " + rs.getString("symbolName"));
		//// a = "UPDATE " + name_schema + "." + name_table + " "
		//// + "SET symbol = '" + j + "' "
		//// + "WHERE symbolName ='" + rs.getString(2) + "' ;\n\n";
		////
		// Connection conn = tr.getConnectionMySql().getConn();
		// PreparedStatement statement = conn.prepareStatement("UPDATE " +
		// name_schema + "." + name_table + ""
		// + " SET symbol = '" + j + "' "
		// + "WHERE symbolName =?;\n\n");
		////
		// statement.setString(1,rs.getString(2));
		////
		// statement.executeUpdate();
		//////
		// System.out.println(j++);
		// }

		s = "SELECT distinct symbol, symbolName, type FROM " + name_schema + "." + name_table + " where symbolName like '=%' "
				+ "or symbolName = '' " + "or symbolName = 'ותק===='";
		System.out.println(s);
		rs = tr.gettabledb(s);
		while (rs.next()) {
			Connection conn = tr.getConnectionMySql().getConn();
			PreparedStatement statement = conn.prepareStatement("UPDATE " + name_schema + "." + name_table + ""
					+ " SET symbolName = ?" + "WHERE symbolName =?;\n\n");
			//
			statement.setString(1, rs.getString(3));
			statement.setString(2, rs.getString(2));

			statement.executeUpdate();
			System.out.println(rs.getString(2));
		}

//		System.out.println("update symbol ");
//		a = "  UPDATE " + name_schema + "." + name_table + "\n"
//				+ "SET SymbolName = substring(SymbolName,1,position(':' in SymbolName) )\n"
//				+ "where  type not like '%השתתפות%' ;";
//
//		tr.Insertintodb1(a);
	}

}
