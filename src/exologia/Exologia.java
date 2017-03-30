package exologia;

import java.sql.SQLException;

import mySQL.Trysql;

public class Exologia {

	Trysql t = Trysql.getInstance();

	public void createTableTempExologia(String name_schema, String name_table_temp) throws SQLException {
		
		System.out.println("create tbl temp");
		
		String s = "CREATE TABLE if not exists " + name_schema + "." + name_table_temp + "(" + "symbol VARCHAR(200),"
				+ "tokef VARCHAR(200)," + "gmar_tokef VARCHAR(200)," + "ofi VARCHAR(200),"
				+ "symbolname_male VARCHAR(200)," + "symbolname_katsar VARCHAR(200)," + "taarif VARCHAR(200),"
				+ "ahpala VARCHAR(200)," + "yeridat_mida VARCHAR(200)," + "mas_hachnassa VARCHAR(200),"
				+ "soug_lemass VARCHAR(200)," + "bl VARCHAR(200)," + "a VARCHAR(200)," + "aa VARCHAR(200),"
				+ "aaa VARCHAR(200)," + "aaaa VARCHAR(200)," + "aaaaa VARCHAR(200)," + "aaaaaa VARCHAR(200)" + ");";

		t.Insertintodb(s);

		truncate(name_schema, name_table_temp);

	}

	public void createExologia(String name_schema, String name_table_exologia) throws SQLException{
		
		System.out.println("create tbl exologia");
		
		String s = "create table if not exists " + name_schema + "." + name_table_exologia + "("
				+ "in_id	int(10),"
				+ "cid	int(10),"
				+ "dyear	smallint(6),"
				+ "Symbol	int(11),"
				+ "SymbolName	varchar(100),"
				+ "IsMasMaasikim	tinyint(1),"
				+ "IsMasSachar	tinyint(1),"
				+ "IsBil	tinyint(1),"
				+ "IsMasHachnasa	tinyint(1),"
				+ "IsMasBruto	tinyint(1),"
				+ "Special	varchar(100),"
				+ "type	varchar(100),"
				+ "note	varchar(500),"
				+ "m1	double,"
				+ "m2	double,"
				+ "m3	double,"
				+ "m4	double,"
				+ "m5	double,"
				+ "m6	double,"
				+ "m7	double,"
				+ "m8	double,"
				+ "m9	double,"
				+ "m10	double,"
				+ "m11	double,"
				+ "m12	double,"
				+ "total	double,"
				+ "run_version	int(11),"
				+ "SymbolName___arabic	varchar(179),"
				+ "SymbolName___chinese	varchar(179),"
				+ "SymbolName___english	varchar(179),"
				+ "SymbolName___french	varchar(179),"
				+ "SymbolName___hebrew	varchar(179),"
				+ "SymbolName___portuguese	varchar(179),"
				+ "SymbolName___russian	varchar(179),"
				+ "SymbolName___spanish	varchar(179),"
				+ "SymbolName___Italiano	varchar(261),"
				+ "template	varchar(300),"
				+ "comp_name	varchar(100),"
				+ "to_edit	tinyint(3),"
				+ "version	int(11),"
				+ "permission	int(11)"
				+ ");";
		
		t.Insertintodb(s);
		
		truncate(name_schema, name_table_exologia);
	}

	public void load(String path, String name_schema, String name_table_temp) throws SQLException {
		
		System.out.println("load");
		
		String s = "LOAD DATA  LOCAL INFILE '" + path + "/exologia.csv' "
				+ " INTO TABLE  " + name_schema + "." + name_table_temp
				+ " FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES   \n"
				+ "(symbol, tokef, gmar_tokef, ofi, symbolname_male, symbolname_katsar, "
				+ "taarif,"
				+ "ahpala, yeridat_mida,"
				+ " mas_hachnassa, soug_lemass, bl);";

		t.Insertintodb(s);
	}
	
	public void insertToExologia(String name_schema, String name_table_exologia, int dyear, int cid, String name_table_temp) throws SQLException{
		
		System.out.println("insert");
		
		String s = "insert ignore into " + name_schema + "." + name_table_exologia
				+ "(cid, dyear, symbol, symbolName)"
				+ "Select " + cid + ", " + dyear + ",  symbol, symbolname_male from " + name_schema + "." + name_table_temp
				+ "	where bl=1 and mas_hachnassa = 1 and ofi = 1;";
		
		t.Insertintodb(s);
		
		s = "update " + name_schema + "." + name_table_exologia + " set Special = ';tlush_tashlum;'";
		
		t.Insertintodb(s);
		
	}

	public void truncate(String name_schema, String name_table_temp) throws SQLException {
		String s = "truncate table " + name_schema + "." + name_table_temp;

		t.Insertintodb(s);

	}

}
