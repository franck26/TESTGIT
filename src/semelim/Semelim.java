package semelim;

import java.util.regex.*;
import java.sql.ResultSet;
import java.sql.SQLException;

import mySQL.Trysql;

public class Semelim {

	private Trysql tr;
	private static Semelim s = null;

	private Semelim(){
		tr = Trysql.getInstance();
	}

	public static Semelim getInstance(){
		if(s == null){
			s = new Semelim();
		}
		return s;
	}

	public void create_table(String name_schema, String name_table) throws SQLException{
		String a="CREATE TABLE  if not exists " + name_schema + ".`"+name_table+"` (\n" +
				"  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n" +
				"  `name_tz` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `mahlaka` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
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
				"  `symbol` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `symbolName` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `num_oved` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `full_name` varchar(45) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  PRIMARY KEY (`in_id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";

		tr.Insertintodb1(a);

		truncate(name_schema, name_table);
	}

	public void truncate(String name_schema, String name_table) throws SQLException{

		String x = "TRUNCATE `" + name_schema + "`.`" + name_table + "`;";
		tr.Insertintodb1(x);
	}

	public void load(String name_schema,String name_table) throws SQLException {

			String a = "LOAD DATA  LOCAL INFILE"
					+ " '/home/user1/hevra/hevroth/avtaha/2016.csv'"
					+ " INTO TABLE  `"+name_schema+"`." + name_table
					+ "   FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES "
					+ "  (  name_tz, mahlaka, a, bruto, mas_hachnassa, leumi, tagmoulim, keren, istadrout, nikouy_aherim, schar_neto,  nikouy_reshut, neto_letashlum, symbol, symbolName)"
					+ ";";

			tr.Insertintodb1(a);

	}
	
	public void create_table_symbol(String name_schema, String name_table_symbol) throws SQLException{
		String a="CREATE TABLE  if not exists " + name_schema + ".`"+name_table_symbol+"` (\n" +
				"  `in_id` int(11) NOT NULL AUTO_INCREMENT,\n" +
				"  `symbol` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  `symbolName` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,\n" +
				"  PRIMARY KEY (`in_id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";

		tr.Insertintodb1(a);
		
		truncate(name_schema, name_table_symbol);
		
	}
	
	public void insert_symbol_in_table_symbol(String name_schema, String name_table_symbol, String name_table) throws SQLException{
//		String a="insert into " + name_schema + ". " + name_table_symbol + ""
//				+ "(symbol, symbolName)"
//				+ "Select symbol, symbolName "
//				+ "FROM " + name_schema + "." + name_table + ""
//				+ " group by symbolName";
//		System.out.println(a);
//		tr.Insertintodb1(a);
//		
//
//		String v = "ALTER TABLE " + name_schema + "." + name_table_symbol + "AUTO_INCREMENT = 1500";
//		tr.Insertintodb1(v);
		
		//On va chercher une ligne dans la base de données
	      String query = "SELECT symbol, symbolName FROM " + name_schema + "." + name_table_symbol;         

	      //On exécute la requête
	      ResultSet res = tr.gettabledb(query);
	      String regex = "[0-9a-zA-Z]?[A-Z]+[0-9]*";
	      int i = 1600;
	      while(res.next()){
//	    	  System.out.println(res.getString("symbol"));
	    	  if(res.getString("symbol").matches(regex)){
	    		  System.out.println(res.getString("symbol"));
	    		  System.out.println("on va te changer toi");
	    		  
	    		  
	    		  String s = "update " + name_schema + "." + name_table_symbol + " set symbol = '" + i + "' where symbolName = '"
		    		  		+ "" + res.getString("symbolName") + "'";
	    		  System.out.println(s);
	    		  tr.Insertintodb(s);
	    		  
	    		  
	    		  i += 1;
	    	  }
	      }
	}

}
