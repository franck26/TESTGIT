package main;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import com.sun.java_cup.internal.runtime.virtual_parse_stack;

import mySQL.Trysql;

public class Main2 {

	public static void main(String[] args) {
		Trysql t = Trysql.getInstance();

		try {
			for(int i = 6; i <= 12; i++){
				


				System.out.println("debut : " + i);
				
				String sheilta = "LOAD DATA LOCAL INFILE '/home/user1/hevra/otsma/avidar/2015/" + i + ".csv'  "
						+ "INTO TABLE Franck_new.avidar_2017_2017       "
						+ "FIELDS TERMINATED BY ','enclosed by '\"'LINES TERMINATED BY '\n' IGNORE 1 LINES"
						+ " (num_worker,f_name,l_name,  tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, "
						+ "tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,"
						+ "m" + i +", sub, sub, sub, sub, sub, sub, sub, sub, sub, sub, sub, sub, sub, sub, sub) set dyear=2015, kovets = " + i + " ;";
				
				t.Insertintodb(sheilta);

				System.out.println("fin : " + i + "\n\n");
			}
			
			
			
			System.out.println("fini");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		//atamoth
		
//		try {
//			ResultSet rs = t.gettabledb("select  total  , id, Symbol, SymbolName from diff_taxes_reports.tbl_101 where cid = 777777777 and Symbol = 66 and dyear = 2017 and (total < -100 or total > 100)");
//
//			Vector<Integer> id = new Vector<>();
//			Vector<Double> total = new Vector<>();
//			while(rs.next()){
//				id.add(rs.getInt(2));
//				total.add(rs.getDouble(1));
//			}
//
//
//			Vector<Integer> id1 = new Vector<>();
//			Vector<String> symbol = new Vector<>();
//			Vector<String> symbolName = new Vector<>();
//
//			for(int i = 0; i < id.size(); i++){
//				Trysql t1 = Trysql.getInstance();
//
//				ResultSet rs1 = t1.gettabledb("select id, Symbol, SymbolName from diff_taxes_reports.tbl_101 where cid = 777777777 "
//						+ "and dyear = 2017 and  total  = '"+ total.get(i) +"' and id = '"+ id.get(i) +"' and Symbol <> 66");
//				
//				if(rs1.getFetchSize() != 0){
//					id1.add(rs1.getInt("id")); 
//					symbol.add(rs1.getString("symbol"));
//					symbolName.add(rs1.getString("symbolName"));
//				}
//			}
//
//			for(int j = 0; j < id1.size(); j++){
//				System.out.println(id1.get(j) + "   " + symbol.get(j) + "    " + symbolName.get(j));
//			}
//
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
		

	}
}