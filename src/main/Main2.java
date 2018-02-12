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
			for(int j = 2009; j <= 2017; j++){
				System.out.println(j);
				if(j != 2015)
					for(int i = 1; i <= 12; i++){
						//
						//
						//
						System.out.println("debut : " + i);

						String sheilta = ""
								//							+ "LOAD DATA LOCAL INFILE '/home/user1/hevra/otsma/avidar/" + j + "/" + i + ".csv'  "
								//							+ "INTO TABLE " + t.getConnectionMySql().getSchema()+ ".avidar_2009_2017_kvatsim       "
								//							+ "FIELDS TERMINATED BY ','enclosed by '\"'LINES TERMINATED BY '\n' IGNORE 1 LINES"
								//							+ " (num_worker,f_name,l_name,  tat_mifal, agaf, machlaka, name_machlaka, derog, tear_derog, darga, teor_darga, "
								//							+ "tchilat_avoda, id, kod_esok, tear_isok, symbol, symbol_name,"
								//							+ "m" + i +", sub, sub, sub, sub, sub, sub, sub, sub, sub, sub, sub, sub, sub, sub, sub) set dyear=" + j + ", kovets = " + i + " ;"


								//							""
								+ "LOAD DATA LOCAL INFILE '/home/user1/hevra/otsma/avidar/gemel/" + j + "/" + i + ".csv'  "
								+ "INTO TABLE " + t.getConnectionMySql().getSchema()+ ".gemelAvidar  "  + (j != 2017 ? "   charset hebrew   " : "") 
								+ " FIELDS TERMINATED BY ','enclosed by '\"'LINES TERMINATED BY '\n' IGNORE 1 LINES"
								+ " (shem_kupa, shem_kupa, num_worker, num_worker, num_worker, num_worker, num_worker, num_worker, "
								+ "num_worker, num_worker, num_worker, num_worker, num_worker, "
								+ "id, last_name, first_name, misra, misra, misra, misra, bruto, bruto, bruto, bruto, ahuz_nikuy_befoal, "
								+ "ahuz_hafracha_befoal, ahuz_pits_befoal, bruto, bruto, bruto, bruto, bruto, nikuy, hafracha, pits) set dyear=" + j + ", m = " + i + ", cid = '924123749' ;"


								//							+ "insert into Franck_new.avidar_2009_2017 (cid, id, dyear, num_worker, m1, m2, m3, m4, m5, m6,  m7, m8, m9, m10, m11, m12, sub, f_name, l_name\n" +
								//", symbol, symbol_Name)\n" +
								//"select cid, id, dyear, num_worker, sum(m1) , sum(m2) , sum(m3) , sum(m4) , sum(m5) , sum(m6) \n" +
								//", sum(m7) , sum(m8) , sum(m9) , sum(m10) , sum(m11) , sum(m12) , sub,  f_name,   l_name , symbol, symbol_Name\n" +
								//"from Franck_new.avidar_2009_2013_kvatsim where dyear = " + j + " group by id, dyear, symbol, symbol_name, sub;"
								+ "";
						t.Insertintodb(sheilta);
						//										String load = "LOAD DATA  LOCAL INFILE  '/home/user1/hevra/micpal/gil/alphon/alphon/" + j + ".csv' \n" +
						//					" INTO TABLE franck.alphonGil\n" +
						//					" FIELDS TERMINATED BY ','  ENCLOSED BY  '\\\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES \n" +
						//					" (num_worker, first_name, first_name, last_name, id, id, is_male, birthday, birthday, birthday, birthday, birthday, birthday, birthday, birthday, birthday, \n" +
						//					"  marital_status, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, \n" +
						//					"  zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, zikuy_points, \n" +
						//					"  zikuy_points, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, \n" +
						//					"  job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, job_precent, \n" +
						//					"  start_service_date, start_service_date, start_service_date, start_service_date, start_service_date, start_service_date, start_service_date, start_service_date, \n" +
						//					"  finished_service_date)\n" +
						//					" set dyear =  " + j + ", cid  =   932064389 ;";
						//					t.Insertintodb(load);
						//

						System.out.println("fin : " + i + "\n\n");
					}

				//				String s = "insert into Franck_new.avidar_2009_2017 (cid, id, dyear, num_worker, m1, m2, m3, m4, m5, m6,  m7, m8, m9, m10, m11, m12, sub, f_name, l_name\n" +
				//						", symbol, symbol_Name)\n" +
				//						"select cid, id, dyear, num_worker, sum(m1) , sum(m2) , sum(m3) , sum(m4) , sum(m5) , sum(m6) \n" +
				//						", sum(m7) , sum(m8) , sum(m9) , sum(m10) , sum(m11) , sum(m12) , sub,  f_name,   l_name , symbol, symbol_Name\n" +
				//						"from Franck_new.avidar_2009_2013_kvatsim where dyear = " + j + " group by id, dyear, symbol, symbol_name, sub;";
				//
				//				t.Insertintodb(s);
				//
				//				s = "insert  into Franck_new.AVIDAR_2009_2017_101\n" +
				//						"            ( `FullName`,`cid`,`dyear`,`id`,Symbol,`SymbolName`,\n" +
				//						"            m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12, \n" +
				//						"            type, num_worker, original_id, source) \n" +
				//						"            \n" +
				//						"\n" +
				//						"\n" +
				//						"SELECT concat(f_name,'  ',l_name) , 924123749,`dyear`,`id`,`symbol`, \n" +
				//						"CONCAT(`symbol_name`, IF(sub = '', '', ' : '),  IFNULL(sub,'')) AS symbolname ,\n" +
				//						"sum(m1),sum(m2),sum(m3),sum(m4),sum(m5),sum(m6),sum(m7),sum(m8),sum(m9),sum(m10),sum(m11), sum(m12), IFNULL(sub,'') as type, \n" +
				//						"num_worker, tat_mifal, 'original_otsma' \n" +
				//						"\n" +
				//						"   FROM Franck_new.avidar_2009_2017  where dyear = " + j + " group by dyear,id,`symbol`, symbol_name, sub ;";
				//				t.Insertintodb(s);
				//				System.out.println("fin " + j + "\n\n");


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