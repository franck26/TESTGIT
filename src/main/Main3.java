package main;

import java.sql.SQLException;
import java.util.Vector;

import mySQL.Trysql;

public class Main3 {

	public static void main(String[] args) throws SQLException {
		Trysql t = Trysql.getInstance();


//		Vector<String> value = new Vector<>();
//
//		value.add("bruto");value.add("nikuy");value.add("hafracha");value.add("pits");value.add("ahuz_nikuy_befoal");value.add("ahuz_hafracha_befoal");value.add("ahuz_pits_befoal");		
//
//
//		Vector<String> symbolname = new Vector<>();
//
//		symbolname.add("ברוטו - ");
//		symbolname.add("ניכוי עובד - ");
//		symbolname.add("הפרשת מעביד - ");
//		symbolname.add("פיצויים - ");
//		symbolname.add("אחוז ניכוי בפועל - ");
//		symbolname.add("החוז הפרשה בפועל - ");
//		symbolname.add("אחוז פיצויים בפועל - ");
//
//		for (int a = 0; a < 7; a++){
//			for (int b = 1; b <= 5 ; b++){
//				String hagdara = "";
//
//				switch (b) {
//				case 1:
//					hagdara = "קהש" ;
//					break;
//
//				case 2:
//					hagdara = "ציבורי" ;
//					break;
//
//				case 3:
//					hagdara = "נסיעות" ;
//					break;
//
//				case 4:
//					hagdara = "ש.נוספות" ;
//					break;
//
//				case 5:
//					hagdara = "רגיל" ;
//					break;
//
//				default:
//					break;
//				}
				for(int i = 1; i <= 12; i++){
					System.out.println(i);

//					String s = "insert ignore into Franck_new.tbl_101 \n"
//							+ "            (`FullName`,`cid`,`dyear`,`id`, num_worker, `Symbol`,`SymbolName`,m" + i + ") \n"
//							+ "            SELECT   concat(last_name, ' ', first_name), cid,`dyear`,`id`, `num_worker`, 40"+ a + b +", '" + symbolname.get(a) + hagdara + "', sum(" + value.get(a)+ ") "
//							+ "				  \n"
//							+ "            FROM  Franck_new.gemelAvidar where m=" + i + " and num = " + b + " group by id,dyear\n"
//							+ "            on duplicate key update m" + i + "=values(m" + i + ");";

					
					String s = "insert into Franck_new.`tbl_101_3`(fullname, id, dyear, cid, symbol, symbolname, m" + i +", type, source, num_worker) \n"
							+ " SELECT fullname, id, dyear, '910280924', 555, 'שעות נוכחות' , shaoth, type, 'original_micpal' , num_worker"
							+ " FROM Franck_new.lotanNohehour2008_2017 where m=" + i + " \n"
							+ "            on duplicate key update m" + i + "=values(m" + i + ");";
//					System.out.println(s);
					t.Insertintodb(s);
				}
//			}
//		}
		System.out.println("fini");
	}

}
