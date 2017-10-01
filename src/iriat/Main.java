package iriat;

import java.sql.SQLException;

import mySQL.Trysql;

public class Main {

	public static void main(String[] args) throws InterruptedException, SQLException {
		Trysql t = Trysql.getInstance();
		
		for (int i = 1; i <= 12; i++) {
//			String sql = ""
//			+ "insert into `franck`.`kiryatYam_101`"+
//			"(cid, dyear, id, FullName, Symbol, SymbolName, m" + i + ")"+
//			"SELECT '222729884' as cid, '2015' as dyear, id as id, concat(firstname,' ',lastname) as  FullName, '1000' as Symbol, 'ברוטו' as SymbolName,sum(bruto) as val "
//			+ "FROM franck.kiryatYam_r66_2015 "+
//			"where CAST(substring(period,5) AS UNSIGNED)=" + i + " "
//					+ "group by idWorker, period"+
//			"     on duplicate key update m" + i + "=values(m"+i+")  ;  ";
//
//			
//			 String sql=" insert into `franck`.`kiryatYam_101` (cid, dyear, id, FullName, Symbol, SymbolName, m"+i+") "
//			 		+ "SELECT '222729884' as cid, '2015' as dyear, idWorker as id, concat(firstname,' ',lastname) as  FullName, '2000' as Symbol, 'ביטוח לאומי' as SymbolName,sum(bl) as val "
//			 		+ "FROM franck.kiryatYam_r66_2015 where CAST(substring(period,5) AS UNSIGNED)="+i+" "
//			 				+ "group by idWorker, period      "
//			 				+ "on duplicate key update m"+i+"=values(m"+i+") ;";
//			 
//			 
			 
			 
//			String sql=" insert into `franck`.`kiryatYam_101` (cid, dyear, id, FullName, Symbol, SymbolName, m"+i+") "
//					+ "SELECT '222729884' as cid, '2015' as dyear, idWorker as id, concat(firstname,' ',lastname) as  FullName, '3000' as Symbol, 'קופות גמל' as SymbolName,sum(kpg) as val "
//					+ "FROM franck.kiryatYam_r66_2015 "
//					+ "where CAST(substring(period,5) AS UNSIGNED)="+i+"  "
//							+ "group by idWorker, period   "
//							+ "on duplicate key update m"+ i +"=values(m"+i+") ;";
//			
			
//			$res=mysql_query($sql) or die($sql." ".mysql_error());
			
			
			String sql=" insert into `franck`.`kiryatYam_101`  (cid, dyear, id, FullName, Symbol, SymbolName, m"+i+")  "
					+ "SELECT '222729884' as cid, '2015' as dyear, idWorker as id, concat(firstname,' ',lastname) as  FullName, '4000' as Symbol, 'מס שכר' as SymbolName,"
					+ "sum(payrollTax) as val "
					+ ""
					+ "FROM franck.kiryatYam_r66_2015 "
					+ "where CAST(substring(period,5) AS UNSIGNED)="+i+" "
							+ "group by idWorker, period "
							+ "on duplicate key update m"+i+"=values(m"+i+"); ";
			System.out.println(sql);
			
//			 $res=mysql_query($sql) or die($sql." ".mysql_error());
//			 echo "$sql <br>";	 
		}         
		
		Thread.sleep(30*1000);

	}

}
