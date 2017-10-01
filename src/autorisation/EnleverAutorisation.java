package autorisation;

import java.beans.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mySQL.Trysql;

public class EnleverAutorisation {

	public static void main(String[] args) throws SQLException, InterruptedException {
		//elever toutes les autorisations sur les hevroth
		/*/
		Trysql tr = Trysql.getInstance();

		String s = "SELECT cid, editors FROM system_management.tbl_sicumin where editors regexp \"(.*);franck;(.*)\"";
		System.out.println(s + "\n\n");
		
		ResultSet r = tr.getStmt().executeQuery(s);

		while(r.next()){

			String maching = r.getString(2);
			System.out.println(maching);
				System.out.println("update system_management.tbl_sicumin "
						+ "\nset editors = '" + maching.replaceAll("franck", "") + "' \nwhere cid = " + r.getString(1) + " ;\n\n");
			Thread.sleep(10000);

		}

		//*/
		
		String s = "SELECT distinct symbol FROM franck.kiryatYam_101";
		Trysql tr = Trysql.getInstance();
		
		System.out.println(s + "\n\n");
		ResultSet r = tr.getStmt().executeQuery(s);

		while(r.next()){

			String maching = r.getString(1);
			System.out.println(maching);
			String a = "update franck.kiryatYam_101 "
					+ "\n set type = ';tlush_tashlum;' \n where symbol = " + r.getString(1) + " ;\n\n";
//				System.out.println(a);
				tr.getStmt().executeUpdate(a);
//				System.out.println("ca a ete execute \n\n");
//			Thread.sleep(10000);

		}
		
		tr.getStmt().execute("update franck.kiryatYam_101 set type = ';b_tlush;;tlush_shovy;' where symbolName regexp 'שןןי'");
		
		tr.getStmt().execute("update franck.kiryatYam_101 set type = ';b_tlush;' where symbol = 1000");

		
		System.out.println("finish");
		
		


	}

}
