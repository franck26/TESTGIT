package main;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import mySQL.Trysql;

public class Main3 {

	public static void main(String[] args) throws SQLException {
		Trysql t = Trysql.getInstance();

                int id = 0; String name = "";
		
                ResultSet r = t.gettabledb("SELECT * FROM franck.sameleth_gemel;");
                
                while(r.next()){
                    if(r.getInt("num_worker") != 0){
                        id = r.getInt("num_worker");
                        name = r.getString("fullname");
                    } else{
                        Statement s = t.getConnectionMySql().getConn().createStatement();
                        String sheilta = "update franck.sameleth_gemel set fullname = \"" + name +"\", num_worker = " + id + " where in_id = " + r.getInt("in_id") + " ; " ;
                        
                        System.out.println(sheilta);
                        s.execute(sheilta);
                        s.close();
                    }
                }
                
                System.out.println("fini");
	}

}
