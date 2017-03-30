/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oketz;

import java.sql.SQLException;
import java.util.ArrayList;
/**
 *
 * @author user1
 */
public class RunOketz {
	private Oketz u;

	public RunOketz(){
		u = Oketz.getInstance();
	}



	public void mainOketz() throws SQLException{

		System.out.println("oketz");
		String  name_schema 		=   "franck",
                name_hevra			=	"raga",
                name_table 	 		=   "tbl_" + name_hevra,
                name_table_101     	=   "tbl_" + name_hevra.toUpperCase() + "_101";

		int cid = 930946090;
		
		u.create_table_oketz(name_schema, name_table);
		u.create_table_101_oketz(name_schema, name_table_101);
		

		for (int j = 2015; j <= 2017; j++) {
            
            if (j <= 2016){
                for (int k = 1; k < 13; k++) {         
                    String a="/home/user1/hevra/oketz/RGA/sahar/"+ j + "/" + k + ".csv";      
                    String[][] mat = u.csv2dar(a, 5000, 8);
                    ArrayList<ArrayList<ArrayList<String>>> al = u.get_the_right_coulms(mat,5000);

                    u.insert(al,name_schema,name_table,cid,j,k);
                }
            }
            else{
              String a="/home/user1/hevra/oketz/RGA/sahar/"+ j + "/1.csv";      
              String[][] mat = u.csv2dar(a, 5000, 8);
              ArrayList<ArrayList<ArrayList<String>>> al = u.get_the_right_coulms(mat,5000);

              u.insert(al,name_schema,name_table,cid,j,1);
            }
        }

		u. create_symbels_numbers_and_insert_into_main_table1(name_schema,"temp",name_table_101); 
		u.Malam_replace_comma(name_schema, name_table_101);
		u.update_total(name_schema, name_table_101);
	}
}