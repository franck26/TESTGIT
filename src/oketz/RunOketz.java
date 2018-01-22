/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oketz;

import java.io.File;
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



	public void mainOketz(String name_schema, String name_hevra, String name_table, String name_table_101, int cid, String path, int year1, int year2) throws SQLException, InterruptedException{

		System.out.println("oketz");
		
////		u.create_table_oketz(name_schema, name_table);
//		u.create_table_101_oketz(name_schema, name_table_101);
//		
//
//		for (int j = year1; j <= year2; j++) {
//            File f = new File(path + "/"+ j);
//            
//            File[] listefichier = f.listFiles();
//            
//            int i = 0;
//            
//            for (int a = 0; a < listefichier.length; a++){
//            	if (listefichier[a].getAbsolutePath().endsWith(".csv")) {
//            		i++;
//            	}
//            }
//            
//            System.out.println(j + " : " + i + " files .csv");
//                for (int k = 1; k <= i; k++) {  
//                    String a= path + "/"+ j + "/" + k + ".csv";      
//                    String[][] mat = u.csv2dar(a, 5000, 12);
//                    ArrayList<ArrayList<ArrayList<String>>> al = u.get_the_right_coulms(mat,5000);
//
//                    u.insert(al,name_schema,name_table_101,cid,j,k);
//            }
//        }

		u.bituahLeoumiMaavid(name_schema, name_hevra + "_BituahLeumiMaavid", year1, year2, cid, path, name_table_101);
		
		u. create_symbels_numbers_and_insert_into_main_table1(name_schema,"temp",name_table_101); 
		u.Malam_replace_comma(name_schema, name_table_101);
		
		u.modifyAgdaroth(path, name_schema, year1, year2, name_table_101);
		
		u.update_total(name_schema, name_table_101);
	}
	
	
	
	
	
	
	
	
	
	
}