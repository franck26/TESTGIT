/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chelan;

import java.sql.SQLException;
import mySQL.Trysql;

/**
 *
 * @author user1
 */
public class Alfon {
    
    Trysql t = Trysql.getInstance();
    
    public void createTableAlphonChelan(String schema, String nameTable) throws SQLException{
        
        String sheilta = "CREATE TABLE if not exists " + schema + "." + nameTable + " (\n" +
"  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
"  `cid` int(10) NOT NULL,\n" +
"  `dyear` int(6) NOT NULL,\n" +
"  `id` int(11) NOT NULL,\n" +
"  `num_worker` int(11) NOT NULL,\n" +
"  `first_name` varchar(50) DEFAULT NULL,\n" +
"  `last_name` varchar(50) DEFAULT NULL,\n" +
"  `birthday` varchar(45) DEFAULT '-1',\n" +
"  `is_male` tinyint(1) NOT NULL DEFAULT '1',\n" +
"  PRIMARY KEY (`in_id`),\n" +
"  UNIQUE KEY `unique_1` (`cid`,`dyear`,`id`) USING BTREE,\n" +
"  KEY `Index_2` (`cid`,`dyear`,`id`)\n" +
") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='InnoDB free: 1508352 kB; InnoDB free: 2484224 kB; InnoDB fre';";
        
        t.Insertintodb(sheilta);
        
    }
    
    public void loadDataAlphonChelan(String schema, String nameTable, String path, int cid, int dyear) throws SQLException{
        String sheilta = "load data local infile '" + path + "'\n" +
"\n" +
"into table " + schema + "." + nameTable + "\n" +
"\n" +
"FIELDS TERMINATED BY ','  ENCLOSED BY  '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES\n" +
"\n" +
"(num_worker, last_name, first_name, id, birthday, is_male) \n" +
"\n" +
"set cid = " + cid + ", dyear = " + dyear + ";";
        
        t.Insertintodb(sheilta);
    }
    
    public void changeMin(String schema, String nameTable) throws SQLException{
        String sheilta = "UPDATE " + schema + "." + nameTable + " SET is_male = replace(is_male, 'זכר', 1), is_male = replace(is_male, 'נקבה', 0) ;";
        
        t.Insertintodb(sheilta);
    }
    
    public void changeBirthday(String schema, String nameTable) throws SQLException{
        String s = "UPDATE " + schema + "." + nameTable + " SET birtday = concat () ;";
        t.Insertintodb(s);
    }
    
    public void create101Details(String schema, String nameTable101) throws SQLException{
        String sheilta = "CREATE TABLE if not exists " + schema + "." + nameTable101 + " (\n" +
"  `in_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
"  `cid` int(10) NOT NULL,\n" +
"  `dyear` int(6) NOT NULL,\n" +
"  `id` int(11) NOT NULL,\n" +
"  `num_worker` int(11) NOT NULL,\n" +
"  `first_name` varchar(50) DEFAULT NULL,\n" +
"  `last_name` varchar(50) DEFAULT NULL,\n" +
"  `month` int(6) NOT NULL,\n" +
"  `run_version` int(11) DEFAULT '0',\n" +
"  `is_toshav` varchar(45) DEFAULT '1',\n" +
"  `aliya_date` varchar(45) DEFAULT '-1',\n" +
"  `marital_status` varchar(45) DEFAULT NULL,\n" +
"  `birthday` varchar(45) DEFAULT '-1',\n" +
"  `age_in_months` int(11) unsigned DEFAULT '0',\n" +
"  `is_mekabel_kitzba` tinyint(1) DEFAULT '0',\n" +
"  `is_baal_shlita` tinyint(1) NOT NULL DEFAULT '0',\n" +
"  `is_oved_zar` tinyint(1) NOT NULL DEFAULT '0',\n" +
"  `another_work` tinyint(1) DEFAULT '0',\n" +
"  `main_kitzva_type` varchar(20) DEFAULT NULL,\n" +
"  `intra_division` varchar(45) NOT NULL,\n" +
"  `intra_division_exp` varchar(45) NOT NULL,\n" +
"  `time_nechut` varchar(20) DEFAULT NULL,\n" +
"  `get_nechut_date` varchar(45) NOT NULL DEFAULT '-1',\n" +
"  `nechut_perecent` varchar(45) NOT NULL DEFAULT '-1',\n" +
"  `nechut_start` varchar(45) NOT NULL DEFAULT '-1',\n" +
"  `nechut_end` varchar(45) NOT NULL DEFAULT '-1',\n" +
"  `start_service_date` varchar(45) DEFAULT '-1',\n" +
"  `finished_service_date` varchar(45) DEFAULT '-1',\n" +
"  `is_male` tinyint(1) NOT NULL DEFAULT '1',\n" +
"  `city` varchar(100) DEFAULT NULL,\n" +
"  `street` varchar(200) DEFAULT NULL,\n" +
"  `street_num` varchar(45) DEFAULT NULL,\n" +
"  `zip_code` varchar(45) DEFAULT NULL,\n" +
"  `phone_munber` varchar(45) DEFAULT NULL,\n" +
"  `cell_phone` varchar(20) DEFAULT NULL,\n" +
"  `bank` varchar(45) DEFAULT NULL,\n" +
"  `branch` varchar(45) DEFAULT NULL,\n" +
"  `account_number` varchar(45) DEFAULT NULL,\n" +
"  `numOfChildren_fullPoints` int(3) unsigned DEFAULT '0',\n" +
"  `numOfChildren_halfPoints` int(3) unsigned DEFAULT '0',\n" +
"  `zikuy_points` double DEFAULT '0',\n" +
"  `numOfChildren` int(3) unsigned DEFAULT '0',\n" +
"  `typeOfIncomes_thisEmployer` varchar(100) DEFAULT NULL,\n" +
"  `typeOfIncomes_otherEmployer` varchar(100) DEFAULT '9',\n" +
"  `spouse_id` varchar(45) DEFAULT NULL,\n" +
"  `spouse_Lname` varchar(45) DEFAULT NULL,\n" +
"  `spouse_Fname` varchar(45) DEFAULT NULL,\n" +
"  `spouse_dob` varchar(45) DEFAULT NULL,\n" +
"  `spouse_aliya_date` varchar(45) DEFAULT NULL,\n" +
"  `spouse_income` varchar(100) DEFAULT NULL,\n" +
"  `degree_date` varchar(45) DEFAULT '-1',\n" +
"  `degree_kod` int(3) unsigned DEFAULT '0',\n" +
"  `start_pay` date DEFAULT NULL,\n" +
"  `finish_pay` date DEFAULT NULL,\n" +
"  `bank_precent` double DEFAULT '100',\n" +
"  `version` int(11) DEFAULT NULL,\n" +
"  `city_for_mh` varchar(100) NOT NULL DEFAULT '0',\n" +
"  `kvutzat_gil` varchar(100) DEFAULT 'מגיל 18 עד גיל פרישה',\n" +
"  `mas_shuly_percent` double NOT NULL DEFAULT '0',\n" +
"  `const_mas` double NOT NULL DEFAULT '0',\n" +
"  `prisha_date` varchar(45) DEFAULT '-1',\n" +
"  `notes` longtext,\n" +
"  `MSV_branch_num` varchar(3) NOT NULL DEFAULT '000',\n" +
"  `MSV_bank_num` varchar(45) NOT NULL DEFAULT '00',\n" +
"  `MSV_account_num` int(45) unsigned NOT NULL DEFAULT '0',\n" +
"  `more_extra_zikuy_points` double NOT NULL DEFAULT '0',\n" +
"  `start_working_date` varchar(45) DEFAULT '-1',\n" +
"  `is_101` tinyint(1) DEFAULT '0',\n" +
"  `files` varchar(100) DEFAULT NULL,\n" +
"  `job_precent` double DEFAULT '100',\n" +
"  `months2calc` int(11) DEFAULT NULL,\n" +
"  `is_year_mh_calc` tinyint(1) DEFAULT '1',\n" +
"  `start_month_forMh` int(10) NOT NULL DEFAULT '1',\n" +
"  `derog` int(10) unsigned DEFAULT '0',\n" +
"  `dargat_sachar` int(10) unsigned DEFAULT '0',\n" +
"  `vetek` double DEFAULT '0',\n" +
"  `tafkid` int(10) DEFAULT NULL,\n" +
"  `is_pensyoner` tinyint(4) DEFAULT '0',\n" +
"  `source` varchar(100) DEFAULT NULL,\n" +
"  `permission` int(11) unsigned DEFAULT NULL,\n" +
"  PRIMARY KEY (`in_id`),\n" +
"  UNIQUE KEY `unique_1` (`cid`,`dyear`,`month`,`id`,`run_version`) USING BTREE,\n" +
"  KEY `Index_2` (`cid`,`dyear`,`id`,`month`)\n" +
") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='InnoDB free: 1508352 kB; InnoDB free: 2484224 kB; InnoDB fre';";
        
        t.Insertintodb(sheilta);
    }
    
    public void insertIntoDetails(String schema, String nameTable, String nameTable101){
        String sheilta = "INSERT INTO " + schema + "." + nameTable101 + "\n" +
"(\n" +
"`cid`,\n" +
"`dyear`,\n" +
"`id`,\n" +
"`num_worker`,\n" +
"`first_name`,\n" +
"`last_name`,\n" +
"`birthday`,\n" +
"`is_male`)\n" +
"SELECT"
                + " `cid`,\n" +
"`dyear`,\n" +
"`id`,\n" +
"`num_worker`,\n" +
"`first_name`,\n" +
"`last_name`,\n" +
"`birthday`,\n" +
"`is_male`, \n" +
"from " + schema + "." + nameTable ; 
        
        
    }
    
}
