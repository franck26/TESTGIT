package xlsxToCsv;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.Scanner;

import javax.swing.JOptionPane;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 *
 * @author Sayada Franck
 */

public class XlsxToCsv {

	static String path = "/home/user1/hevra/micpal/lotan";


	static void convert(File inputFile, File outputFile) {

		try {
			FileOutputStream fos = new FileOutputStream(outputFile);
			// Get the workbook object for XLSX file
			XSSFWorkbook wBook = new XSSFWorkbook(
					new FileInputStream(inputFile));
			// Get first sheet from the workbook
			XSSFSheet sheet = wBook.getSheetAt(0);
			Row row;
			Cell cell;
			// Iterate through each rows from first sheet
			Iterator<Row> rowIterator = sheet.iterator();

			StringBuffer data = new StringBuffer();
			while (rowIterator.hasNext()) {
				row = rowIterator.next();

				for(int i = 0; i <= row.getLastCellNum(); i++)
					if(row.getCell(i) == null){
						data.append(",");
					}
					else{
						cell = row.getCell(i);
						switch (cell.getCellType()) {
						case Cell.CELL_TYPE_BOOLEAN:
							data.append("\"" + cell.getBooleanCellValue() + "\",");

							break;
						case Cell.CELL_TYPE_NUMERIC:
							java.text.DecimalFormat df = new java.text.DecimalFormat("0.##");
							data.append("\"" + df.format(cell.getNumericCellValue()) + "\",");

							break;
						case Cell.CELL_TYPE_STRING:
							data.append("" + cell.getStringCellValue().replace('\'', ' ').replace('"', ' ') + ",");
							break;

						case Cell.CELL_TYPE_BLANK:
							data.append("\"\",");
							break;
						default:
							data.append("\"" + cell.getStringCellValue().replace('\'', ' ') + "\",");

						}


					}
				data.append("\n");
			}

			fos.write(data.toString().getBytes());
			fos.close();

		} catch (Exception ioe) {
			ioe.printStackTrace();
		}
		System.out.println("The conversion of " + outputFile + " is ok"
				//				+ "" + inputFile.delete() + " "
				+ "!\n");
	}
	

	// testing the application

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		convertToCsv(path);
	}

	public static void convertToCsv(String path){
		File repertoire = new File(path);

		if(repertoire.exists()){
			
			
			File[] listefichiers;

			int i;
			listefichiers=repertoire.listFiles();

			for (int j = 0; j < listefichiers.length; j++) {
				System.out.println(listefichiers[j]);
			}
			System.out.println();

			for(i=0;i<listefichiers.length;i++){

				if(listefichiers[i].isDirectory()){
					convertToCsv(listefichiers[i].getAbsolutePath());

				}else if(listefichiers[i].isFile()){

					if(listefichiers[i].getAbsolutePath().endsWith(".xlsx")==true){

						// reading file from desktop
						File inputFile = new File(listefichiers[i].getAbsolutePath());

						//*/
						String [] str = listefichiers[i].getAbsolutePath().split("/");

						String a = "";
						a = (String) JOptionPane.showInputDialog("שם חדש של הקובץ : \"" + str[str.length - 1] + "\" (בלי .CSV)");

						boolean exist = false;
						//*/
					
						File outputFile = new File(path + "/" + a + ".csv");
						convert(inputFile, outputFile);
					}
				}
			}

		}else
			System.out.println("le fichier n'existe pas !!!");

		System.out.println("finish  !!!"); 
	}
}
