package xlsxToCsv;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 *
 * @author Munawwar
 */
public class XlsxToCsv {
	
	static String dossier = "/home/user1/Downloads";


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

				// For each row, iterate through each columns
				Iterator<Cell> cellIterator = row.cellIterator();
				while (cellIterator.hasNext()) {

					cell = cellIterator.next();
//					data.append("\"" + cell.getNumericCellValue() + "\",");

					switch (cell.getCellType()) {
					case Cell.CELL_TYPE_BOOLEAN:
						data.append(cell.getBooleanCellValue() + ",");

						break;
					case Cell.CELL_TYPE_NUMERIC:
						data.append("" + cell.getNumericCellValue() + ",");

						break;
					case Cell.CELL_TYPE_STRING:
						data.append("" + cell.getStringCellValue() + ",");
						break;

					case Cell.CELL_TYPE_BLANK:
						data.append("" + ",");
						break;
					default:
						data.append(cell + ",");

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
		convertToCsv(dossier);
	}
	
	public static void convertToCsv(String path){
		File repertoire = new File(path);

		if(repertoire.exists()){

			File[] listefichiers;


			int i;
			listefichiers=repertoire.listFiles();

			int lengh = listefichiers.length;
			
			do{
				
				
				lengh--;
				
			}while(lengh != 0);
			
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

						System.out.println(listefichiers[i]);

						Scanner sc = new Scanner(System.in);

						System.out.println("comment renommer le fichier (sans le .csv) : ");

						String str = sc.nextLine();

						inputFile.renameTo(new File(inputFile.getPath()+File.separator+str));

						// writing excel data to csv
						//					File outputFile = new File(path + "/" + listefichiers[i].substring(0,listefichiers[i].length()-5) + ".csv");
						File outputFile = new File(path + "/" + str + ".csv");
						convert(inputFile, outputFile);
					}
				}
			}

		}else
			System.out.println("le fichier n'existe pas !!!");

		System.out.println("finish"); 
	}
}
