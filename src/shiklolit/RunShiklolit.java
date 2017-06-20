package shiklolit;

import java.sql.SQLException;

public class RunShiklolit {
	private Shiklolit shiklolit;

	public RunShiklolit() {
		shiklolit = Shiklolit.getInstance();
	}


	public void mainTamhirPerMonth(String name_schema, String name_hevra, String pathFile, String name_table_sofi_101, int year1, int year2, int cid) throws SQLException{
		String	
		name_table_tamhir 		= name_hevra  + "_tamhir",
		name_table_tamhir_101	= name_table_tamhir + "_101",
		name_table_tashlum 			= name_hevra  + "_tashlum",
		name_table_tashlum_101 		= name_table_tashlum + "_101",
		name_table_alfon			= name_hevra + "_alfon", 
		name_table_alfon_101		= name_table_alfon + "_101";

		////
		shiklolit.create_table_Shiklolit_tamchir(name_schema, name_table_tamhir);
		shiklolit.create_table_Shiklolit_tashlumim(name_schema, name_table_tashlum);
		//		
		//
		shiklolit.create_101_shiklolit(name_schema, name_table_tamhir_101);
		shiklolit.create_101_shiklolit(name_schema, name_table_tashlum_101);

		shiklolit.createTableAlfon(name_schema, name_table_alfon);
		shiklolit.createTableAlfon101(name_schema, name_table_alfon_101);
		//		
		shiklolit.create_101_shiklolit(name_schema, name_table_sofi_101);
		//		
		//		tamhir
		shiklolit.Shiklolit_load_tamhir_per_month(pathFile, name_schema, name_table_tamhir, year1, year2);
		shiklolit.Shiklolit_insert_into_101_tamchir_per_month(name_schema, name_table_tamhir, name_table_tamhir_101, cid);


		//		tashlum + alfon
		for (int i = year1; i <= year2; i++) {

			shiklolit.Shiklolit_load_data_tashlumim(pathFile, name_schema, name_table_tashlum, "", i);
			shiklolit.Malam_replace_comma(name_schema, name_table_tashlum);

			shiklolit.loadDetails(pathFile, name_schema, name_table_alfon, "", i, cid);


		}
//				shiklolit.shiklolit_get_the_right_rows_false(name_schema, name_table_tashlum, name_table_tashlum_101 , cid);


		shiklolit.shiklolit_get_the_right_rows1(name_schema, name_table_tashlum, name_table_tashlum_101, cid);


		shiklolit.create_symbels_numbers_and_insert_into_main_table(name_schema, "temp", name_table_tashlum_101);
		shiklolit.insert_to_101_sofi(name_schema, name_table_tashlum_101, name_table_tamhir_101, name_table_sofi_101);
		shiklolit.update_total(name_schema, name_table_sofi_101);

		shiklolit.changeMin(name_schema, name_table_alfon);
		
		shiklolit.insert101Details(name_schema, name_table_alfon, name_table_alfon_101);

		System.out.println("fini");
	}
}
