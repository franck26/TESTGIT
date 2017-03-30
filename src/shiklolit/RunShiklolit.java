package shiklolit;

import java.sql.SQLException;

public class RunShiklolit {
	private Shiklolit shiklolit;

	public RunShiklolit() {
		shiklolit = Shiklolit.getInstance();
	}


	public void mainTamhirPerMonth() throws SQLException{
		String	name_schema		= "franck",
				name_table 		= "mishkan_tamhir",
				name_table_101	= "Mishkan_tamhir_101",
				pathFile		= "/home/user1/hevra/shiklolit/michkan";
////
//		shiklolit.create_table_Shiklolit_tamchir(name_schema, name_table);
//		shiklolit.create_101_shiklolit(name_schema, name_table_101);
//		shiklolit.Shiklolit_load_tamhir_per_month(pathFile, name_schema, name_table, 2009, 2010);
//		shiklolit.Shiklolit_insert_into_101_tamchir_per_month(name_schema, name_table, name_table_101, 952352839);
//		
		mainTashlumim(name_table, name_table_101);
	}

	public void mainTashlumim(String name_table_tamhir, String name_table_tamhir_101) throws SQLException{

		String	name_schema					= "franck",
				name_table_tashlum 			= "mishkan22222_tashlum",
				name_table_tashlum_101 		= name_table_tashlum + "_101",
				name_table_sofi_101			= "mishkan22222_101",
				pathFile					= "/home/user1/hevra/shiklolit/michkan";

		shiklolit.create_table_Shiklolit_tashlumim(name_schema, name_table_tashlum);

		shiklolit.create_101_shiklolit(name_schema, name_table_sofi_101);

		for (int i = 2009; i <= 2010; i++) {
			String s = "/" + i + "/" + i;

			shiklolit.Shiklolit_load_data_tashlumim(pathFile + s, name_schema, name_table_tashlum, "charset hebrew", i);
			shiklolit.Malam_replace_comma(name_schema, name_table_tashlum);
			shiklolit.shiklolit_get_the_right_rows(name_schema, name_table_tashlum, name_table_tashlum_101 , i, 952352839);
		}
		
		shiklolit.create_symbels_numbers_and_insert_into_main_table(name_schema, "temp", name_table_tashlum_101);
		
		shiklolit.insert_to_101_sofi(name_schema, name_table_tashlum_101, name_table_tamhir_101, name_table_sofi_101);
		
		shiklolit.update_total(name_schema, name_table_sofi_101);
		System.out.println("fini");
	}
}
