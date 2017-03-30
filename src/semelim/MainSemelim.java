package semelim;

import java.sql.SQLException;

public class MainSemelim {

	static Semelim s = Semelim.getInstance();

	public static void maintoSemelim() throws SQLException{
		String name_schema = "franck";
		String name_table = "test_symbol";
		String name_table_symbol = "symbol";

//		s.create_table(name_schema, name_table);
//		s.create_table_symbol(name_schema, name_table_symbol);

//		s.load(name_schema, name_table);

		s.insert_symbol_in_table_symbol(name_schema, name_table_symbol, name_table);

	}

}