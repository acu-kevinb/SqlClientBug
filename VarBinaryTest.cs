using System.Data;
using Microsoft.Data.SqlClient;


public static class Program {
	const int Size = 20_000;

	static byte[] data = Enumerable.Range(0, Size)
		.Select(i => (byte)(i % 256))
		.ToArray();

	public static async Task Main(string[] args) {
		try {
			var connStr = args[0];
			using var conn = new SqlConnection(connStr);
			await conn.OpenAsync();
			var cmd = conn.CreateCommand();
			cmd.CommandText = """
				IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'TestVarBinary')
				BEGIN
				    CREATE TABLE dbo.TestVarBinary (Id INT IDENTITY(1,1) PRIMARY KEY, Data VARBINARY(MAX));
				END
				""";
			await cmd.ExecuteNonQueryAsync();

			cmd.CommandText = """
				IF NOT EXISTS (SELECT 1 FROM dbo.TestVarBinary)
					INSERT INTO dbo.TestVarBinary (Data) VALUES (@data);
				BEGIN
					UPDATE dbo.TestVarBinary SET Data = @data;
				END
				""";	
			cmd.Parameters.Add(new SqlParameter("@data", SqlDbType.VarBinary, Size) { Value = data });
			await cmd.ExecuteNonQueryAsync();

			cmd.CommandText = "SELECT Data FROM dbo.TestVarBinary";
			cmd.Parameters.Clear();
			var result = (byte[])await cmd.ExecuteScalarAsync();
			if (result.SequenceEqual(data)) {
				Console.WriteLine("Data matches.");
			} else {
				Console.Error.WriteLine("Data mismatch.");
			}
		} catch (Exception ex) {
			Console.Error.WriteLine(ex);
		}
	}
}

