using System.Data;
using Microsoft.Data.SqlClient;


public static class Program
{



    public static async Task Main(string[] args)
    {
        try
        {
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

            var startSize = 0;
            var currentSize = startSize;
            var maxSize = 100_000;
            var currentState = State.None;
            var currentStateStartedAtSize = startSize;

            while (currentSize <= maxSize)
            {
                byte[] data = Enumerable.Range(0, currentSize)
                    .Select(i => (byte)(i % 256))
                    .ToArray();

                cmd.CommandText = """
				IF NOT EXISTS (SELECT 1 FROM dbo.TestVarBinary)
					INSERT INTO dbo.TestVarBinary (Data) VALUES (@data);
				BEGIN
					UPDATE dbo.TestVarBinary SET Data = @data;
				END
				""";
                cmd.Parameters.Add(new SqlParameter("@data", SqlDbType.VarBinary, currentSize) { Value = data });
                await cmd.ExecuteNonQueryAsync();

                cmd.CommandText = "SELECT Data FROM dbo.TestVarBinary";
                cmd.Parameters.Clear();
                var result = (byte[])await cmd.ExecuteScalarAsync();
                if (result.Length != data.Length)
                {
                    if (currentState != State.LengthMismatch)
                    {
                        WriteStateChange(currentSize, currentState, currentStateStartedAtSize);
                        currentState = State.LengthMismatch;
                        currentStateStartedAtSize = currentSize;
                    }
                    currentSize++;
                }
                else if (result.SequenceEqual(data))
                {
                    if (currentState != State.Match)
                    {
                        WriteStateChange(currentSize, currentState, currentStateStartedAtSize);
                        currentState = State.Match;
                        currentStateStartedAtSize = currentSize;
                    }
                    currentSize++;
                }
                else
                {
                    if (currentState != State.ContentMismatch)
                    {
                        WriteStateChange(currentSize, currentState, currentStateStartedAtSize);
                        currentState = State.ContentMismatch;
                        currentStateStartedAtSize = currentSize;
                    }
                    currentSize++;
                }
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex);
        }
    }

    private static void WriteStateChange(int currentSize, State currentState, int currentStateStartedAtSize)
    {
        if (currentState != State.None)
        {
            Console.WriteLine($"{currentStateStartedAtSize} -> {currentSize - 1}\t({currentSize - currentStateStartedAtSize})\t{currentState.ToString()}");
        }
    }

    public enum State
    {
        None,
        Match,
        LengthMismatch,
        ContentMismatch
    }
}

