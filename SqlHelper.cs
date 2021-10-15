using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PostgreSQLCopyHelper;

namespace PostgresLoggingTrigger;

internal static class SqlHelper
{
    private const string ConnectionString = "Server=localhost; Port=5432; Database=testdb; No Reset On Close=True; Username=postgres; Password=12345; Application Name=PostgresLoggingTrigger; SearchPath=trigger_test";

    public static async Task DropSchemasAsync()
    {
        await DropSchemaAsync("trigger_test");
        await DropSchemaAsync("logging");
    }

    public static Task CreateSchemaAsync() => ApplyFileAsync("SetupSchema.sql");

    public static Task SetupLoggingUtilsAsync() => ApplyFileAsync("SetupLoggingUtils.sql");

    public static Task SetupBatchLoggingUtilsAsync() => ApplyFileAsync("SetupBatchLoggingUtils.sql");

    public static async Task SetupTableChangeLoggingAsync()
    {
        await using var connection = await OpenConnectionAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT logging.enable_table_changes_logging('trigger_test', 'foo')";
        await command.ExecuteNonQueryAsync();
    }

    public static async Task PrintTableAsync(string table)
    {
        await using var connection = await OpenConnectionAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM {table} ORDER BY id";
        await using var reader = await command.ExecuteReaderAsync();
        var fieldCount = reader.FieldCount;
        const char separator = '\t';
        // ReSharper disable AccessToDisposedClosure
        Console.WriteLine(table);
        Console.WriteLine(string.Join(separator, Enumerable.Range(0, fieldCount).Select(i => reader.GetName(i))));
        while (await reader.ReadAsync())
        {
            Console.WriteLine(string.Join(separator, Enumerable.Range(0, fieldCount).Select(i => FormatValue(reader.GetValue(i)))));
        }
        // ReSharper restore AccessToDisposedClosure
    }

    public static async Task TruncateTableAsync(string table)
    {
        await using var connection = await OpenConnectionAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = $"TRUNCATE TABLE {table}";
        await command.ExecuteNonQueryAsync();
    }

    private static async Task SetApplicationUserAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string? userName)
    {
        if (userName is null)
        {
            return;
        }
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT logging.set_application_user(@application_user)";
        command.Parameters.AddWithValue("@application_user", userName);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task DropSchemaAsync(string schema)
    {
        await using var connection = await OpenConnectionAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = $"DROP SCHEMA IF EXISTS {schema} CASCADE";
        await command.ExecuteNonQueryAsync();
    }

    private static async Task ApplyFileAsync(string fileName)
    {
        var commandText = await File.ReadAllTextAsync(fileName);

        await using var connection = await OpenConnectionAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = commandText;
        await command.ExecuteNonQueryAsync();
    }

    private static string FormatValue(object? value) => value is null or DBNull ? "<NULL>" : value.ToString()!;

    private static async Task<NpgsqlConnection> OpenConnectionAsync()
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    public static class ForFoo
    {
        public static Task AddAsync(Foo foo, string? userName) => AddAsync(new[] { foo }, userName);

        public static async Task AddAsync(IReadOnlyList<Foo> foos, string? userName)
        {
            var commandBuilder = new StringBuilder(@"
INSERT INTO trigger_test.foo(added_at, unique_int_value, int_value, varchar_value) VALUES");

            commandBuilder.Append(string.Join(",",
                foos.Select((f, i) =>
                    $"(transaction_timestamp(), @unique_int_value{i}, @int_value{i}, @varchar_value{i})")));

            commandBuilder.Append(
                " ON CONFLICT(unique_int_value) DO UPDATE SET int_value = EXCLUDED.int_value, varchar_value=EXCLUDED.varchar_value");

            await using var connection = await OpenConnectionAsync();
            await using var transaction = await connection.BeginTransactionAsync();

            await SetApplicationUserAsync(connection, transaction, userName);

            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandBuilder.ToString();

            for (var i = 0; i < foos.Count; i++)
            {
                var foo = foos[i];
                command.Parameters.AddWithValue($"@unique_int_value{i}", foo.UniqueIntValue);
                command.Parameters.AddWithValue($"@int_value{i}",
                    foo.IntValue.HasValue ? foo.IntValue.Value : DBNull.Value);
                command.Parameters.AddWithValue($"@varchar_value{i}",
                    foo.VarcharValue != null ? foo.VarcharValue : DBNull.Value);
            }

            await command.ExecuteNonQueryAsync();

            await transaction.CommitAsync();
        }

        public static Task DeleteAsync(Foo foo, string? userName) => DeleteAsync(new[] { foo }, userName);

        public static async Task DeleteAsync(IReadOnlyList<Foo> foos, string? userName)
        {
            await using var connection = await OpenConnectionAsync();
            await using var transaction = await connection.BeginTransactionAsync();

            await SetApplicationUserAsync(connection, transaction, userName);

            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "DELETE FROM trigger_test.foo WHERE unique_int_value = ANY(:unique_int_values);";

            command.Parameters.AddWithValue("unique_int_values", foos.Select(f => f.UniqueIntValue).ToArray())
                .NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer;

            await command.ExecuteNonQueryAsync();

            await transaction.CommitAsync();
        }

        public static Task UpdateAsync(Foo foo, string? userName) => UpdateAsync(new[] { foo }, userName);

        // just an example of bulk update, not necessarily efficient
        public static async Task UpdateAsync(IReadOnlyList<Foo> foos, string? userName)
        {
            var commandBuilder = new StringBuilder(@"
UPDATE trigger_test.foo f SET unique_int_value = tmp.unique_int_value, int_value = tmp.int_value, varchar_value = tmp.varchar_value FROM (SELECT ");

            commandBuilder.Append(string.Join(" UNION ",
                foos.Select((f, i) =>
                    $"@unique_int_value{i} AS unique_int_value, @int_value{i} AS int_value, @varchar_value{i} AS varchar_value")));

            commandBuilder.Append(") tmp WHERE f.unique_int_value = f.unique_int_value");

            await using var connection = await OpenConnectionAsync();
            await using var transaction = await connection.BeginTransactionAsync();

            await SetApplicationUserAsync(connection, transaction, userName);

            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandBuilder.ToString();

            for (var i = 0; i < foos.Count; i++)
            {
                var foo = foos[i];
                command.Parameters.AddWithValue($"@unique_int_value{i}", foo.UniqueIntValue);
                command.Parameters.AddWithValue($"@int_value{i}",
                    foo.IntValue.HasValue ? foo.IntValue.Value : DBNull.Value);
                command.Parameters.AddWithValue($"@varchar_value{i}",
                    foo.VarcharValue != null ? foo.VarcharValue : DBNull.Value);
            }

            command.Parameters.AddWithValue("unique_int_values", foos.Select(f => f.UniqueIntValue).ToArray())
                .NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer;

            await command.ExecuteNonQueryAsync();

            await transaction.CommitAsync();
        }

        public static async Task CopyAsync(IReadOnlyList<Foo> foos, string? userName)
        {
            var now = DateTime.Now;
            var copyHelper = new PostgreSQLCopyHelper.PostgreSQLCopyHelper<Foo>("trigger_test", "foo")
                .MapTimeStamp("added_at", _ => now)
                .MapInteger("unique_int_value", x => x.UniqueIntValue)
                .MapNullable("int_value", x => x.IntValue, NpgsqlDbType.Integer)
                .MapVarchar("varchar_value", x => x.VarcharValue);

            await using var connection = await OpenConnectionAsync();
            await using var transaction = await connection.BeginTransactionAsync();

            await SetApplicationUserAsync(connection, transaction, userName);

            await copyHelper.SaveAllAsync(connection, foos);

            await transaction.CommitAsync();
        }
    }
}