using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;

const string connectionString =
    "Server=localhost; Port=5432; Database=testdb; No Reset On Close=True; Username=postgres; Password=12345; Application Name=PostgresLoggingTrigger";

await DropSchemasAsync();
await CreateSchemaAsync();
await SetupLoggingUtilsAsync();
await SetupTableChangeLoggingAsync();

// action_type = 1
await ExecuteAsync(
    "INSERT INTO trigger_test.first_table(added_at, int_value, varchar_value) VALUES (transaction_timestamp(), 1, 'first');", null);
// no change in log table
await ExecuteAsync(
    "UPDATE trigger_test.first_table SET int_value = int_value WHERE id = 1;", null);
// action_type = 2
await ExecuteAsync(
    "UPDATE trigger_test.first_table SET int_value = 2 WHERE id = 1;", "first_user");
// action_type = 2
await ExecuteAsync(
    "UPDATE trigger_test.first_table SET int_value = NULL WHERE id = 1;", "first_user");
// action_type = 3
await ExecuteAsync(
    "DELETE FROM trigger_test.first_table WHERE id = 1;", "second_user");
await PrintTableAsync("trigger_test.first_table");
await PrintTableAsync("trigger_test.first_table_change_log");

static async Task DropSchemasAsync()
{
    await DropSchemaAsync("trigger_test");
    await DropSchemaAsync("logging");
}

static async Task DropSchemaAsync(string schema)
{
    await using var connection = await OpenConnectionAsync();

    await using var command = connection.CreateCommand();
    command.CommandText = $"DROP SCHEMA IF EXISTS {schema} CASCADE";
    await command.ExecuteNonQueryAsync();
}

static Task CreateSchemaAsync() => ApplyFileAsync("SetupSchema.sql");

static Task SetupLoggingUtilsAsync() => ApplyFileAsync("SetupLoggingUtils.sql");

static async Task ApplyFileAsync(string fileName)
{
    var commandText = await File.ReadAllTextAsync(fileName);

    await using var connection = await OpenConnectionAsync();

    await using var command = connection.CreateCommand();
    command.CommandText = commandText;
    await command.ExecuteNonQueryAsync();
}

static async Task SetupTableChangeLoggingAsync()
{
    await using var connection = await OpenConnectionAsync();

    await using var command = connection.CreateCommand();
    command.CommandText = "SELECT logging.enable_table_changes_logging('trigger_test', 'first_table')";
    await command.ExecuteNonQueryAsync();
}

static async Task ExecuteAsync(string commandText, string? userName)
{
    await using var connection = await OpenConnectionAsync();
    await using var transaction = await connection.BeginTransactionAsync();

    await SetApplicationUserAsync(connection, transaction, userName);

    await using var command = connection.CreateCommand();
    command.Transaction = transaction;
    command.CommandText = commandText;
    await command.ExecuteNonQueryAsync();

    await transaction.CommitAsync();
}

static async Task SetApplicationUserAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string? userName)
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

static async Task PrintTableAsync(string table)
{
    await using var connection = await OpenConnectionAsync();

    await using var command = connection.CreateCommand();
    command.CommandText = $"SELECT * FROM {table} ORDER BY id";
    await using var reader = await command.ExecuteReaderAsync();
    var fieldCount = reader.FieldCount;
    const char separator = '\t';
    // ReSharper disable AccessToDisposedClosure
    Console.WriteLine("----------- " + table);
    Console.WriteLine(string.Join(separator, Enumerable.Range(0, fieldCount).Select(i => reader.GetName(i))));
    while (await reader.ReadAsync())
    {
        Console.WriteLine(string.Join(separator, Enumerable.Range(0, fieldCount).Select(i => FormatValue(reader.GetValue(i)))));
    }
    // ReSharper restore AccessToDisposedClosure
}

static string FormatValue(object? value) => value?.ToString() ?? "<NULL>";

static async Task<NpgsqlConnection> OpenConnectionAsync()
{
    var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync();
    return connection;
}