using System;
using System.Linq;using System.Threading.Tasks;
using PostgresLoggingTrigger;

await SqlHelper.DropSchemasAsync();
await SqlHelper.CreateSchemaAsync();
await SqlHelper.CreateSchemaAsync();
//await SqlHelper.SetupLoggingUtilsAsync();
await SqlHelper.SetupBatchLoggingUtilsAsync();
await SqlHelper.SetupTableChangeLoggingAsync();

// action_type = 1, single insert
var first = new Foo(1, 2, "first");
await SqlHelper.ForFoo.AddAsync(first, null);
await PrintTablesAsync("AFTER SINGLE INSERT");

// action_type = 1, batch insert
var batch = Enumerable
    .Range(2, 3)
    .Select(i => new Foo(2 * i, 2 * i + 1, $"batch_element_{i}"))
    .ToArray();
await SqlHelper.ForFoo.AddAsync(batch, "batch_user");
await PrintTablesAsync("AFTER BATCH INSERT");
await SqlHelper.TruncateTableAsync("trigger_test.foo_change_log");

// action_type = 3, batch insert
await SqlHelper.ForFoo.DeleteAsync(batch, "delete_batch_user");
await PrintTablesAsync("AFTER LOG TRUNCATE AND BATCH DELETE");



//// no change in log table
//await SqlHelper.ExecuteAsync(
//    "UPDATE trigger_test.foo SET int_value = int_value WHERE id = 1;", null);
//// action_type = 2
//await SqlHelper.ExecuteAsync(
//    "UPDATE trigger_test.foo SET int_value = 2 WHERE id = 1;", "first_user");
//// action_type = 2
//await SqlHelper.ExecuteAsync(
//    "UPDATE trigger_test.foo SET int_value = NULL WHERE id = 1;", "first_user");
//// action_type = 3
//await SqlHelper.ExecuteAsync(
//    "DELETE FROM trigger_test.foo WHERE id = 1;", "second_user");

static async Task PrintTablesAsync(string header)
{
    Console.WriteLine($"----------- {header}");
    await SqlHelper.PrintTableAsync("trigger_test.foo");
    await SqlHelper.PrintTableAsync("trigger_test.foo_change_log");
}

public readonly record struct Foo(int UniqueIntValue, int? IntValue, string? VarcharValue);