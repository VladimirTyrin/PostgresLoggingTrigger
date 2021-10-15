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

// action_type = 3, batch insert
await SqlHelper.TruncateTableAsync("trigger_test.foo_change_log");
await SqlHelper.ForFoo.DeleteAsync(batch, "delete_batch_user");
await PrintTablesAsync("AFTER LOG TRUNCATE AND BATCH DELETE");

// action_type = 2, update
var firstUpdated = first with { VarcharValue = "Updated varchar value" };
await SqlHelper.TruncateTableAsync("trigger_test.foo_change_log");
await SqlHelper.ForFoo.UpdateAsync(firstUpdated, "update_user");
await PrintTablesAsync("AFTER LOG TRUNCATE AND SINGLE UPDATE");

// no changes, update
await SqlHelper.TruncateTableAsync("trigger_test.foo_change_log");
await SqlHelper.ForFoo.UpdateAsync(firstUpdated, "update_user");
await PrintTablesAsync("AFTER LOG TRUNCATE AND SINGLE UPDATE WITH NO CHANGES");

// action_type = 1 + action_type = 2, INSERT ... ON CONFLICT UPDATE
await SqlHelper.TruncateTableAsync("trigger_test.foo_change_log");
firstUpdated = firstUpdated with { IntValue = 100 };
var second = new Foo(100, 500, "first");
await SqlHelper.ForFoo.AddAsync(new[] {firstUpdated, second}, "merge_user");
await PrintTablesAsync("AFTER LOG TRUNCATE AND MERGE");

// no changes, update
await SqlHelper.TruncateTableAsync("trigger_test.foo_change_log");
var copyBatch = Enumerable
    .Range(200, 3)
    .Select(i => new Foo(2 * i, 2 * i + 1, $"batch_element_{i}"))
    .ToArray();
await SqlHelper.ForFoo.CopyAsync(copyBatch, "copy_user");
await PrintTablesAsync("AFTER LOG TRUNCATE AND COPY");


static async Task PrintTablesAsync(string header)
{
    Console.WriteLine($"----------- {header}");
    await SqlHelper.PrintTableAsync("trigger_test.foo");
    await SqlHelper.PrintTableAsync("trigger_test.foo_change_log");
}

public readonly record struct Foo(int UniqueIntValue, int? IntValue, string? VarcharValue);