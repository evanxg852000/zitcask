const std = @import("std");
const fs = std.fs;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const bitcask = @import("./bitcask.zig");
    const Storage = bitcask.Storage;
    const Params = bitcask.Params;
    const dbDir = try std.fs.cwd().makeOpenPath("testdb", .{});
    var store = try Storage.open(allocator, dbDir, Params.small);
    defer store.deinit();

    const key = "name";
    const value = "Mary James";

    try store.put(key, value);

    const v = (try store.get(key)).?;
    defer store.free(v);

    try stdout.print("KV-> `{s}`:`{s}` \n", .{ key, v });
}
