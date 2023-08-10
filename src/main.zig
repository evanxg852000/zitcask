const std = @import("std");



pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    const LogFile = @import("./logfile.zig").LogFile;
    var logfile = try LogFile.open(std.fs.cwd(), 12, 1024);

    const key = "name";
    const value = "evance";
    const info = try logfile.writeItem(key, value);

    var buff = [_]u8{0}**32;
    _ = try logfile.readValue(info[1], &buff);

    try stdout.print("EVAN: {s}.\n", .{buff});
}


