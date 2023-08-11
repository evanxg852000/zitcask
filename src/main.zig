const std = @import("std");
const fs = std.fs;



pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    //var dir = try fs.cwd().makeOpenPath("foo/test", .{});
    //var dir = try fs.cwd().makeDir("foot/test");
    //var dir = try fs.openDirAbsolute("./foo/test", .{});
    //_ = try dir.openIterableDir(".", .{});

    const dir = try std.fmt.parseInt(u32, std.fs.path.stem("00012.bin"), 10);  


    try stdout.print("EVAN: {}.\n", .{dir});

    // const LogFile = @import("./logfile.zig").LogFile;
    // var logfile = try LogFile.open(std.fs.cwd(), 12, 1024);

    // const key = "name";
    // const value = "evance";
    // const info = try logfile.writeItem(key, value);

    // var buff = [_]u8{0}**32;
    // _ = try logfile.readValue(info[1], &buff);

    
}


