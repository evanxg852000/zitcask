const std = @import("std");

const File = std.fs.File;

// Pre-allocate file size.
// TODO: find a better way
pub fn setFileSize(file: *File, desiredSize: usize, comptime bufferSize: usize) !void {
    try file.seekTo(0);
    var written: usize = 0;
    var buffer = [_]u8{0} ** bufferSize;
    while(written < desiredSize) {
        const restSize: usize = if (desiredSize - written > bufferSize) bufferSize else desiredSize - written;
        _ = try file.write(buffer[0..restSize]);
        written += restSize;
    }
    try file.sync();
}

// @intCast cannot be easily used in certain situation because 
// it cannot infer the type.
pub fn toInt(comptime T: type, v: anytype) T {
    return @intCast(v);
}
