const std = @import("std");

const Allocator = std.mem.Allocator;
const File = std.fs.File;
const Dir = std.fs.Dir;
const ArrayList = std.ArrayList;
const parseInt = std.fmt.parseInt;
const stem = std.fs.path.stem;



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

pub fn ownedFileIdsFromDir(allocator: Allocator, path: Dir) ![]u32 {
    var fileIds = ArrayList(u32).init();
    const dirIter = path.openIterableDir(".", .{});
    defer dirIter.close();
    while (try dirIter.next()) |path| {
        if (path.kind == .File) {
            const fileId try parseInt(u32, stem(path.name), 10);  
            try fileIds.append(id);
        }
    }
    return fileIds.toOwnedSlice();
}
