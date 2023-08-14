const std = @import("std");
const utils = @import("./utils.zig");

const Allocator = std.mem.Allocator;
const setFileSize = utils.setFileSize;
const toInt = utils.toInt;
const Dir = std.fs.Dir;
const File = std.fs.File;

pub const Item = struct {
    key: []const u8,
    value: []const u8,
    offset: usize,
};

pub const WrittenItem = struct {
    itemOffset: usize,
    valueOffset: usize,
};

pub const ReadItem = struct {
    itemOffset: usize,
    valueOffset: usize,
    key: []u8,
    value: []u8,

    pub fn size(self: *const @This()) usize {
        return 8 + self.key.len + self.value.len;
    }
};

pub const LogFile = struct {
    const Self = @This();

    id: u32,
    fileName: []u8,
    handle: File,
    maxSize: usize,
    writePosition: usize,
    allocator: Allocator,

    pub fn openOrCreate(allocator: Allocator, dir: Dir, id: u32, maxSize: usize) !Self {
        var fileName = try std.fmt.allocPrint(allocator, "{d:0>16}", .{id});
        errdefer allocator.free(fileName);

        var fileHandle: File = undefined;
        fileHandle = dir.openFile(fileName, .{ .mode = .read_write }) catch blk: {
            fileHandle = try dir.createFile(fileName, .{ .read = true });
            //TODO: add file pre-allocation feature later.
            // try setFileSize(&fileHandle, maxSize, 4096);
            break :blk fileHandle;
        };

        return Self{
            .id = id,
            .fileName = fileName,
            .handle = fileHandle,
            .maxSize = maxSize,
            .writePosition = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.fileName);
    }

    fn close(self: Self) void {
        self.handle.close();
    }

    pub fn setWritePosition(self: *Self, position: usize) void {
        self.writePosition = position;
    }

    pub fn isFull(self: *const Self) bool {
        return self.writePosition >= self.maxSize;
    }

    // key_size,value_size, key, val
    pub fn writeItem(self: *Self, key: []const u8, value: []const u8) !WrittenItem {
        var pos = self.writePosition;

        try self.handle.seekTo(pos);
        const writer = self.handle.writer();
        try writer.writeIntLittle(u32, @as(u32, @intCast(key.len)));
        try writer.writeIntLittle(u32, @as(u32, @intCast(value.len)));
        _ = try writer.write(key);
        _ = try writer.write(value);
        try self.sync();

        self.writePosition += 8 + key.len + value.len;
        return WrittenItem{
            .itemOffset = pos,
            .valueOffset = pos + 8 + key.len,
        };
    }

    pub fn iterator(self: *Self) LogFileIterator {
        return LogFileIterator.init(self);
    }

    fn readItemAlloc(self: *Self, itemOffset: usize) !ReadItem {
        try self.handle.seekTo(itemOffset);
        const reader = self.handle.reader();
        const keySize = try reader.readIntLittle(u32);
        const valueSize = try reader.readIntLittle(u32);

        var keyBuffer = try self.allocator.alloc(u8, @as(usize, keySize));
        _ = try reader.readAll(keyBuffer);

        var valueBuffer = try self.allocator.alloc(u8, @as(usize, valueSize));
        _ = try reader.readAll(valueBuffer);

        return ReadItem{
            .itemOffset = itemOffset,
            .valueOffset = itemOffset + 8 + keyBuffer.len,
            .key = keyBuffer,
            .value = valueBuffer,
        };
    }

    pub fn freeReadItem(self: *Self, readItem: ReadItem) void {
        self.allocator.free(readItem.key);
        self.allocator.free(readItem.value);
    }

    pub fn readValueAlloc(self: *Self, valueOffset: usize, valueSize: usize) ![]u8 {
        try self.handle.seekTo(valueOffset);
        const reader = self.handle.reader();
        var buffer = try self.allocator.alloc(u8, valueSize);
        _ = try reader.readAll(buffer);
        return buffer;
    }

    pub fn freeValue(self: *Self, value: []u8) void {
        self.allocator.free(value);
    }

    fn sync(self: *Self) !void {
        return self.handle.sync();
    }
};

pub const LogFileIterator = struct {
    const Self = @This();

    offset: usize,
    eof: bool,
    logFile: *LogFile,

    pub fn init(logFile: *LogFile) Self {
        return Self{
            .offset = 0,
            .eof = false,
            .logFile = logFile,
        };
    }

    pub fn next(self: *Self) ?ReadItem {
        if (self.eof) {
            return null;
        }

        const readItem = self.logFile.readItemAlloc(self.offset) catch blk: {
            self.eof = true;
            break :blk null;
        };

        if (readItem) |payload| {
            self.offset += payload.size();
        }

        return readItem;
    }

    pub fn getOffset(self: *Self) usize {
        return self.offset;
    }
};

test "Log File" {
    const expect = std.testing.expect;
    const allocator = std.testing.allocator;

    var logFile = try LogFile.openOrCreate(allocator, std.fs.cwd(), 12, 0);
    defer logFile.deinit();

    const key = "name";
    const value = "jhon";
    const writtenItem = try logFile.writeItem(key, value);
    try expect(writtenItem.itemOffset == 0);
    try expect(writtenItem.valueOffset == 12);

    const fetchedValue = try logFile.readValueAlloc(writtenItem.valueOffset, value.len);
    defer logFile.freeValue(fetchedValue);

    try expect(std.mem.eql(u8, fetchedValue, value));

    const readItem = try logFile.readItemAlloc(0);
    defer logFile.freeReadItem(readItem);

    try expect(std.mem.eql(u8, readItem.key, key));
    try expect(std.mem.eql(u8, readItem.value, value));
    try expect(readItem.itemOffset == 0);
    try expect(readItem.valueOffset == 12);
}

test "Log File iterator" {
    const expect = std.testing.expect;
    const allocator = std.testing.allocator;

    var logFile = try LogFile.openOrCreate(allocator, std.fs.cwd(), 11, 0);
    defer logFile.deinit();

    const data = [_][]const u8{ "foo", "bar", "baz", "biz" };
    for (data) |item| {
        _ = try logFile.writeItem(item, item);
    }

    var iter = logFile.iterator();
    var index: usize = 0;
    while (iter.next()) |readItem| : (index += 1) {
        try expect(std.mem.eql(u8, readItem.key, data[index]));
        try expect(std.mem.eql(u8, readItem.value, data[index]));
        logFile.freeReadItem(readItem);
    }
}
