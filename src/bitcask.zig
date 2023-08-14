const std = @import("std");
const concurrentmap = @import("./concurrentmap.zig");
const logfile = @import("./logfile.zig");
const utils = @import("./utils.zig");

const Allocator = std.mem.Allocator;
const Dir = std.fs.Dir;
const RwLock = std.Thread.RwLock;
const AutoHashMap = std.AutoHashMap;
const ConcurrentMap = concurrentmap.ConcurrentMap;
const LogFile = logfile.LogFile;
const ReadItem = logfile.ReadItem;
const WrittenItem = logfile.WrittenItem;

const SHARDS = 16;
const ZITCASK_TOMBSTONE = "__zitcask_tombstone__";
const ZITCASK_SENTINEL = "__zitcask_sentinel__";

const bitcask = @This();

pub const Params = struct {
    pub const small: Params = .{
        .numShards = 8,
        .maxLogFileSize = 1024 * 1024 * 30, // 30Mib
    };

    pub const standard: Params = .{
        .numShards = 32,
        .maxLogFileSize = 1024 * 1024 * 256, // 256Mib
    };

    pub const xlarge: Params = .{
        .numShards = 128,
        .maxLogFileSize = 1024 * 1024 * 512, // 512Mib
    };

    numShards: usize,
    maxLogFileSize: usize,
};

const Entry = struct {
    fileId: u32,
    valueOffset: usize,
    valueSize: usize,
};

pub const Storage = struct {
    const Self = @This();

    path: Dir,
    currentFileId: u32,
    fileDir: AutoHashMap(u32, LogFile),
    keyDir: ConcurrentMap(Entry),
    mutex: RwLock,
    params: Params,
    allocator: Allocator,

    pub fn open(allocator: Allocator, path: Dir, params: bitcask.Params) !Self {
        var fileDir = std.AutoHashMap(u32, LogFile).init(allocator);
        errdefer fileDir.deinit();
        var keyDir = try ConcurrentMap(Entry).init(allocator, params.numShards);
        errdefer keyDir.deinit();

        var listOfFileIds = try ensureFileIds(allocator, path);
        defer allocator.free(listOfFileIds);
        std.mem.sort(u32, listOfFileIds, {}, std.sort.asc(u32));

        // Open, add log files and their content.
        for (listOfFileIds) |fileId| {
            var dataLogFile = try LogFile.openOrCreate(allocator, path, fileId, params.maxLogFileSize);
            var dataLogFileIter = dataLogFile.iterator();
            while (dataLogFileIter.next()) |readItem| {
                // if item is deleted, remove previous entry.
                if (std.mem.eql(u8, readItem.value, ZITCASK_TOMBSTONE)) {
                    _ = keyDir.remove(readItem.key);
                    dataLogFile.freeReadItem(readItem);
                    continue;
                }

                try keyDir.put(readItem.key, Entry{
                    .fileId = fileId,
                    .valueOffset = readItem.valueOffset,
                    .valueSize = readItem.value.len,
                });
                dataLogFile.freeReadItem(readItem);
            }

            dataLogFile.setWritePosition(dataLogFileIter.getOffset());
            try fileDir.put(fileId, dataLogFile);
        }
        // std.process.exit(1);

        return Self{
            .path = path,
            .currentFileId = listOfFileIds[listOfFileIds.len - 1],
            .fileDir = fileDir,
            .keyDir = keyDir,
            .mutex = RwLock{},
            .params = params,
            .allocator = allocator,
        };
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        var currentLogFile = try self.ensureCurrentFileSize();
        const writeItem = try currentLogFile.writeItem(key, value);
        try self.keyDir.put(key, Entry{
            .fileId = self.currentFileId,
            .valueOffset = writeItem.valueOffset,
            .valueSize = value.len,
        });
    }

    pub fn get(self: *Self, key: []const u8) !?[]u8 {
        const entryOpt = self.keyDir.get(key);
        if (entryOpt) |entry| {
            var currentLogFile = self.fileDir.get(entry.fileId).?;
            return try currentLogFile.readValueAlloc(entry.valueOffset, entry.valueSize);
        }
        return null;
    }

    pub fn free(self: *Self, value: []u8) void {
        self.allocator.free(value);
    }

    fn remove(self: *Self, key: []const u8) !bool {
        const entryOpt = self.keyDir.get(key);
        if (entryOpt) |_| {
            // record operation in log
            var currentLogFile = try self.ensureCurrentFileSize();
            _ = try currentLogFile.put(key, ZITCASK_TOMBSTONE);
            // remove from KeyDir
            try self.keyDir.remove(key);
            return true;
        }
        return false;
    }

    fn compact(self: *Self) !void {
        //TODO:
        _ = self;
    }

    pub fn deinit(self: *Self) void {
        var iter = self.fileDir.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.fileDir.deinit();
        self.keyDir.deinit();
    }

    fn ensureCurrentFileSize(self: *Self) !*LogFile {
        var currentLogFile = self.fileDir.getPtr(self.currentFileId).?;
        if (!currentLogFile.isFull()) {
            return currentLogFile;
        }

        const fileId = self.currentFileId + 1;
        var logFile = try LogFile.openOrCreate(self.allocator, self.path, fileId, self.params.maxLogFileSize);
        try self.fileDir.put(fileId, logFile);
        self.currentFileId = fileId;
        return &logFile;
    }
};

// returns a list of the existing data file ids
// or a default list containing one file if dir is empty
fn ensureFileIds(allocator: Allocator, path: Dir) ![]u32 {
    var listOfFileIds = try utils.ownedFileIdsFromDir(allocator, path);
    if (listOfFileIds.len == 0) {
        allocator.free(listOfFileIds);
        listOfFileIds = try allocator.alloc(u32, 1);
        listOfFileIds[0] = 0; // default first fileId: 0
    }
    return listOfFileIds;
}
