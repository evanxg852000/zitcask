const std = @import("std");
const logfile = @import("./logfile.zig");
const utils = @import("./utils.zig");

const Dir = std.fs.Dir;
const AutoHashMap = std.AutoHashMap;
const LogFile = logfile.LogFile;
const ReadItem = logfile.ReadItem;
const WrittenItem = logfile.WrittenItem;

const SHARDS = 16
const ZITCASK_TOMBSTONE = "__zitcask_tombstone__";

const bitcask = @This();

pub const Params =  struct {
    pub const small: Params = . {
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
}

const Entry = struct {
    fileId: u32,
    valueOffset: usize,
    valueSize: usize,
}

pub const Storage = struct {
    const Self = @This();

    path: Dir,
    currentFileId: u32,
    fileDir: AutoHashMap(u32, LogFile),
    keyDir: ConcurrentMap(Entry),
    mutex: RWLock,
    params: Params,
    allocator: Allocator,
    
    pub fn open(allocator: Allocator, path: Dir, params: bitcask.Params) !Self {
        const currentFileId: u32 = 0;
        var fileDir = std.AutoHashMap(u32, LogFile).init(allocator);
        errdefer fileDir.deinit();
        var keyDir = try ConcurrentMap(Entry).init(allocator, params.numShards);
        errdefer keyDir.deinit();

        var listOfFileIds = ensureFileIds(allocator, path)
        defer allocator.free(listOfFileIds);
        std.sort.sort(u32, listOfFileIds, {}, std.sort.asc(u32));

        // Open, add log files and their content.
        for(listOfFileIds) |fileId| {
            var dataLogFile = try LogFile.openOrCreate(allocator, path, fileId, params.maxLogFileSize);
            var dataLogFileIter = dataLogFile.iterator();
            while(dataLogFileIter.next()) |readItem| {
                // if item is deleted, remove previous entry.
                if(std.mem.eql(readItem.value, ZITCASK_TOMBSTONE)) {
                    self.keyDir.remove(readItem.key);
                    dataLogFile.freeReadItem(readItem);
                    continue;
                }
                try self.keyDir.put(readItem.key, Entry{
                    .fileId = fileId,
                    .valueOffset = readItem.valueOffset,
                    .valueSize = readItem.value.len,
                });
                dataLogFile.freeReadItem(readItem);
            }
            try fileDir.put(fileId, logFile);
        }
        
        return Self {
            .path = path,
            .currentFileId = listOfFileIds[listOfFileIds.len-1],
            .fileDir = fileDir,
            .keyDir = keyDir,
            .mutex = RWLock{},
            .params = params,
            .allocator = allocator,
        }
    }

    pub fn put(self: *Self, key: [] const u8, value: [] const u8) !void {
        var currentLogFile = try self.ensureCurrentFileSize();
        const writeItem = try currentLogFile.put(key, value);
        try self.keyDir.put(key, Entry{
            .fileId = currentFileId,
            .valueOffset = writeItem.valueOffset,
            .valueSize = value.len,
        });
    }

    pub fn get(key: []const u8) !?[]u8 {
        const entryOpt = self.keyDir.get(key);
        if(entryOpt) |entry| {
            var currentLogFile = try self.fileDir.get(entry.fileId).?;
            return try currentLogFile.readValueAlloc(entry.valueOffset, entry.valueSize);
        }
        return null;
    }

    pub fn free(self: *Self, value: []u8) void {
        self.allocator.free(value);
    }

    fn remove(self: *Self, key: []const u8) !bool {
        const entryOpt = self.keyDir.get(key);
        if(entryOpt) |entry| {
            // record operation in log
            var currentLogFile = try self.ensureCurrentFileSize();
            const writeItem = try currentLogFile.put(key, ZITCASK_TOMBSTONE);
            // remove from KeyDir
            try self.keyDir.remove(key);
            return true;
        }
        return false;
    }

    fn compact(self: *self) !void {
        //TODO:
    }

    pub fn deinit(self: *self) void {
        self.fileDir.deinit();
        self.keyDir.deinit();
    }

    fn ensureCurrentFileSize(self: *Self) !*LogFile {
        var currentLogFile = self.fileDir.get(self.currentFileId).?;
        if(!currentLogFile.isFull()) {
            return &currentLogFile;
        }

        const fileId = self.currentFileId + 1;
        var logFile = try LogFile.openOrCreate(self.allocator, self.path, fileId, self.params.maxLogFileSize)
        try self.fileDir.put(fileId, logFile);
        self.currentFileId = fileId;
        return &logFile;
    }

}


// returns a list of the existing data file ids
// or a default list containing one file if dir is empty
fn ensureFileIds(allocator: Allocator, path: Dir) ![]u32 {
    var listOfFileIds = utils.ownedFileIdsFromDir(allocator, path,)
    if(listOfFileIds.len == 0) {
        allocator.free(listOfFileIds);
        listOfFileIds = allocator.alloc(u32, 1);
        // first fileId: 0
        listOfFileIds[0] = 0;
    }
    return listOfFileIds;
}
