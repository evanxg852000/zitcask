
const LOG_FILE_MAX_SIZE_BYTES = 1000 * 1000 * 512; // 512MB
const SHARDS = 16 

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

pub const BitcaskStorage = struct {
    const Self = @This();

    path: []const u8,
    fileDir: ArrayList(fileHandle),
    keyDir: ConcurrentMap(Entry),
    mutex: RWLock,
    numShards: usize,
    maxLogFileSize: usize,
    allocator: Allocator,

    pub fn open(allocator: Allocator, dir: []const u8, params: bitcask.Params) !Self {

        // load files

    }

    fn put(key: []const u8, value: []const u8) !void {

    }

    fn get(key: []const u8) ![]const u8 {

    }

    fn remove(key: []const u8) !bool {

    }

    fn compact(self: *self) !void {

    }


    listFile()
}
