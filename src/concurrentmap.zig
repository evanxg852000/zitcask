const std = @import("std");

const Allocator = std.mem.Allocator;
const Mutex = std.Thread.Mutex;
const ArrayList = std.ArrayList;
const StringHashMap = std.StringHashMap;
const Fnv1a_32 = std.hash.Fnv1a_32;

const testing = std.testing;

fn HashMapShard(comptime V: type) type {
    return struct {
        const Self = @This();

        items: StringHashMap(V),
        mutex: std.Thread.Mutex,
        allocator: Allocator,

        fn init(allocator: Allocator) Self {
            return Self{
                .items = StringHashMap(V).init(allocator),
                .mutex = Mutex{},
                .allocator = allocator,
            };
        }

        fn deinit(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            var keyIter = self.items.keyIterator();
            while (keyIter.next()) |key| {
                self.allocator.free(key.*);
            }
            self.items.deinit();
        }

        fn itemsCount(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.items.count();
        }

        fn put(self: *Self, key: []const u8, value: V) !void {
            self.mutex.lock();
            defer self.mutex.unlock();
            const ownedKey = try self.allocator.dupe(u8, key);
            const oldEntryOpt = try self.items.fetchPut(ownedKey, value);
            if (oldEntryOpt) |_| {
                self.allocator.free(ownedKey);
            }
        }

        fn get(self: *Self, key: []const u8) ?V {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.items.get(key);
        }

        fn remove(self: *Self, key: []const u8) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.items.fetchRemove(key)) |oldEntry| {
                self.allocator.free(oldEntry.key);
                return true;
            }
            return false;
        }
    };
}

pub fn ConcurrentMap(comptime V: type) type {
    return struct {
        const Self = @This();
        const HashMapShardType = HashMapShard(V);

        shards: ArrayList(HashMapShardType),
        allocator: Allocator,

        pub fn init(allocator: Allocator, num_shards: usize) !Self {
            var shards = ArrayList(HashMapShardType).init(allocator);
            errdefer shards.deinit();
            for (0..num_shards) |_| {
                try shards.append(HashMapShardType.init(allocator));
            }
            return Self{
                .shards = shards,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.shards.items) |*map| {
                map.deinit();
            }
            self.shards.deinit();
        }

        pub fn itemsCount(self: *Self) usize {
            var count: usize = 0;
            for (self.shards.items) |*map| {
                count += map.itemsCount();
            }
            return count;
        }

        pub fn shardsCount(self: *Self) usize {
            return self.shards.items.len;
        }

        pub fn put(self: *Self, key: []const u8, value: V) !void {
            const index = @as(usize, Fnv1a_32.hash(key)) % self.shards.items.len;
            const shard = &self.shards.items[index];
            return shard.put(key, value);
        }

        pub fn get(self: *Self, key: []const u8) ?V {
            const index = @as(usize, Fnv1a_32.hash(key)) % self.shards.items.len;
            const shard = &self.shards.items[index];
            return shard.get(key);
        }

        pub fn remove(self: *Self, key: []const u8) bool {
            const index = @as(usize, Fnv1a_32.hash(key)) % self.shards.items.len;
            const shard = &self.shards.items[index];
            return shard.remove(key);
        }
    };
}

test "Concurent map shard" {
    const expect = std.testing.expect;
    const allocator = std.testing.allocator;

    var cms = HashMapShard(u32).init(allocator);
    defer cms.deinit();
    try cms.put("10", 10);
    try cms.put("11", 11);

    try expect(cms.itemsCount() == 2);

    try cms.put("10", 10);
    try cms.put("12", 12);
    try expect(cms.itemsCount() == 3);
    try expect(cms.get("10") == 10);
    try expect(cms.get("2") == null);

    try expect(cms.remove("10") == true);
    try expect(cms.remove("2") == false);
    try expect(cms.itemsCount() == 2);
}

test "concurrent haspmap" {
    const expect = std.testing.expect;
    const allocator = std.testing.allocator;

    var chm = try ConcurrentMap(u32).init(allocator, 4);
    defer chm.deinit();
    try expect(chm.shardsCount() == 4);
    try expect(chm.itemsCount() == 0);

    try chm.put("10", 10);
    try chm.put("11", 11);
    try chm.put("12", 12);

    try expect(chm.shardsCount() == 4);
    try expect(chm.itemsCount() == 3);

    try expect(chm.get("11") == 11);

    try expect(chm.remove("10") == true);
    try expect(chm.remove("2") == false);
    try expect(chm.itemsCount() == 2);
}
