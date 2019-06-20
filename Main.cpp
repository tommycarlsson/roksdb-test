// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <random>
#include <array>
#include <algorithm>
#include <iostream>
#include <fstream>

#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <args.hxx>

#include "timer.h"

using namespace rocksdb;
using namespace std;

//std::string kDBPath = "data/rocksdb_simple_example";
std::string kDBPath = "D:/disk-test/rocksdb_simple_example";


auto logger = spdlog::basic_logger_st("logger", "log.txt");

int hello_world()
{
    DB* db;
    Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;

    // open DB
    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());

    // Put key-value
    s = db->Put(WriteOptions(), "key1", "value");
    assert(s.ok());
    string value;
    // get value
    s = db->Get(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value");

    // atomically apply a set of updates
    {
        WriteBatch batch;
        batch.Delete("key1");
        batch.Put("key2", value);
        s = db->Write(WriteOptions(), &batch);
    }

    s = db->Get(ReadOptions(), "key1", &value);
    assert(s.IsNotFound());

    db->Get(ReadOptions(), "key2", &value);
    assert(value == "value");

    {
        PinnableSlice pinnable_val;
        db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
        assert(pinnable_val == "value");
    }

    {
        string string_val;
        // If it cannot pin the value, it copies the value to its internal buffer.
        // The intenral buffer could be set during construction.
        PinnableSlice pinnable_val(&string_val);
        db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
        assert(pinnable_val == "value");
        // If the value is not pinned, the internal buffer must have the value.
        assert(pinnable_val.IsPinned() || string_val == "value");
    }

    PinnableSlice pinnable_val;
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &pinnable_val);
    assert(s.IsNotFound());
    // Reset PinnableSlice after each use and before each reuse
    pinnable_val.Reset();
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
    pinnable_val.Reset();
    // The Slice pointed by pinnable_val is not valid after this point

    delete db;

    return 0;
}

int just_read()
{
    DB* db;
    Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;

    // open DB
    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());

    // get value
    string value;
    s = db->Get(ReadOptions(), "key1", &value);
    assert(s.IsNotFound());

    db->Get(ReadOptions(), "key2", &value);
    assert(value == "value");

    {
        PinnableSlice pinnable_val;
        db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
        assert(pinnable_val == "value");
    }

    {
        string string_val;
        // If it cannot pin the value, it copies the value to its internal buffer.
        // The intenral buffer could be set during construction.
        PinnableSlice pinnable_val(&string_val);
        db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
        assert(pinnable_val == "value");
        // If the value is not pinned, the internal buffer must have the value.
        assert(pinnable_val.IsPinned() || string_val == "value");
    }

    PinnableSlice pinnable_val;
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &pinnable_val);
    assert(s.IsNotFound());
    // Reset PinnableSlice after each use and before each reuse
    pinnable_val.Reset();
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
    pinnable_val.Reset();
    // The Slice pointed by pinnable_val is not valid after this point

    delete db;

    return 0;
}

using Blob = vector<char>;

void fill_blob(Blob& blob)
{
    random_device rnd;
    default_random_engine eng(rnd());

    uniform_int_distribution<> uid1(0, 255);

    generate(blob.begin(), blob.end(), [&]
    {
        return uid1(eng);
    });
}

double write_rocks(Blob& blob, int count, string file_name)
{
    DB* db;
    Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;

    // open DB
    //Status s = DB::Open(options, "Z:/tmp/rocksdb-test/big-data", &db);
    Status s = DB::Open(options, file_name, &db);

    Timer timer;
        
    // Put key-value one by one
    for (auto i(0); i != count; ++i)
    {
        timer.start();
        s = db->Put(WriteOptions(), to_string(i), Slice(blob.data(), blob.size()));
        timer.stop();
        //fill_blob(blob);
    }

    delete db;

    return timer.elapsedSeconds();
}

double read_rocks(Blob& blob, int count, string file_name)
{
    DB* db;
    Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();

    // open DB
    Status s = DB::Open(options, file_name, &db);

    Timer timer;

    // Get key-value one by one
    for (auto i(0); i != count; ++i)
    {
        PinnableSlice pinnable_val;
        timer.start();
        s = db->Get(ReadOptions(), db->DefaultColumnFamily(), to_string(i), &pinnable_val);
        timer.stop();
    }

    delete db;

    return timer.elapsedSeconds();
}

double file_stream_write(Blob& blob, int count, string file_name)
{
    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        timer.start();
        auto myfile = ofstream(name, ios::binary);
        myfile.write(blob.data(), blob.size());
        myfile.close();
        timer.stop();
    }
    return timer.elapsedSeconds();
}

double c_style_io_write(Blob& blob, int count, string file_name)
{
    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        timer.start();
        FILE* file = fopen(file_name.c_str(), "wb");
        fwrite(blob.data(), 1, blob.size(), file);
        fclose(file);
        timer.stop();
    }
    return timer.elapsedSeconds();
}

double file_stream_no_sync_write(Blob& blob, int count, string file_name)
{
    Timer timer;
    ios_base::sync_with_stdio(false);
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        timer.start();
        auto myfile = ofstream(file_name, ios::binary);
        myfile.write(blob.data(), blob.size());
        myfile.close();
        timer.stop();
    }
    return timer.elapsedSeconds();
}

void print_result(double secs, string const& msg, size_t blob_size, int nbr_of_blobs)
{
    spdlog::info("{:7.2f}s, {:7.1f}MB/s :{}", secs, nbr_of_blobs * (blob_size / 1048576) / secs, msg);
}

int main(int argc, char* argv[])
{
    spdlog::set_default_logger(logger);
    spdlog::set_level(spdlog::level::info);
    spdlog::flush_on(spdlog::level::info);
    spdlog::set_pattern("[%D %H:%M:%S] %v");

    args::ArgumentParser parser("This is a io performance test program.");
    args::HelpFlag help(parser, "help", "Display this help menu", { 'h', "help" });
    args::CompletionFlag completion(parser, { "complete" });
    args::ValueFlag<int> nbrOfBlobs(parser, "nbrOfBlobs", "Number of blobs", { 'n' }, 100);
    args::ValueFlag<int> blobSize(parser, "blobSize", "Size of a blob [MB]", { 's' }, 1048576 * 15);

    try
    {
        parser.ParseCLI(argc, argv);
    }
    catch (const args::Completion& e)
    {
        std::cout << e.what();
        return 0;
    }
    catch (const args::Help&)
    {
        std::cout << parser;
        return 0;
    }
    catch (const args::ParseError& e)
    {
        std::cerr << e.what() << std::endl;
        std::cerr << parser;
        return 1;
    }

    int const nbr_of_blobs = nbrOfBlobs.Get();
    int const blob_size = blobSize.Get();

    spdlog::info("===== Start test with a blob of size {} bytes and with {} nbr of blobs ============",
        blob_size, nbr_of_blobs);

    hello_world();
    just_read();

    Blob blob(blob_size, '1');

    Timer timer;
    timer.start();

    double secs(0);

    secs = write_rocks(blob, nbr_of_blobs, "D:/disk-test/rocksdb");
    print_result(secs, "Big data single write", blob.size(), nbr_of_blobs);

    secs = read_rocks(blob, nbr_of_blobs, "D:/disk-test/rocksdb");
    print_result(secs, "Big data single read", blob.size(), nbr_of_blobs);

    secs = file_stream_write(blob, nbr_of_blobs, "D:/disk-test/file_stream_write");
    print_result(secs, "file_stream_write", blob.size(), nbr_of_blobs);

    secs = file_stream_write(blob, nbr_of_blobs, "D:/disk-test/c_style_io_write");
    print_result(secs, "c_style_io_write", blob.size(), nbr_of_blobs);

    secs = file_stream_write(blob, nbr_of_blobs, "D:/disk-test/file_stream_no_sync_write");
    print_result(secs, "file_stream_no_sync_write", blob.size(), nbr_of_blobs);

    timer.stop();
    spdlog::info("Total time: {}s", timer.elapsedSeconds());

    return 0;
}