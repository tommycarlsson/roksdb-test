// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//#ifdef _WIN32
#include <Windows.h>
#include <Psapi.h>
//#endif
#include <cstdio>
#include <string>
#include <random>
#include <array>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>

#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <H5Cpp.h>
#include <highfive/H5DataSet.hpp>
#include <highfive/H5DataSpace.hpp>
#include <highfive/H5File.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <args.hxx>

#include "timer.h"

using namespace rocksdb;
using namespace std;

//std::string kDBPath = "data/rocksdb_simple_example";
std::string kDBPath = "D:/disk-test/rocksdb_simple_example";


auto logger = spdlog::basic_logger_st("logger", "log.txt");

#ifdef ROCKSDB
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
#endif

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

void write_chunks(size_t index, function<void(Blob const &)> const writer)
{
    static Blob const chunk1(96, '1');
    static Blob const chunk2(90, '2');
    static Blob const chunk3(96, '3');
    static Blob const chunk4(60, '4');
    static array<Blob, 4> const chunks{ chunk1, chunk2, chunk3, chunk4 };
    static size_t const chunks_size(96 + 90 + 96 + 60);
    while (index > chunks_size)
    {
        for_each(chunks.begin(), chunks.end(), writer);
        index -= chunks_size;
    }
}

void read_chunks(size_t index, function<void(Blob&)> const reader)
{
    static Blob chunk1(96);
    static Blob chunk2(90);
    static Blob chunk3(96);
    static Blob chunk4(60);
    static array<Blob, 4> chunks{ chunk1, chunk2, chunk3, chunk4 };
    static size_t const chunks_size(96 + 90 + 96 + 60);
    while (index > chunks_size)
    {
        for_each(chunks.begin(), chunks.end(), reader);
        index -= chunks_size;
    }
}

#ifdef ROCKSDB
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
#endif // ROCKSDB

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

double file_stream_read(Blob& blob, int count, string file_name)
{
    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        timer.start();
        auto myfile = ifstream(name, ios::binary);
        myfile.read(blob.data(), blob.size());
        myfile.close();
        timer.stop();
    }
    return timer.elapsedSeconds();
}

double file_stream_write_seq(size_t blob_size, int count, string file_name)
{
    struct Writer
    {
        Writer(ofstream& ofs) : m_ofs(ofs) {}
        void write(Blob const& blob) const { m_ofs.write(blob.data(), blob.size()); }
        ofstream& m_ofs;
    };

    using std::placeholders::_1;

    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        timer.start();
        auto myfile = ofstream(name, ios::binary);
        write_chunks(blob_size, bind(&Writer::write, Writer(myfile), _1));
        myfile.close();
        timer.stop();
    }
    return timer.elapsedSeconds();
}

double file_stream_read_seq(size_t blob_size, int count, string file_name)
{
    struct Reader
    {
        Reader(ifstream& ifs) : m_ifs(ifs) {}
        void read(Blob& blob) const { m_ifs.read(blob.data(), blob.size()); }
        ifstream& m_ifs;
    };

    using std::placeholders::_1;

    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        timer.start();
        auto myfile = ifstream(name, ios::binary);
        read_chunks(blob_size, bind(&Reader::read, Reader(myfile), _1));
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
        FILE* file = fopen(name.c_str(), "wb");
        fwrite(blob.data(), 1, blob.size(), file);
        fclose(file);
        timer.stop();
    }
    return timer.elapsedSeconds();
}

double c_style_io_read(Blob& blob, int count, string file_name)
{
    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        timer.start();
        FILE* file = fopen(name.c_str(), "rb");
        fread(blob.data(), 1, blob.size(), file);
        fclose(file);
        timer.stop();
    }
    return timer.elapsedSeconds();
}

double c_style_io_write_seq(size_t blob_size, int count, string file_name)
{
    struct Writer
    {
        Writer(FILE* file) : m_file(file) {}
        void write(Blob const& blob) const { fwrite(blob.data(), 1, blob.size(), m_file); }
        FILE* m_file;
    };

    using std::placeholders::_1;

    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        timer.start();
        FILE* file = fopen(name.c_str(), "wb");
        write_chunks(blob_size, bind(&Writer::write, Writer(file), _1));
        fclose(file);
        timer.stop();
    }
    return timer.elapsedSeconds();
}

double c_style_io_read_seq(size_t blob_size, int count, string file_name)
{
    struct Reader
    {
        Reader(FILE* file) : m_file(file) {}
        void read(Blob& blob) const { fread(blob.data(), 1, blob.size(), m_file); }
        FILE* m_file;
    };

    using std::placeholders::_1;

    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        timer.start();
        FILE* file = fopen(name.c_str(), "rb");
        read_chunks(blob_size, bind(&Reader::read, Reader(file), _1));
        fclose(file);
        timer.stop();
    }
    return timer.elapsedSeconds();
}

double hdf5_write(Blob& blob, int count, string file_name)
{
    using namespace H5;

#pragma pack(push,1)
    struct SLSSCommonHeader
    {
        char cFileSignature[6]{ 'A', 'B', 'C', 'C', 'D', 'E' };
        int nVersion = 8;
    } header;
#pragma pack( pop )

    hsize_t dims[1];
    dims[0] = blob.size();
    DataSpace dataspace(1, dims);

    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        //fill_blob(blob);
        auto name = file_name + to_string(i) + ".hdf5";
        //cout << "Writing: " << name << endl;
        timer.start();
        FileCreatPropList fileProp;
        fileProp.setUserblock(512);
        H5File file(name, H5F_ACC_TRUNC, fileProp);
        DataSet dataset = file.createDataSet("blob", PredType::STD_I8LE, dataspace);
        dataset.write(blob.data(), PredType::NATIVE_CHAR);

        file.close();

        auto myfile = ofstream(name, ios::binary | ios::in | ios::out);
        myfile.write((char*)&header, sizeof(header));
        myfile.close();

        timer.stop();
    }
    return timer.elapsedSeconds();
}

double hdf5_read(Blob& blob, int count, string file_name)
{
    using namespace H5;

    hsize_t dims[1];
    dims[0] = blob.size();
    DataSpace dataspace(1, dims);

    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i) + ".hdf5";
        //cout << "Reading: " << name << endl;
        timer.start();
        H5File file(name, H5F_ACC_RDONLY);
        DataSet dataset = file.openDataSet("blob");
        dataset.read(blob.data(), PredType::NATIVE_CHAR);
        timer.stop();
    }
    return timer.elapsedSeconds();
}

double hdf5_write_seq(size_t blob_size, int count, string file_name)
{
    using namespace H5;
    using std::placeholders::_1;

    struct Writer
    {
        Writer(DataSet& dataset) : m_dataset(dataset) {}
        void write(Blob const& blob)
        {
            if (m_cursor + blob.size() >= m_size[0])
            {
                m_size[0] += 1024*1024;
                m_dataset.extend(m_size);
            }
            hsize_t dims[1]{ blob.size() };
            DataSpace mspace(1, dims);
            hsize_t offset[1]{ m_cursor };
            DataSpace fspace(m_dataset.getSpace());
            fspace.selectHyperslab(H5S_SELECT_SET, dims, offset);
            m_dataset.write(blob.data(), PredType::NATIVE_CHAR, mspace, fspace);
            m_cursor += blob.size();
        }
        DataSet& m_dataset;
        hsize_t m_cursor{ 0 };
        hsize_t m_size[1]{ 0 };
    };

    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i) + ".hdf5";
        
        timer.start();

        hsize_t fdims[1]{ 1024*1024*15 };
        hsize_t fdims_max[1]{ H5S_UNLIMITED };
        DataSpace fspace(1, fdims, fdims_max);

        DSetCreatPropList cparms;
        hsize_t chunk_dims[1]{ 1024*1024 };
        cparms.setChunk(1, chunk_dims);

        H5File file(name, H5F_ACC_TRUNC);
        DataSet dataset = file.createDataSet("blobs", PredType::STD_I8LE, fspace, cparms);

        Writer writer(dataset);
        write_chunks(blob_size, bind(&Writer::write, &writer, _1));

        file.close();

        timer.stop();
    }
    return timer.elapsedSeconds();
}

void emptyWorkingSet()
{
#ifdef _WIN32
    ::EmptyWorkingSet(::GetCurrentProcess());
#endif // _WIN32
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

    ostringstream os;
    os << "To run one test explicitly\n";
    os << "0\t All tests (default)\n";
    os << "1\t write_rocks\n";
    os << "2\t file_stream_write\n";
    os << "3\t c_style_io_write\n";
    os << "4\t read_rocks\n";
    os << "5\t file_stream_read\n";
    os << "6\t c_style_io_read\n";
    os << "7\t file_stream_write_seq\n";
    os << "8\t c_style_io_write_seq\n";
    os << "9\t file_stream_read_seq\n";
    os << "10\t c_style_io_read_seq\n";
    os << "11\t hdf5_write\n";
    os << "12\t hdf5_read\n";
    os << "13\t hdf5_write_seq\n";

    args::ArgumentParser parser("This is a io performance test program.", os.str());
    args::HelpFlag help(parser, "help", "Display this help menu", { 'h', "help" });
    args::CompletionFlag completion(parser, { "complete" });
    args::ValueFlag<int> nbrOfBlobs(parser, "nbrOfBlobs", "Number of blobs", { 'n' }, 100);
    args::ValueFlag<int> blobSize(parser, "blobSize", "Size of a blob [MB]", { 's' }, 1048576 * 15);
    args::PositionalList<int> tests(parser, "tests", "Tests to run");

    try
    {
        parser.ParseCLI(argc, argv);
    }
    catch (const args::Completion& e)
    {
        cout << e.what();
        return 0;
    }
    catch (const args::Help&)
    {
        cout << parser;
        return 0;
    }
    catch (const args::ParseError& e)
    {
        cerr << e.what() << std::endl;
        cerr << parser;
        return 1;
    }

    auto t = args::get(tests);

    //hello_world();
    //just_read();

    int const nbr_of_blobs = nbrOfBlobs.Get();
    int const blob_size = blobSize.Get();

    spdlog::info("===== Start test with a blob of size {} bytes and with {} nbr of blobs ============",
        blob_size, nbr_of_blobs);

    Blob blob(blob_size, '1');

    Timer timer;
    timer.start();

    double secs(0);

#ifdef ROCKSDB
    if (t.empty() || find(t.begin(), t.end(), 1) != t.end())
    {
        cout << "Running write_rocks ..." << endl;
        secs = write_rocks(blob, nbr_of_blobs, "D:/disk-test/rocksdb");
        print_result(secs, "write_rocks", blob.size(), nbr_of_blobs);
    }
#endif // ROCKSDB

    if (t.empty() || find(t.begin(), t.end(), 2) != t.end())
    {
        cout << "Running file_stream_write ..." << endl;
        secs = file_stream_write(blob, nbr_of_blobs, "D:/disk-test/file_stream_write");
        print_result(secs, "file_stream_write", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 3) != t.end())
    {
        cout << "Running c_style_io_write ..." << endl;
        secs = c_style_io_write(blob, nbr_of_blobs, "D:/disk-test/c_style_io_write");
        print_result(secs, "c_style_io_write", blob.size(), nbr_of_blobs);
    }
    
#ifdef ROCKSDB
    if (t.empty() || find(t.begin(), t.end(), 4) != t.end())
    {
        cout << "Running read_rocks ..." << endl;
        secs = read_rocks(blob, nbr_of_blobs, "D:/disk-test/rocksdb");
        print_result(secs, "read_rocks", blob.size(), nbr_of_blobs);
    }
#endif // ROCKSDB

    if (t.empty() || find(t.begin(), t.end(), 5) != t.end())
    {
        cout << "Running file_stream_read ..." << endl;
        secs = file_stream_read(blob, nbr_of_blobs, "D:/disk-test/file_stream_write");
        print_result(secs, "file_stream_read", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 6) != t.end())
    {
        cout << "Running c_style_io_read ..." << endl;
        secs = c_style_io_read(blob, nbr_of_blobs, "D:/disk-test/c_style_io_write");
        print_result(secs, "c_style_io_read", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 7) != t.end())
    {
        cout << "Running file_stream_write_seq ..." << endl;
        secs = file_stream_write_seq(blob.size(), nbr_of_blobs, "D:/disk-test/file_stream_write_seq");
        print_result(secs, "file_stream_write_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 8) != t.end())
    {
        cout << "Running c_style_io_write_seq ..." << endl;
        secs = c_style_io_write_seq(blob.size(), nbr_of_blobs, "D:/disk-test/c_style_io_write_seq");
        print_result(secs, "c_style_io_write_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 9) != t.end())
    {
        cout << "Running file_stream_read_seq ..." << endl;
        secs = file_stream_read_seq(blob.size(), nbr_of_blobs, "D:/disk-test/file_stream_write_seq");
        print_result(secs, "file_stream_read_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 10) != t.end())
    {
        cout << "Running c_style_io_read_seq ..." << endl;
        secs = c_style_io_read_seq(blob.size(), nbr_of_blobs, "D:/disk-test/c_style_io_write_seq");
        print_result(secs, "c_style_io_read_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 11) != t.end())
    {
        cout << "Running hdf5_write ..." << endl;
        secs = hdf5_write(blob, nbr_of_blobs, "D:/disk-test/hdf5_write");
        print_result(secs, "hdf5_write", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 12) != t.end())
    {
        emptyWorkingSet();
        cout << "Running hdf5_read ..." << endl;
        secs = hdf5_read(blob, nbr_of_blobs, "D:/disk-test/hdf5_write");
        print_result(secs, "hdf5_read", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 13) != t.end())
    {
        cout << "Running hdf5_write_seq ..." << endl;
        secs = hdf5_write_seq(blob.size(), nbr_of_blobs, "D:/disk-test/hdf5_write_seq");
        print_result(secs, "hdf5_write_seq", blob.size(), nbr_of_blobs);
    }

    timer.stop();
    spdlog::info("Total time: {}s", timer.elapsedSeconds());

    return 0;
}