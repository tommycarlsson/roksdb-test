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
#include <mio/mmap.hpp>
#include <cereal/archives/binary.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <args.hxx>

#include "timer.h"
#include "fake.h"

using namespace rocksdb;
using namespace std;

auto logger = spdlog::basic_logger_st("logger", "log.txt");
bool random = false;

const size_t bufsize = 1024 * 1024;
unique_ptr<char[]> buf(new char[bufsize]);

using Blob = vector<char>;

void fill_blob(Blob& blob)
{
    if (!random)
        return;

    random_device rnd;
    default_random_engine eng(rnd());

    uniform_int_distribution<> uid1(0, 255);

    generate(blob.begin(), blob.end(), [&]
    {
        return uid1(eng);
    });
}

void write_chunks(size_t index, Timer& timer, function<void(Blob const &)> const writer)
{
    static Blob chunk1(96, '1');
    static Blob chunk2(90, '2');
    static Blob chunk3(96, '3');
    static Blob chunk4(60, '4');
    static array<Blob, 4> chunks{ chunk1, chunk2, chunk3, chunk4 };
    static size_t const chunks_size(96 + 90 + 96 + 60);
    
    if (random)
    {
        while (index > chunks_size)
        {
            timer.stop();
            for_each(chunks.begin(), chunks.end(), [] (Blob& b){ fill_blob(b); });
            timer.start();

            for_each(chunks.begin(), chunks.end(), writer);
            index -= chunks_size;
        }
    }
    else
    {
        while (index > chunks_size)
        {
            for_each(chunks.begin(), chunks.end(), writer);
            index -= chunks_size;
        }
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
        cout << '#';
    }
    cout << endl;

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
        cout << '#';
    }
    cout << endl;

    delete db;
    
    return timer.elapsedSeconds();
}

double file_stream_write(Blob& blob, int count, string file_name)
{
    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        fill_blob(blob);
        timer.start();
        auto myfile = ofstream(name, ios::binary);
        myfile.rdbuf()->pubsetbuf(buf.get(), bufsize);
        myfile.write(blob.data(), blob.size());
        myfile.close();
        timer.stop();
        cout << '#';
    }
    cout << endl;
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
        cout << '#';
    }
    cout << endl;
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
        myfile.rdbuf()->pubsetbuf(buf.get(), bufsize);
        write_chunks(blob_size, timer, bind(&Writer::write, Writer(myfile), _1));
        myfile.close();
        timer.stop();
        cout << '#';
    }
    cout << endl;
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
        cout << '#';
    }
    cout << endl;
    return timer.elapsedSeconds();
}


double c_style_io_write(Blob& blob, int count, string file_name)
{
    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        fill_blob(blob);
        timer.start();
        FILE* file = fopen(name.c_str(), "wb");
        fwrite(blob.data(), 1, blob.size(), file);
        fclose(file);
        timer.stop();
        cout << '#';
    }
    cout << endl;
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
        cout << '#';
    }
    cout << endl;
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
        write_chunks(blob_size, timer, bind(&Writer::write, Writer(file), _1));
        fclose(file);
        timer.stop();
        cout << '#';
    }
    cout << endl;
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
        cout << '#';
    }
    cout << endl;
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
        fill_blob(blob);
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
        cout << '#';
    }
    cout << endl;
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
        cout << '#';
    }
    cout << endl;
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
            //H5Dwrite_chunk(m_dataset.getId(), H5P_DEFAULT, 0, offset, blob.size(), blob.data());
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
        write_chunks(blob_size, timer, bind(&Writer::write, &writer, _1));

        file.close();

        timer.stop();
        cout << '#';
    }
    cout << endl;
    return timer.elapsedSeconds();
}

double mio_write(Blob& blob, int count, string file_name)
{
    error_code error;
    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        fill_blob(blob);
        auto name = file_name + to_string(i) + ".mio";
        timer.start();
        auto myfile = ofstream(name, ios::binary | ios::trunc);
        myfile.seekp(blob.size() - 1);
        myfile.put('e');
        myfile.close();
        mio::mmap_sink rw_mmap = mio::make_mmap_sink(
            name, 0, mio::map_entire_file, error);
        if (error)
        {
            cout << error.message();
            return 0.0;
        }
        copy(begin(blob), end(blob), begin(rw_mmap));
        rw_mmap.sync(error);
        if (error)
        {
            cout << error.message();
            return 0.0;
        }
        rw_mmap.unmap();
        timer.stop();
        cout << '#';
    }
    cout << endl;
    return timer.elapsedSeconds();
}

double mio_read(Blob& blob, int count, string file_name)
{
    error_code error;
    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i) + ".mio";
        //cout << "Reading: " << name << endl;
        timer.start();
        mio::mmap_source ro_mmap;
        ro_mmap.map(name, error);
        if (error)
        {
            cout << error.message();
            return 0.0;
        }
        copy(begin(ro_mmap), end(ro_mmap), begin(blob));
        timer.stop();
        cout << '#';
    }
    cout << endl;
    return timer.elapsedSeconds();
}

double mio_write_seq(size_t blob_size, int count, string file_name)
{
    using namespace mio;
    using std::placeholders::_1;

    struct Writer
    {
        Writer(mmap_sink& rw_mmap) : m_rw_mmap(rw_mmap) {}
        void write(Blob const& blob)
        {
            error_code error;
            memcpy(&m_rw_mmap[m_cursor], blob.data(), blob.size());
            if (error) 
            {
                spdlog::error("Fail to sync in mio");
                return;
            }
            m_cursor += blob.size();
        }
        mmap_sink& m_rw_mmap;
        size_t m_cursor{ 0 };
    };

    error_code error;
    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i) + ".mio";
        timer.start();
        auto myfile = ofstream(name, ios::binary | ios::trunc);
        myfile.seekp(blob_size - 1);
        myfile.put('e');
        myfile.close();
        mio::mmap_sink rw_mmap = mio::make_mmap_sink(
            name, 0, mio::map_entire_file, error);
        if (error)
        {
            cout << error.message();
            return 0.0;
        }
        Writer writer(rw_mmap);
        write_chunks(blob_size, timer, bind(&Writer::write, &writer, _1));
        rw_mmap.sync(error);
        if (error)
        {
            cout << error.message();
            return 0.0;
        }
        rw_mmap.unmap();
        timer.stop();
        cout << '#';
    }
    cout << endl;
    return timer.elapsedSeconds();
}

double mio_read_seq(size_t blob_size, int count, string file_name)
{
    using namespace mio;

    struct Reader
    {
        Reader(mmap_source& ro_mmap) : m_ro_mmap(ro_mmap) {}
        void read(Blob& blob) const
        {
            memcpy(blob.data(), &m_ro_mmap[index], blob.size());
            index += blob.size();
        }
        mmap_source& m_ro_mmap;
        mutable size_t index{ 0 };
    };

    using std::placeholders::_1;

    error_code error;
    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i) + ".mio";
        timer.start();
        mmap_source ro_mmap;
        ro_mmap.map(name, error);
        if (error)
        {
            cout << error.message();
            return 0.0;
        }
        Reader reader(ro_mmap);
        mmap_source::const_iterator iter(ro_mmap.begin());
        read_chunks(blob_size, bind(&Reader::read, &reader, _1));
        timer.stop();
        cout << '#';
    }
    cout << endl;
    return timer.elapsedSeconds();
}

double cereal_write_seq(size_t blob_size, int count, string file_name)
{
    using namespace cereal;
    using std::placeholders::_1;

    struct Writer
    {
        Writer(BinaryOutputArchive& oarchive, Fake& fake) : m_oarchive(oarchive), m_fake(fake) {}
        void write(Blob const& blob) const { m_oarchive(m_fake); }
        BinaryOutputArchive& m_oarchive;
        Fake& m_fake;
    };

    Fake fake;

    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        timer.start();
        auto myfile = ofstream(name, ios::binary | ios::trunc);
        myfile.rdbuf()->pubsetbuf(buf.get(), bufsize);
        BinaryOutputArchive oarchive(myfile);
        write_chunks(blob_size, timer, bind(&Writer::write, Writer(oarchive, fake), _1));
        myfile.close();
        timer.stop();
        cout << '#';
    }
    cout << endl;
    return timer.elapsedSeconds();
}

double cereal_read_seq(size_t blob_size, int count, string file_name)
{
    using namespace cereal;
    using std::placeholders::_1;

    struct Reader
    {
        Reader(BinaryInputArchive& iarchive, Fake& fake) : m_iarchive(iarchive), m_fake(fake) {}
        void read(Blob& blob) const { m_iarchive(m_fake); }
        BinaryInputArchive& m_iarchive;
        Fake& m_fake;
    };

    Fake fake;

    Timer timer;
    for (auto i(0); i != count; ++i)
    {
        auto name = file_name + to_string(i);
        timer.start();
        auto myfile = ifstream(name, ios::binary);
        BinaryInputArchive iarchive(myfile);
        read_chunks(blob_size, bind(&Reader::read, Reader(iarchive, fake), _1));
        myfile.close();
        timer.stop();
        cout << '#';
    }
    cout << endl;
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
    os << "14\t mio_write\n";
    os << "15\t mio_read\n";
    os << "16\t mio_write_seq\n";
    os << "17\t mio_read_seq\n";
    os << "18\t cereal_write_seq\n";
    os << "19\t cereal_read_seq\n";

    args::ArgumentParser parser("This is a io performance test program.", os.str());
    args::HelpFlag help(parser, "help", "Display this help menu", { 'h', "help" });
    args::CompletionFlag completion(parser, { "complete" });
    args::ValueFlag<int> nbrOfBlobs(parser, "nbrOfBlobs", "Number of blobs", { 'n' }, 100);
    args::ValueFlag<int> blobSize(parser, "blobSize", "Size of a blob [MB]", { 's' }, 1048576 * 15);
    args::ValueFlag<std::string> dir(parser, "dir", "Output directory", { 'd', "dir" }, "D:/disk-test");
    args::Flag randomFlag(parser, "random", "Fill blob with random values and unique file names", { 'r' }, false);
    args::PositionalList<int> tests(parser, "tests", "Tests to run");

    ostringstream cmdLine;
    cmdLine << "Args: ";
    for (auto i(1); i != argc; ++i)
    {
        cmdLine << string(argv[i]) + " ";
    }

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

    random = randomFlag.Get();

    auto t = args::get(tests);
    int const nbr_of_blobs = args::get(nbrOfBlobs);
    int const blob_size = args::get(blobSize);

    spdlog::info("===== Start test with a rnd ({}) blob of size {} bytes and with {} nbr of blobs ============",
        random, blob_size, nbr_of_blobs);
    spdlog::info(cmdLine.str());

    Blob blob(blob_size, '1');

    srand(time(0));
    auto extension = random ? "_" + std::to_string(rand()) + "-" : "";

    Timer timer;
    timer.start();

    double secs(0);

    auto path(args::get(dir));

    if (t.empty() || find(t.begin(), t.end(), 1) != t.end())
    {
        cout << "Running write_rocks ..." << endl;
        secs = write_rocks(blob, nbr_of_blobs, path + "/rocksdb");
        print_result(secs, "write_rocks", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 2) != t.end())
    {
        cout << "Running file_stream_write ..." << endl;
        secs = file_stream_write(blob, nbr_of_blobs, path + "/file_stream_write" + extension);
        print_result(secs, "file_stream_write", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 3) != t.end())
    {
        cout << "Running c_style_io_write ..." << endl;
        secs = c_style_io_write(blob, nbr_of_blobs, path + "/c_style_io_write" + extension);
        print_result(secs, "c_style_io_write", blob.size(), nbr_of_blobs);
    }
    
    if (t.empty() || find(t.begin(), t.end(), 4) != t.end())
    {
        cout << "Running read_rocks ..." << endl;
        secs = read_rocks(blob, nbr_of_blobs, path + "/rocksdb");
        print_result(secs, "read_rocks", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 5) != t.end())
    {
        cout << "Running file_stream_read ..." << endl;
        secs = file_stream_read(blob, nbr_of_blobs, path + "/file_stream_write" + extension);
        print_result(secs, "file_stream_read", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 6) != t.end())
    {
        cout << "Running c_style_io_read ..." << endl;
        secs = c_style_io_read(blob, nbr_of_blobs, path + "/c_style_io_write" + extension);
        print_result(secs, "c_style_io_read", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 7) != t.end())
    {
        cout << "Running file_stream_write_seq ..." << endl;
        secs = file_stream_write_seq(blob.size(), nbr_of_blobs, path + "/file_stream_write_seq" + extension);
        print_result(secs, "file_stream_write_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 8) != t.end())
    {
        cout << "Running c_style_io_write_seq ..." << endl;
        secs = c_style_io_write_seq(blob.size(), nbr_of_blobs, path + "/c_style_io_write_seq" + extension);
        print_result(secs, "c_style_io_write_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 9) != t.end())
    {
        cout << "Running file_stream_read_seq ..." << endl;
        secs = file_stream_read_seq(blob.size(), nbr_of_blobs, path + "/file_stream_write_seq" + extension);
        print_result(secs, "file_stream_read_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 10) != t.end())
    {
        cout << "Running c_style_io_read_seq ..." << endl;
        secs = c_style_io_read_seq(blob.size(), nbr_of_blobs, path + "/c_style_io_write_seq" + extension);
        print_result(secs, "c_style_io_read_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 11) != t.end())
    {
        cout << "Running hdf5_write ..." << endl;
        secs = hdf5_write(blob, nbr_of_blobs, path + "/hdf5_write" + extension);
        print_result(secs, "hdf5_write", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 12) != t.end())
    {
        cout << "Running hdf5_read ..." << endl;
        secs = hdf5_read(blob, nbr_of_blobs, path + "/hdf5_write" + extension);
        print_result(secs, "hdf5_read", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 13) != t.end())
    {
        cout << "Running hdf5_write_seq ..." << endl;
        secs = hdf5_write_seq(blob.size(), nbr_of_blobs, path + "/hdf5_write_seq" + extension);
        print_result(secs, "hdf5_write_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 14) != t.end())
    {
        cout << "Running mio_write ..." << endl;
        secs = mio_write(blob, nbr_of_blobs, path + "/mio_write" + extension);
        print_result(secs, "mio_write", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 15) != t.end())
    {
        cout << "Running mio_read ..." << endl;
        secs = mio_read(blob, nbr_of_blobs, path + "/mio_write" + extension);
        print_result(secs, "mio_read", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 16) != t.end())
    {
        cout << "Running mio_write_seq ..." << endl;
        secs = mio_write_seq(blob.size(), nbr_of_blobs, path + "/mio_write_seq" + extension);
        print_result(secs, "mio_write_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 17) != t.end())
    {
        cout << "Running mio_read_seq ..." << endl;
        secs = mio_read_seq(blob.size(), nbr_of_blobs, path + "/mio_write_seq" + extension);
        print_result(secs, "mio_read_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 18) != t.end())
    {
        cout << "Running cereal_write_seq ..." << endl;
        secs = cereal_write_seq(blob.size(), nbr_of_blobs, path + "/cereal_write_seq" + extension);
        print_result(secs, "cereal_write_seq", blob.size(), nbr_of_blobs);
    }

    if (t.empty() || find(t.begin(), t.end(), 19) != t.end())
    {
        cout << "Running cereal_read_seq ..." << endl;
        secs = cereal_read_seq(blob.size(), nbr_of_blobs, path + "/cereal_write_seq" + extension);
        print_result(secs, "cereal_read_seq", blob.size(), nbr_of_blobs);
    }

    timer.stop();
    cout << endl;

    spdlog::info("Total time: {}s", timer.elapsedSeconds());

    return 0;
}