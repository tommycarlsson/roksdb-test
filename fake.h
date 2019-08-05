#pragma once
#include <vector>
#include <cereal/types/vector.hpp>

struct Fake
{
    char d1[96];
    char d2[90];
    char d3[96];
    char d4[60];

    template<class Archive>
    void serialize(Archive& archive)
    {
        archive(d1,d2,d3,d4);
    }
};

struct FakeData
{
    std::vector<Fake> fakes;

    template<class Archive>
    void serialize(Archive& archive)
    {
        archive(fakes);
    }
};


