#pragma once
#include <cereal/types/vector.hpp>

struct Fake
{
    char d1[96];
    char d2[90];
    char d3[96];
    char d4[60];
};

template<class Archive>
void serialize(Archive& archive, Fake& f)
{
    archive(
        f.d1,
        f.d2,
        f.d3,
        f.d4);
}
