#pragma once
#include <chrono>
#include <ratio>

class Timer
{
public:
    Timer() : m_elapsed(0) {}

    void start()
    {
        m_start = std::chrono::high_resolution_clock::now();
    }

    void stop()
    {
        auto now(std::chrono::high_resolution_clock::now());
        m_elapsed += now - m_start;
    }

    void reset()
    {
        m_elapsed = std::chrono::duration<double>(0);
    }

    double elapsedSeconds()
    {
        return m_elapsed.count();
    }

private:
    std::chrono::time_point<std::chrono::steady_clock> m_start;
    std::chrono::duration<double> m_elapsed;
};
