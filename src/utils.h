#ifndef UTILS_H
#define UTILS_H

#include <mutex>
#include <fstream>


#ifdef STDERR_DEBUGGING
#define dmsg(fmt, params...) dprint("%s " fmt, __func__, params)
#else
#define dmsg(x...)
#endif

class ScopedLock    // XXX todo: might use std::lock_guard where supported
{
    ScopedLock(const ScopedLock&);
    void operator= (const ScopedLock&);
    std::mutex& m;
    public: 
        ScopedLock(std::mutex& m_): m(m_)
        {
            m.lock();
        }
        ~ScopedLock()
        {
            m.unlock();
        }
};

#ifdef __linux__
inline std::deque<std::string>
parse_proc_self(const char *fname)
{
    std::deque<std::string> ret;
    std::ifstream f(fname);
    while(f.good())
    {
        std::string col;
        f>>col;
        if(!f.good()) break;
        ret.push_back(col);
    }
    return ret;
}

inline long getRSSBytes()
{
    return std::stol(parse_proc_self("/proc/self/stat")[23]) * SYSTEMPAGESIZE;
}

inline long getVirtBytes()
{
    return std::stol(parse_proc_self("/proc/self/stat")[22]);
}

#else   // non-linux

inline long getRSSBytes()
{
    return -1;
}

inline long getVirtBytes()
{
    return -1;
}

#endif

#endif //UTILS_H
