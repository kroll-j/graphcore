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
inline std::map<std::string, std::string>
parse_proc_self(const char *fname)
{
    const char *columns[]= { "size", "resident", "share", "text", "lib", "data", "dt" };
    std::map<std::string, std::string> ret;
    std::ifstream f(fname);
    if(f.good())
    {
        for(auto col: columns)
            f >> ret[col];
    }
    return ret;
}

inline long getRSSBytes()
{
    return std::stol(parse_proc_self("/proc/self/statm")["resident"]) * SYSTEMPAGESIZE;
}

inline long getVirtBytes()
{
    return std::stol(parse_proc_self("/proc/self/statm")["size"]);
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
