#ifndef UTILS_H
#define UTILS_H

#include <mutex>


#ifdef STDERR_DEBUGGING
#define dmsg(fmt, params...) dprint("%s " fmt, __func__, params)
#else
#define dmsg(x...)
#endif


class ScopedLock
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

#endif //UTILS_H
