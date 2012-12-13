// This file is part of graphcore
// (c) Wikimedia Deutschland, written by Johannes Kroll in 2011, 2012
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
// 
// this is a special-purpose fixed-size memory allocator intended for 
// use with deque.

#ifndef MMAP_POOL_H
#define MMAP_POOL_H

#include <sys/mman.h>
#include <stdexcept>
#include <bitset>
#include <algorithm>


#ifndef dmsg
#define dmsg(...) 
#endif


#ifndef SYSTEMPAGESIZE
#warning SYSTEMPAGESIZE not defined, assuming 4k
#define SYSTEMPAGESIZE  4096
#endif


class verbose_bad_alloc: public std::bad_alloc
{
    char what_[128];
    public:
        verbose_bad_alloc(const char* w)
        { strncpy(what_, w, sizeof(what_)); what_[sizeof(what_)-1]= 0; dmsg("%s\n", w); }
    
        const char* what() const throw()
        { return what_; }
};


// manages one chunk of memory of CHUNKSIZE bytes in CHUNKSIZE blocks
// does system memory allocation like mmap()
template<size_t CHUNKSIZE, size_t BLOCKSIZE>
struct Model_ChunkManager
{
    enum { ChunkSize= CHUNKSIZE, BlockSize= BLOCKSIZE };
    Model_ChunkManager();
    virtual ~Model_ChunkManager();
    // allocate blocks inside the memory chunk
    void allocate(size_t offset, size_t nblocks);
    // deallocate blocks
    void deallocate(size_t offset, size_t nblocks);
    // get chunk address
    void *address();
};

// handles blockmap allocation/deallocation of chunks in one block
template<typename CHUNKMANAGER>
struct Model_BlockAllocator
{
    CHUNKMANAGER manager;
    Model_BlockAllocator();
    virtual ~Model_BlockAllocator();
    void *allocate();
    void deallocate(void *p);
    size_t blocksLeft();
};

// handles allocation of chunks and interface to allocate/free single blocks
template<typename BLOCKMAPALLOCATOR, size_t MAXCHUNKS>
struct Model_FixedsizeAllocator
{
    BLOCKMAPALLOCATOR *blockmapAllocator[MAXCHUNKS];
    Model_FixedsizeAllocator();
    virtual ~Model_FixedsizeAllocator();
    void *allocate(size_t nblocks);
    void deallocate(void *p, size_t nblocks);
    size_t blocksLeft();
};

/////////////////////


template<size_t CHUNKSIZE, size_t BLOCKSIZE>
class MmapChunkManager
{
    public:
        enum { ChunkSize= CHUNKSIZE, BlockSize= BLOCKSIZE };

        MmapChunkManager()
        {
            static_assert(BLOCKSIZE==SYSTEMPAGESIZE, "this chunk manager requires that block size equals system page size");
            static_assert((CHUNKSIZE/BLOCKSIZE)*BLOCKSIZE == CHUNKSIZE, "chunk size must be divisible by block size");
            mapping= ::mmap(0, CHUNKSIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, -1, 0);
            if(mapping==(void*)-1)
            {
                perror("mmap");
                throw verbose_bad_alloc("mmap failed");
            }
        }
        
        ~MmapChunkManager()
        {
            if(::munmap(mapping, BLOCKSIZE)<0)
            {
                perror("mmap");
                throw verbose_bad_alloc("munmap failed");
            }
        }
        
        void allocate(size_t offset, size_t nblocks) { }

        void deallocate(size_t offset, size_t nblocks) { }

        void *address()
        {
            return mapping;
        }

    private:
        void *mapping;
};


// handles blockmap allocation/deallocation of chunks in one block
template<typename CHUNKMANAGER>
class SimpleBitmapBlockAllocator
{
    public:
        typedef CHUNKMANAGER ChunkManager;
        enum { ChunkSize= CHUNKMANAGER::ChunkSize };
    
        SimpleBitmapBlockAllocator() { }
        
        virtual ~SimpleBitmapBlockAllocator() { }
        
        void *allocate(int nblocks)
        {
            if(nblocks!=1)
                throw verbose_bad_alloc("only single-block allocation supported");
            
            ssize_t blkidx= findFreeBlock();
            if(blkidx<0) throw verbose_bad_alloc("no free block found in chunk");
            
            blockmap[blkidx]= 1;
            manager.allocate(blkidx*CHUNKMANAGER::BlockSize, nblocks);
            return (void*) ((char*)manager.address() + blkidx*CHUNKMANAGER::BlockSize);
        }
        
        void deallocate(void *p, int nblocks)
        {
            if(nblocks!=1)
                throw verbose_bad_alloc("only single-block allocation supported");
            ssize_t idx= ((char*)p - (char*)manager.address()) / CHUNKMANAGER::BlockSize;
            manager.deallocate(idx*CHUNKMANAGER::BlockSize, nblocks);
            blockmap[idx]= 0;
        }
        
        size_t blocksLeft()
        {
            return blockmap.size() - blockmap.count();
        }
        
        size_t blocksTotal()
        {
            return blockmap.size();
        }
        
        void *address()
        {
            return manager.address();
        }
    
    protected:
        CHUNKMANAGER manager;
        typedef std::bitset<CHUNKMANAGER::ChunkSize/CHUNKMANAGER::BlockSize> blockmap_t;
        blockmap_t blockmap;

        // returns index of free block or -1 if none found
        virtual ssize_t findFreeBlock()
        {
            // naive version: scan through the whole chunk sequentially
            for(size_t i= 0; i<blockmap.size(); i++)
                if(!blockmap[i]) return i;
            return -1;
        }
};


// handles blockmap allocation/deallocation of blocks in one chunks
template<typename CHUNKMANAGER>
class SubmipBlockAllocator: public SimpleBitmapBlockAllocator<CHUNKMANAGER>
{
    using SimpleBitmapBlockAllocator<CHUNKMANAGER>::blockmap;
    using SimpleBitmapBlockAllocator<CHUNKMANAGER>::manager;
    
    public:
        typedef CHUNKMANAGER ChunkManager;
        enum { ChunkSize= CHUNKMANAGER::ChunkSize };
        enum { SubdivSize= 64, BlocksTotal= CHUNKMANAGER::ChunkSize/CHUNKMANAGER::BlockSize, Subdivs= BlocksTotal/SubdivSize };
        
        SubmipBlockAllocator()
        {
            static_assert( SubdivSize*Subdivs==CHUNKMANAGER::ChunkSize/CHUNKMANAGER::BlockSize, "chunk size must be divisible by subdiv size" );
            static_assert( (Subdivs & (Subdivs-1)) == 0, "number of subdivs must be a power of 2" );
            initSubdivMipmaps(subdivMipmaps, 0, BlocksTotal);
            //~ listSubmips(); exit(1);
        }
        
        virtual ~SubmipBlockAllocator()
        {
        }

        void *allocate(int nblocks)
        {
            if(nblocks!=1)
                throw verbose_bad_alloc("only single-block allocation supported");
            
            ssize_t blkidx= findFreeBlock();
            if(blkidx<0)
            {
                dmsg("found no free block, blockmap.size()-blockmap.count()=%u, mips[0]=%u\n", blockmap.size()-blockmap.count(), subdivMipmaps[0]);
                throw verbose_bad_alloc("no free block in chunk");
            }
            
            blockmap[blkidx]= 1;
            //~ checkSubdivs();
            manager.allocate(blkidx*CHUNKMANAGER::BlockSize, nblocks);
            return (void*) ((char*)manager.address() + blkidx*CHUNKMANAGER::BlockSize);
        }
        
        void deallocate(void *p, int nblocks)
        {
            if(nblocks!=1)
                throw verbose_bad_alloc("only single-block allocation supported");
            ssize_t idx= ((char*)p - (char*)manager.address()) / CHUNKMANAGER::BlockSize;
            blockmap[idx]= 0;
            freeSubmips(idx / SubdivSize);
            manager.deallocate(idx*CHUNKMANAGER::BlockSize, nblocks);
            //~ checkSubdivs();
        }
        
        size_t blocksLeft()
        {
            //~ return blockmap.size() - blockmap.count();
            return subdivMipmaps[0];
        }
        

    protected:
        uint32_t subdivMipmaps[Subdivs*2-1];
    
        void initSubdivMipmaps(uint32_t *mip, int depth, uint32_t nfree)
        {
            for(int i= 0; i<(1<<depth); i++)
                mip[i]= nfree;
            if((1<<depth)<Subdivs)
                initSubdivMipmaps(mip+(1<<depth), depth+1, nfree>>1);
            checkSubdivs();
        }
        
        ssize_t findFreeSubdiv()
        {
            uint32_t *mip= subdivMipmaps;
            uint32_t idx= 0;
            for(uint32_t depth= 0; (1<<depth)<=Subdivs; depth++)
            {
                //~ dmsg("subdiv %2d: depth=%d, idx=%2d\n", subdiv, depth, idx);
                idx<<= 1;
                if(!mip[idx]) idx++;
                if(!mip[idx]) return -1;
                mip+= (1<<depth);
            }
            return idx;
            
            //~ uint32_t *mip= subdivMipmaps + Subdivs-1;
            //~ for(ssize_t i= 0; i<Subdivs; i++)
                //~ if(mip[i]>0) return i;
            //~ return -1;
        }
        
        void freeSubmips(uint32_t subdiv)
        {
            walkSubmips(subdiv, [](uint32_t *mip, int idx, uint32_t depth) { mip[idx]++; });

            return;     // xxx
            uint32_t *mip= subdivMipmaps;
            uint32_t div= Subdivs;
            for(uint32_t depth= 0; (1<<depth)<=Subdivs; depth++, div>>= 1)
            {
                int idx= subdiv / div;
                dmsg("subdiv %2d: depth=%d, idx=%2d\n", subdiv, depth, idx);
                mip[idx]++;
                mip+= (1<<depth);
            }
            dmsg("done.\n", 0);
        }
        
        template <typename v>
        void walkSubmips(uint32_t subdiv, v visitor)
        {
            if(subdiv>=Subdivs)
            {
                dmsg("subdiv %u >= %u!\n", subdiv, Subdivs);
                __asm__ volatile ("int3");
            }
            uint32_t *mip= subdivMipmaps;
            uint32_t div= Subdivs;
            for(uint32_t depth= 0; (1<<depth)<=Subdivs; depth++, div>>= 1)
            {
                int idx= subdiv / div;
                //~ dmsg("subdiv %2d: depth=%d, idx=%2d\n", subdiv, depth, idx);
                visitor(mip, idx, depth);
                mip+= (1<<depth);
            }
            //~ dmsg("done.\n", 0);
        }
        
        void checkSubdivs()
        {
            for(int i= 0; i<Subdivs; i++)
            {
                size_t sum= 0;
                for(int k= 0; k<SubdivSize; k++)
                    sum+= (blockmap[i*SubdivSize+k]? 0: 1);
                if(sum != subdivMipmaps[Subdivs-1+i])
                {
                    dmsg("subdiv %d doesn't match sum!\n", i);
                    listSubmips();
                    __asm__ volatile ("int3");
                }
            }

        }
        
        void listSubmips()
        {
            uint32_t *mips= subdivMipmaps;
            for(int i= 1; i<=Subdivs; i<<= 1)
            {
                int sz= (Subdivs*8)/i;
                char fmt[32];
                sprintf(fmt, "%%%dd", sz);
                for(int k= 0; k<i; k++)
                    printf(fmt, mips[k]);
                puts("");
                mips+= i;
            }
            
            for(int i= 0; i<Subdivs; i++)
            {
                size_t sum= 0;
                for(int k= 0; k<SubdivSize; k++)
                    sum+= (blockmap[i*SubdivSize+k]? 0: 1);
                printf("%8zu", sum);
            }
            puts("");
        }

        // returns index of free block or -1 if none found
        virtual ssize_t findFreeBlock()
        {
            ssize_t subdiv= findFreeSubdiv(); //0, 0);
            if(subdiv<0) { listSubmips(); return -1; }
            //~ dmsg("free blocks left in subdiv %zd: %u\n", subdiv, subdivMipmaps[Subdivs-1+subdiv]);
            
            walkSubmips(subdiv, [](uint32_t *mip, int idx, uint32_t depth) { mip[idx]--; });
            
            size_t searchBase= subdiv * SubdivSize;
            for(size_t i= searchBase; i<searchBase+SubdivSize; i++)
                if(!blockmap[i]) { /*dmsg("returning %d\n", i);*/ return i; }
            dmsg("*** found no free block in subdiv %d\n", subdiv);
            listSubmips();
            return -1;
        }
        
        
};


template<typename BLOCKALLOCATOR, size_t MAXCHUNKS>
struct SimpleFixedsizeAllocator
{
    public:
        typedef BLOCKALLOCATOR BlockAllocator;
        enum { BlockSize= BlockAllocator::ChunkManager::BlockSize };
        
        SimpleFixedsizeAllocator(): nChunks(0)
        {
            memset(chunks, 0, sizeof(chunks));
            createChunk();
        }
        
        ~SimpleFixedsizeAllocator()
        {
            for(size_t i= 0; i<nChunks; i++)
                delete chunks[i];
        }
        
        void *allocate(size_t nblocks)
        {
            if(nblocks!=1)
                dmsg("nblocks=%d\n", nblocks),
                throw verbose_bad_alloc("only single-block allocation supported");
            for(size_t i= 0; i<nChunks; i++)
                if(chunks[i]->blocksLeft())
                    return chunks[i]->allocate(nblocks);
            return createChunk()->allocate(nblocks);
        }
        
        void deallocate(void *p, size_t nblocks)
        {
            if(nblocks!=1)
                throw verbose_bad_alloc("only single-block deallocation supported");
            ssize_t cidx= findChunkIdx(p);
            if(cidx<0) throw verbose_bad_alloc("deallocate: chunk idx for pointer not found");
            chunks[cidx]->deallocate(p, nblocks);
            if(chunks[cidx]->blocksLeft() - chunks[cidx]->blocksTotal()==0)
                removeChunk(cidx);
        }
        
        //~ size_t blocksLeft()
        //~ {
            //~ size_t left= 0;
            //~ for(size_t i= 0; i<nChunks; i++)
                //~ left+= chunks[i]->blocksLeft();
            //~ return left;
        //~ }
    
    private:
        BLOCKALLOCATOR *chunks[MAXCHUNKS];
        size_t nChunks;
        
        // find index of chunk that contains p, or -1 if not found
        ssize_t findChunkIdx(void *p)
        {
            auto comp= [] (BLOCKALLOCATOR *chunk, void *p) -> bool
            {
                return (char*)chunk->address() + BLOCKALLOCATOR::ChunkSize <= (char*)p;
            };
            BLOCKALLOCATOR **pchunk= lower_bound(chunks+0, chunks+nChunks, p, comp);
            if(pchunk!=chunks+nChunks && (*pchunk)->address() <= p)
                return pchunk - chunks;
            return -1;
        }
        
        BLOCKALLOCATOR *createChunk()
        {
            dmsg("nChunks=%d\n", nChunks);
            
            if(nChunks==MAXCHUNKS)
                throw verbose_bad_alloc("maximum number of chunks exceeded");
            BLOCKALLOCATOR *newchunk= new BLOCKALLOCATOR;
            auto comp= [] (BLOCKALLOCATOR *a, BLOCKALLOCATOR *b) -> bool
            { return a->address() < b->address(); };
            BLOCKALLOCATOR **pchunk= lower_bound(chunks+0, chunks+nChunks, newchunk, comp);
            for(BLOCKALLOCATOR **dst= chunks+nChunks; dst>pchunk; dst--)
                *dst= *(dst-1);
            *pchunk= newchunk;
            ++nChunks;
            
            return newchunk;
        }
        
        void removeChunk(size_t idx)
        {
            dmsg("nChunks=%d\n", nChunks);

            if(idx>=nChunks)
                throw verbose_bad_alloc("removeChunk: idx>=nChunks");
            delete(chunks[idx]);
            --nChunks;
            for(BLOCKALLOCATOR **dst= chunks+idx; dst<chunks+nChunks; dst++)
                *dst= *(dst+1);
        }
};

enum { CHUNKSIZE= 64*1024*1024, MAX_ALLOC= size_t(2)*1024*1024*1024, MAXCHUNKS= MAX_ALLOC/CHUNKSIZE };
typedef MmapChunkManager<CHUNKSIZE, SYSTEMPAGESIZE> ChunkManager;
typedef SimpleBitmapBlockAllocator<ChunkManager> BlockAllocator;
typedef SimpleFixedsizeAllocator<BlockAllocator, MAXCHUNKS> FixedsizeAllocator;

typedef SubmipBlockAllocator<ChunkManager> QuickBlockAllocator;
typedef SimpleFixedsizeAllocator<QuickBlockAllocator, MAXCHUNKS> QuickFixedsizeAllocator;


// stl-compatible allocator
template<typename T, typename FIXEDSIZEALLOCATOR>
class singlepage_std_allocator: public std::allocator<T>
{
    public:
        typedef T value_type;
        typedef T* pointer;
        typedef const T* const_pointer;
        typedef T& reference;
        typedef const T& const_reference;
        typedef std::size_t size_type;
        typedef std::ptrdiff_t difference_type;
        
        typedef FIXEDSIZEALLOCATOR FixedsizeAllocator;

        template< class U > struct rebind { typedef singlepage_std_allocator<U, FixedsizeAllocator> other; };
    
        singlepage_std_allocator() throw() { }

        ~singlepage_std_allocator() throw() { }
        
        singlepage_std_allocator(const singlepage_std_allocator&) throw() { }

        template<typename _Tp1>
        singlepage_std_allocator(const singlepage_std_allocator<_Tp1, FixedsizeAllocator>&) throw() { }

        pointer allocate(size_type n, const void * = 0)
        {
            //~ printf("allocate n=%zu sizeof(T)=%zu typeid(T)=%s\n", n, sizeof(T), typeid(T).name());

            if(n*sizeof(T)!=FixedsizeAllocator::BlockSize)
                return static_cast<T*>(::operator new(n*sizeof(T)));
            
            return (pointer)getAllocator().allocate(1);
        }
        
        void construct(pointer p, const T& val) 
        {
            //~ printf("construct %p\n", p);
            ::new((void *)p) T(val);
        }

        template<typename... Args>
        void construct(pointer p, Args&&... args)
        {
            //~ printf("construct %p ...\n", p);
            ::new((void *)p) T(std::forward<Args>(args)...);
        }

        void destroy(pointer p)
        {
            //~ printf("destroy %p\n", p);
            p->~T();
        }

        void deallocate(pointer p, size_type n)
        {
            //~ printf("deallocate %p\n", p);
            
            if(n*sizeof(T)!=FixedsizeAllocator::BlockSize)
            {
                ::operator delete(p);
                return;
            }
            
            getAllocator().deallocate(p, 1);
        }

        size_type max_size() const throw() 
        {
            //~ return size_t(MAPPING_SIZE) / sizeof(T);
            return FixedsizeAllocator::BlockSize/sizeof(T);
        }

        static FixedsizeAllocator& getAllocator()
        {
            static FixedsizeAllocator *allocatorInstance;
            if(!allocatorInstance) allocatorInstance= new FixedsizeAllocator;
            return *allocatorInstance;
        }
        
    private:
        
};

#endif //MMAP_POOL_H
