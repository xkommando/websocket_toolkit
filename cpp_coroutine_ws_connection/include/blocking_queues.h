
#pragma once

#include <cassert>
#include <optional>
#include <functional>
#include <list>
#include <deque>
#include <mutex>
#include <queue>
#include <condition_variable>

namespace ws_toolkit {

namespace detail_ {

template<typename Impl, typename ValueT>
class BaseBlockingQueue {

    BaseBlockingQueue(const BaseBlockingQueue &) = delete;
    BaseBlockingQueue(BaseBlockingQueue &&) = delete;
    BaseBlockingQueue &operator=(const BaseBlockingQueue &) = delete;
    BaseBlockingQueue &operator=(BaseBlockingQueue &&) = delete;

 public:
    BaseBlockingQueue(size_t size_limit)
        : size_limit_(size_limit) {}

    using value_type = ValueT;

    /**
     * emplace to the back of the queue, wait for space to become available if queue is full
     */
    template<typename ... Args>
    void put(Args &&... args) {
        std::unique_lock<std::mutex> lock(mutex_);
        while (static_cast<Impl *>(this)->sizeImpl() >= size_limit_) {
            signal_not_full_.wait(lock);
        }
        static_cast<Impl *>(this)->emplaceBackImpl(std::forward<Args>(args) ...);
        signal_not_empty_.notify_one();
    }

    /**
     * emplace to the back of the queue if there is space available and the new element can be emplaced immediately
     * , otherwise return false
     */
    template<typename ... Args>
    bool offer(Args &&... args) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (static_cast<Impl *>(this)->sizeImpl() >= size_limit_) {
            return false;
        }
        static_cast<Impl *>(this)->emplaceBackImpl(std::forward<Args>(args) ...);
        signal_not_empty_.notify_one();
        return true;
    }

    /**
     * emplace to the back of the queue, waiting up to the specific wait time for space to become available
     */
    template<typename DurationT, typename ... Args>
    bool offer(DurationT duration, Args &&... args) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (static_cast<Impl *>(this)->sizeImpl() >= size_limit_) {
            signal_not_full_.wait_for(lock, duration);
        }
        if (static_cast<Impl *>(this)->sizeImpl() >= size_limit_) {
            return false;
        }
        static_cast<Impl *>(this)->emplaceBackImpl(std::forward<Args>(args) ...);
        signal_not_empty_.notify_one();
        return true;
    }

    /**
     * retrieve and remove the head of the queue, wait if necessary until an element becomes available
     */
    value_type take() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (static_cast<Impl *>(this)->isEmptyImpl()) {
            signal_not_empty_.wait(lock);
        }
        value_type e = static_cast<Impl *>(this)->popFrontImpl();
        signal_not_full_.notify_one();
        return e;
    }

    /**
     * retrieve and remove the head of the queue,
     * or returns std::nullopt if the queue is empty.
     */
    std::optional<value_type> poll() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (static_cast<Impl *>(this)->isEmptyImpl()) {
            return std::nullopt;
        }
        value_type e = static_cast<Impl *>(this)->popFrontImpl();
        signal_not_full_.notify_one();
        return e;
    }

    /**
     * retrieve and remove the head of the queue, waiting up to the
     * specified wait time if necessary for an element to become available
     */
    template<typename DurationT>
    std::optional<value_type> poll(DurationT duration) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (static_cast<Impl *>(this)->isEmptyImpl()) {
            signal_not_empty_.wait_for(lock, duration);
        }
        if (static_cast<Impl *>(this)->isEmptyImpl()) {
            return std::nullopt;
        }
        value_type e = static_cast<Impl *>(this)->popFrontImpl();
        signal_not_full_.notify_one();
        return e;
    }

    /**
     * remove all available elements from the queue and add them to the out iterator
     * , this function is more efficient than repeatedly polling from queue.
     */
    template<typename OutIterator>
    void drainTo(OutIterator out_iterator) {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!static_cast<Impl *>(this)->isEmptyImpl()) {
            *out_iterator++ = static_cast<Impl *>(this)->pop_front();
        }
        signal_not_full_.notify_all();
    }

    bool isEmpty() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return static_cast<Impl *>(this)->isEmptyImpl();
    }

    size_t size() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return static_cast<Impl *>(this)->sizeImpl();
    }

    size_t sizeLimit() const noexcept {
        return size_limit_;
    }

 protected:
    std::mutex mutex_;
    const size_t size_limit_;
    std::condition_variable signal_not_full_;
    std::condition_variable signal_not_empty_;
};

}

template<typename ValueT>
class BlockingDeQueue : public detail_::BaseBlockingQueue<BlockingDeQueue<ValueT>, ValueT> {
 public:
    using value_type = ValueT;
    using BaseClass = typename detail_::BaseBlockingQueue<BlockingDeQueue<ValueT>, ValueT>;

    BlockingDeQueue(size_t size_limit)
        : BaseClass(size_limit) {}

    BlockingDeQueue()
        : BaseClass(std::numeric_limits<uint32_t>::max()) {}

 private:
    friend BaseClass;

    template<typename ... Args>
    void emplaceBackImpl(Args &&... args) {
        queue_.emplace_back(ValueT(std::forward<Args>(args) ...));
    }

    ValueT popFrontImpl() {
        ValueT e = std::move(queue_.front());
        queue_.pop_front();
        return e;
    }

    bool isEmptyImpl() const noexcept {
        return queue_.empty();
    }

    bool sizeImpl() const noexcept {
        return queue_.size();
    }
    std::deque<ValueT> queue_;
};

template<typename ValueT>
class BlockingList : public detail_::BaseBlockingQueue<BlockingList<ValueT>, ValueT> {
 public:
    using value_type = ValueT;
    using BaseClass = typename detail_::BaseBlockingQueue<BlockingList<ValueT>, ValueT>;

    BlockingList(size_t size_limit)
        : BaseClass(size_limit) {}

    BlockingList()
        : BaseClass(std::numeric_limits<uint32_t>::max()) {}

 private:
    friend BaseClass;

    template<typename ... Args>
    void emplaceBackImpl(Args &&... args) {
        queue_.emplace_back(ValueT(std::forward<Args>(args) ...));
    }

    ValueT popFrontImpl() {
        ValueT e = std::move(queue_.front());
        queue_.pop_front();
        return e;
    }

    bool isEmptyImpl() const noexcept {
        return queue_.empty();
    }

    bool sizeImpl() const noexcept {
        return queue_.size();
    }
    std::list<ValueT> queue_;
};

namespace detail_ {

template<typename Func, typename ValueT>
concept DoWithBuffer = requires(Func f, ValueT *addr) {
	f(addr);
};

}

template<typename ValueT, typename Allocator = std::allocator<ValueT> >
class BlockingRingBuffer {

    BlockingRingBuffer(const BlockingRingBuffer &) = delete;

    BlockingRingBuffer(BlockingRingBuffer &&) = delete;

    BlockingRingBuffer &operator=(const BlockingRingBuffer &) = delete;

    BlockingRingBuffer &operator=(BlockingRingBuffer &&) = delete;

 public:
    using allocator_type = Allocator;
    using value_type = ValueT;

    using InitFunc = std::function<void(ValueT *, size_t)>;
    using CleanFunc = std::function<void(ValueT *, size_t)>;

    BlockingRingBuffer(size_t ring_size, InitFunc init, CleanFunc clean)
        : ring_size_(ring_size), size_(0), clean_(clean) {
        if (!std::is_trivially_destructible<ValueT>::value
            && !clean_) {
            throw std::runtime_error("non-trivially-destructible object requires a cleaner");
        }
        buffer_start_ = allocator_.allocate(ring_size_);
        buffer_end_ = buffer_start_ + ring_size_;
        head_ = buffer_start_;
        tail_ = buffer_start_;
        for (size_t i = 0; i < capacity(); ++i) {
            init(buffer_start_ + i, i);
        }
    }

    BlockingRingBuffer(size_t ring_size)
        : BlockingRingBuffer(ring_size,
                             [](ValueT *ptr, size_t i) {
                               std::construct_at<ValueT>(ptr);
                             }, [](ValueT *ptr, size_t i) {
          std::destroy_at<ValueT>(ptr);
        }) {}

    ~BlockingRingBuffer() {
        if (buffer_start_ == nullptr)
            return;
        if constexpr(!std::is_trivially_destructible<ValueT>::value) {
            for (size_t i = 0; i < capacity(); ++i) {
                clean_(buffer_start_ + i, i);
            }
        }
        allocator_.deallocate(buffer_start_, ring_size_);
        buffer_start_ = nullptr;
    }

    template<typename Producer> requires(detail_::DoWithBuffer<Producer, ValueT>)
    bool offer(Producer do_with_buffer) {
        std::unique_lock<std::mutex> lock(mutex_);
        while (size_ >= capacity()) {
            signal_not_full_.wait(lock);
        }
        std::exception_ptr pub_except = nullptr;
        if constexpr(std::is_nothrow_invocable<Producer, ValueT *>::value) {
            do_with_buffer(head_);
        } else {
            try {
                do_with_buffer(head_);
            } catch (...) {
                pub_except = std::current_exception();
            }
        }
        if (pub_except != nullptr) {
            std::rethrow_exception(pub_except);
        } else {
            size_++;
            head_++;
            if (head_ == buffer_end_) {
                head_ = buffer_start_;
            }
            signal_not_empty_.notify_one();
            return true;
        }
    }

    template<typename DurationT, typename Producer> requires(detail_::DoWithBuffer<Producer, ValueT>)
    bool offer(DurationT max_wait, Producer do_with_buffer) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (size_ >= capacity()) {
            signal_not_full_.wait_for(lock, max_wait);
        }
        if (size_ >= capacity()) {
            return false;
        }
        std::exception_ptr pub_except = nullptr;
        if constexpr(std::is_nothrow_invocable<Producer, ValueT *>::value) {
            do_with_buffer(head_);
        } else {
            try {
                do_with_buffer(head_);
            } catch (...) {
                pub_except = std::current_exception();
            }
        }
        if (pub_except != nullptr) {
            std::rethrow_exception(pub_except);
        } else {
            size_++;
            head_++;
            if (head_ == buffer_end_) {
                head_ = buffer_start_;
            }
            signal_not_empty_.notify_one();
            return true;
        }
    }

    template<typename Consumer> requires(detail_::DoWithBuffer<Consumer, ValueT>)
    bool poll(Consumer do_with_buffer) {
        std::unique_lock<std::mutex> lock(mutex_);
        while (size_ == 0) {
            signal_not_empty_.wait(lock);
        }
        std::exception_ptr consumer_except = nullptr;
        if constexpr(std::is_nothrow_invocable<Consumer, ValueT *>::value) {
            do_with_buffer(tail_);
        } else {
            try {
                do_with_buffer(tail_);
            } catch (...) {
                consumer_except = std::current_exception();
            }
        }
        if (consumer_except != nullptr) {
            std::rethrow_exception(consumer_except);
        } else {
            size_--;
            tail_++;
            if (tail_ == buffer_end_) {
                tail_ = buffer_start_;
            }
            signal_not_full_.notify_one();
            return true;
        }
    }

    template<typename DurationT, typename Consumer> requires(detail_::DoWithBuffer<Consumer, ValueT>)
    bool poll(DurationT max_wait, Consumer do_with_buffer) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (size_ == 0) {
            signal_not_empty_.wait_for(lock, max_wait);
        }
        if (size_ == 0) {
            return false;
        }
        std::exception_ptr consumer_except = nullptr;
        if constexpr(std::is_nothrow_invocable<Consumer, ValueT *>::value) {
            do_with_buffer(tail_);
        } else {
            try {
                do_with_buffer(tail_);
            } catch (...) {
                consumer_except = std::current_exception();
            }
        }
        if (consumer_except != nullptr) {
            std::rethrow_exception(consumer_except);
        } else {
            size_--;
            tail_++;
            if (tail_ == buffer_end_) {
                tail_ = buffer_start_;
            }
            signal_not_full_.notify_one();
            return true;
        }
    }

private:
    std::mutex mutex_;

    allocator_type allocator_;
    size_t ring_size_;
    size_t size_;
    ValueT *buffer_start_;
    ValueT *buffer_end_;
    CleanFunc clean_;
#pragma GCC diagnostic ignored "-Winterference-size"
    alignas(std::hardware_destructive_interference_size) ValueT *head_;
    std::condition_variable signal_not_empty_;

    alignas(std::hardware_destructive_interference_size) ValueT *tail_;
    std::condition_variable signal_not_full_;
#pragma GCC diagnostic pop

 public:
    size_t size() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return size_;
    }

    bool isEmpty() { return size() == 0; }
    size_t capacity() const noexcept { return buffer_end_ - buffer_start_; }

    allocator_type &allocator() noexcept { return allocator_; }
    const allocator_type &allocator() const noexcept { return allocator_; }
};

/**
 * single-producer-multi-consumer queue,
 * can be used to implement multicast patterns
 *
 * todo: add/remove consumers on the fly
 */
template<typename ValueT, typename Allocator = std::allocator<ValueT> >
class SpmcQueue {

    SpmcQueue(const SpmcQueue &) = delete;
    SpmcQueue(SpmcQueue &&) = delete;
    SpmcQueue &operator=(const SpmcQueue &) = delete;
    SpmcQueue &operator=(SpmcQueue &&) = delete;

 public:
    using allocator_type = Allocator;
    using value_type = ValueT;

    SpmcQueue(size_t ring_size)
        : capacity_(ring_size)
        , next_available_index_(0)
        , last_published_index_(-1)
        , mask_(ring_size) {
        buffer_ = allocator_.allocate(capacity_);
    }
    ~SpmcQueue() {
        allocator_.deallocate(buffer_, capacity_);
    }

    class BufferIterator {
        uint64_t virtual_index_;
        SpmcQueue *q_;
     public:
        explicit BufferIterator(SpmcQueue *q, uint64_t i)
            : virtual_index_(i), q_(q) {}

        using iterator_category = std::random_access_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = ValueT;
        using pointer = ValueT *;
        using const_pointer = const ValueT *;
        using reference = ValueT &;
        using const_reference = const ValueT &;

        inline ValueT &operator*() noexcept { return q_->at(virtual_index_); }
        inline ValueT *operator->() noexcept { return &(q_->at(virtual_index_)); }
        inline const ValueT &operator*() const noexcept { return q_->at(virtual_index_); }
        inline const ValueT *operator->() const noexcept { return &(q_->at(virtual_index_)); }

        inline BufferIterator operator++(int) noexcept {
            auto tmp = *this;
            virtual_index_++;
            return tmp;
        }
        inline BufferIterator &operator++() noexcept {
            virtual_index_++;
            return *this;
        }
        inline BufferIterator operator--(int) noexcept {
            auto tmp = *this;
            virtual_index_--;
            return tmp;
        }
        inline BufferIterator &operator--() noexcept {
            virtual_index_--;
            return *this;
        }
        inline BufferIterator &operator+=(difference_type s) noexcept {
            virtual_index_ += s;
            return *this;
        }
        inline BufferIterator &operator-=(difference_type s) noexcept {
            virtual_index_ -= s;
            return *this;
        }
        friend inline bool operator==(const BufferIterator &l, const BufferIterator &r) noexcept {
            return l.q_ == r.q_ && l.virtual_index_ == r.virtual_index_;
        }
        friend inline bool operator!=(const BufferIterator &l, const BufferIterator &r) noexcept {
            return !(l == r);
        }
        // ignore the 'q_'
        friend bool operator<(const BufferIterator &l, const BufferIterator &r) noexcept {
            return l.virtual_index_ < r.virtual_index_;
        }
        friend bool operator>(const BufferIterator &l, const BufferIterator &r) noexcept {
            return l.virtual_index_ > r.virtual_index_;
        }
        friend bool operator<=(const BufferIterator &l, const BufferIterator &r) noexcept {
            return l.virtual_index_ <= r.virtual_index_;
        }
        friend bool operator>=(const BufferIterator &l, const BufferIterator &r) noexcept {
            return l.virtual_index_ >= r.virtual_index_;
        }
        friend BufferIterator operator+(const BufferIterator &l, difference_type s) noexcept {
            auto tmp = l;
            tmp.virtual_index_ += s;
            return tmp;
        }
        friend BufferIterator operator-(const BufferIterator &l, difference_type s) noexcept {
            auto tmp = l;
            tmp.virtual_index_ -= s;
            return tmp;
        }
        friend difference_type operator-(const BufferIterator &l, const BufferIterator &r) noexcept {
            return l.virtual_index_ - r.virtual_index_;
        }
    };

    class BufferRange {
        SpmcQueue *q_;
        BufferRange(uint64_t idx_begin, uint64_t idx_end, SpmcQueue *q)
            : q_(q), begin_index(idx_begin), end_index(idx_end) {}
        friend SpmcQueue;
     public:
        const uint64_t begin_index;
        const uint64_t end_index;
        BufferIterator begin() const noexcept { return BufferIterator{q_, begin_index}; }
        BufferIterator end() const noexcept { return BufferIterator{q_, end_index}; }
        const BufferIterator cbegin() const noexcept { return BufferIterator{q_, begin_index}; }
        const BufferIterator cend() const noexcept { return BufferIterator{q_, end_index}; }
        size_t count() const noexcept { return end_index - begin_index; }
        bool empty() const noexcept { return begin_index == end_index; }
    };

    class ConsumerHandler {
#pragma GCC diagnostic ignored "-Winterference-size"
        alignas(std::hardware_destructive_interference_size) std::atomic_uint64_t last_read_index_;
#pragma GCC diagnostic pop
        SpmcQueue *q_;
        friend SpmcQueue;
     public:
        explicit ConsumerHandler(uint64_t start_index, SpmcQueue *q)
            : last_read_index_(start_index), q_(q) {}

        /**
         * poll at least 'count' number of items
         */
        BufferRange poll(size_t count) {
			assert(count > 0);
            auto last_idx = last_read_index_.load(std::memory_order_acquire);
            auto req_idx = last_idx + count;
            auto head = q_->last_published_index_.load(std::memory_order_acquire);
            // overflow -> negative
            int64_t diff = static_cast<int64_t>(head - req_idx);
            if (diff < 0) {
                std::unique_lock<std::mutex> lock(q_->mutex_);
                q_->cv_.wait(lock, [&]() {
                  last_idx = last_read_index_.load(std::memory_order_acquire);
                  req_idx = last_idx + count;
                  head = q_->last_published_index_.load(std::memory_order_acquire);
				  diff = static_cast<int64_t>(head - req_idx);
                  return diff >= 0;
                });
            }
            return BufferRange(last_idx + 1, head + 1, q_);
        }
        /**
         * * @return an empty range if timed out
         */
        template<typename Rep, typename Period>
        BufferRange poll(size_t count, const std::chrono::duration<Rep, Period>& timeout) {
            assert(count > 0);
            auto last_idx = last_read_index_.load(std::memory_order_acquire);
            auto req_idx = last_idx + count;
            auto head = q_->last_published_index_.load(std::memory_order_acquire);
            // overflow -> negative
            int64_t diff = static_cast<int64_t>(head - req_idx);
            if (diff < 0) {
                std::unique_lock<std::mutex> lock(q_->mutex_);
                q_->cv_.wait_for(lock, timeout, [&]() {
                  last_idx = last_read_index_.load(std::memory_order_acquire);
                  req_idx = last_idx + count;
                  head = q_->last_published_index_.load(std::memory_order_acquire);
                  diff = static_cast<int64_t>(head - req_idx);
                  return diff >= 0;
                });
                if (diff < 0) {
                    return BufferRange(last_idx, last_idx, q_);
                }
            }
            return BufferRange(last_idx + 1, head + 1, q_);
        }

        /**
         * peek new data and return all of them, otherwise returns an empty range
         * will not block
         */
        BufferRange peek() {
            auto last_idx = last_read_index_.load(std::memory_order_acquire);
            auto req_idx = last_idx + 1;
            auto head = q_->last_published_index_.load(std::memory_order_acquire);
            int64_t diff = static_cast<int64_t>(head - req_idx);
            if (diff >= 0) {
                return BufferRange(last_idx + 1, head + 1, q_);
            } else {
                return BufferRange(last_idx, last_idx, q_);
            }
        }
        void markConsumed(const BufferRange &buf_range) {
            markConsumed(buf_range.end_index - 1);
        }
        void markConsumed(uint64_t last_index) {
            last_read_index_.store(last_index, std::memory_order_release);
            q_->notifyAll();
        }
    };

    uint64_t nextAvailableIndex() {
        const auto target = static_cast<uint64_t>(next_available_index_ - capacity_);
        auto min_offset = minConsumerOffsetAfter(target);
        if (min_offset < 0) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [&]() {
              min_offset = minConsumerOffsetAfter(target);
              return min_offset >= 0;
            });
        }
        return next_available_index_++;
    }
    template<typename Rep, typename Period>
    std::optional<uint64_t> nextAvailableIndex(const std::chrono::duration<Rep, Period>& timeout) {
        const auto target = static_cast<uint64_t>(next_available_index_ - capacity_);
        auto min_offset = minConsumerOffsetAfter(target);
        if (min_offset < 0) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, timeout, [&]() {
              min_offset = minConsumerOffsetAfter(target);
              return min_offset >= 0;
            });
            if (min_offset < 0) {
                return std::nullopt;
            }
        }
        return next_available_index_++;
    }
    void publish(uint64_t index) {
        // assert index > last_published_index_
        last_published_index_.store(index, std::memory_order_release);
        notifyAll();
    }
    /**
     * get a range that has at least 'count' number of empty slots
     * 'next_available_index_' is set to the end of the returned range
     * thus the publisher must publish to all slots in this range
     */
    BufferRange getAvailableRange(size_t count) {
        assert(count > 0);
        const auto target = static_cast<uint64_t>(next_available_index_ + (count - 1) - capacity_);
        auto min_offset = minConsumerOffsetAfter(target);
        if (min_offset < 0) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [&]() {
              min_offset = minConsumerOffsetAfter(target);
              return min_offset >= 0;
            });
        }
        uint64_t left = next_available_index_;
        next_available_index_ += count;
        next_available_index_ += min_offset;
        return BufferRange(left, next_available_index_, this);
    }
    /**
     * @return an empty range if timed out
     */
    template<typename Rep, typename Period>
    BufferRange getAvailableRange(size_t count, const std::chrono::duration<Rep, Period>& timeout) {
        assert(count > 0);
        const auto target = static_cast<uint64_t>(next_available_index_ + (count - 1) - capacity_);
        auto min_offset = minConsumerOffsetAfter(target);
        if (min_offset < 0) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, timeout, [&]() {
              min_offset = minConsumerOffsetAfter(target);
              return min_offset >= 0;
            });
            if (min_offset < 0) {
                return BufferRange(next_available_index_ - 1, next_available_index_ - 1, this);
            }
        }
        uint64_t left = next_available_index_;
        next_available_index_ += count;
        next_available_index_ += min_offset;
        return BufferRange(left, next_available_index_, this);
    }
    /**
     * check if there is empty slots available and return all of them, otherwise return an empty range.
     * will not block
     * publisher must publish to all slots in this range
     */
    BufferRange tryGetAvailableRange() {
        const auto target = static_cast<uint64_t>(next_available_index_ - capacity_);
        auto min_offset = minConsumerOffsetAfter(target);
        if (min_offset < 0) {
            return BufferRange(next_available_index_ - 1, next_available_index_ - 1, this);
        } else {
            uint64_t left = next_available_index_;
            next_available_index_++;
            next_available_index_ += min_offset;
            return BufferRange(left, next_available_index_, this);
        }
    }
    void publish(const BufferRange &buf_range) {
        publish(buf_range.end_index - 1);
    }

    inline ValueT&          operator[](uint64_t index) noexcept { return buffer_[index % mask_]; }
    inline const ValueT&    operator[](uint64_t index) const noexcept { return buffer_[index % mask_]; }
    inline ValueT&          at(uint64_t index) noexcept { return buffer_[index % mask_]; }
    inline const ValueT&    at(uint64_t index) const noexcept { return buffer_[index % mask_]; }

    /**
     * recommend creating all consumers before the consumer threads
     * the queue owns all consumer handlers
    */
    ConsumerHandler *createConsumer() {
        consumers_.emplace_back(last_published_index_.load(std::memory_order_relaxed), this);
        return &consumers_.back();
    }

 private:
    int64_t minConsumerOffsetAfter(uint64_t target) {
        auto ic = consumers_.begin();
        auto read_idx = ic->last_read_index_.load(std::memory_order_acquire);
        ic++;
        // overflow -> negative
        int64_t min_offset = static_cast<int64_t>(read_idx - target);
        while (ic != consumers_.end() && min_offset >= 0) {
            read_idx = ic->last_read_index_.load(std::memory_order_acquire);
            int64_t offset = static_cast<int64_t>(read_idx - target);
            min_offset = std::min(min_offset, offset);
            ic++;
        }
        return min_offset;
    }

    void notifyAll() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.notify_all();
    }

    allocator_type allocator_;
    const size_t capacity_;
    std::mutex mutex_;
    std::condition_variable cv_;

    uint64_t next_available_index_;
    std::atomic_uint64_t last_published_index_;

    ValueT *buffer_;
    const uint64_t mask_;

    std::list<ConsumerHandler> consumers_;
};

}

