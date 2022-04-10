#pragma once

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <initializer_list>

namespace starrocks {

namespace vectorized {

template <typename Derived>
class COW : public boost::intrusive_ref_counter<Derived> {
private:
    Derived *derived() { return static_cast<Derived *>(this); }
    const Derived *derived() const { return static_cast<const Derived *>(this); }

protected:
    template <typename T>
    class mutable_ptr: public boost::intrusive_ptr<T> {
    private:
        using Base = boost::intrusive_ptr<T>;

        template <typename> friend class COW;
        template <typename, typename, typename> friend class ColumnFactory;

        explicit mutable_ptr(T *ptr): Base(ptr) {}
    public:
        mutable_ptr(const mutable_ptr &) = delete;
        mutable_ptr & operator=(const mutable_ptr &) = delete;

        mutable_ptr(mutable_ptr &&) = default;
        mutable_ptr & operator=(mutable_ptr &&) = default;

        template <typename U>
        mutable_ptr(mutable_ptr<U> &&other) : Base(std::move(other)) {}

        mutable_ptr() = default;
        mutable_ptr(std::nullptr_t) {}
    };
public:
    using MutablePtr = mutable_ptr<Derived>;

protected:
    template <typename T>
    class immutable_ptr: public boost::intrusive_ptr<T> {
    private:
        using Base = boost::intrusive_ptr<T>;

        template <typename> friend class COW;
        template <typename, typename, typename> friend class ColumnFactory;
        friend class ColumnHelper;

        explicit immutable_ptr(T *ptr): Base(ptr) {}
    public:
        immutable_ptr(const immutable_ptr &) = default;
        immutable_ptr & operator=(const immutable_ptr &) = default;

        template <typename U>
        immutable_ptr(const immutable_ptr<U> &other): Base(other) {}

        immutable_ptr(immutable_ptr &&) = default;
        immutable_ptr & operator=(immutable_ptr &&) = default;

        template <typename U>
        immutable_ptr(immutable_ptr<U> &&other): Base(std::move(other)) {}

        template <typename U>
        immutable_ptr(mutable_ptr<U> &&other): Base(std::move(other)) {}

        template <typename U>
        immutable_ptr(mutable_ptr<U> &other) = delete;

        immutable_ptr() = default;
        immutable_ptr(std::nullptr_t) {}
    };
public:
    using Ptr = immutable_ptr<Derived>;

    Ptr getPtr() const { return static_cast<Ptr>(derived()); }
    MutablePtr getPtr() { return static_cast<MutablePtr>(derived()); }

protected:
    MutablePtr shallowMutate() const {
        if (this->use_count() > 1) {
            return derived() -> clone();
        } else {
            return assumeMutable();
        }
    }

public:
    static MutablePtr mutate(Ptr ptr) {
        return ptr->shallowMutate();
    }

    MutablePtr assumeMutable() const {
        return const_cast<COW *>(this)->getPtr();
    }

};

}
}

