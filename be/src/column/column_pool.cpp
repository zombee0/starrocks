#include "column/column_pool.h"

namespace starrocks::vectorized {

#define SPEC_RETURN_TO_POOL_IMPL(ColumnType) \
template <> \
inline void return_column<ColumnType> (ColumnType* ptr, size_t chunk_size) { \
    static_assert(InList<ColumnPool<ColumnType>, ColumnPoolList>::value, "Cannot use column pool"); \
    ColumnPool<ColumnType>::singleton()->return_column(ptr, chunk_size); \
}

SPEC_RETURN_TO_POOL_IMPL(Int8Column); 
SPEC_RETURN_TO_POOL_IMPL(UInt8Column); 
SPEC_RETURN_TO_POOL_IMPL(Int16Column); 
SPEC_RETURN_TO_POOL_IMPL(Int32Column);
SPEC_RETURN_TO_POOL_IMPL(UInt32Column); 
SPEC_RETURN_TO_POOL_IMPL(Int64Column); 
SPEC_RETURN_TO_POOL_IMPL(Int128Column); 
SPEC_RETURN_TO_POOL_IMPL(FloatColumn);
SPEC_RETURN_TO_POOL_IMPL(DoubleColumn); 
SPEC_RETURN_TO_POOL_IMPL(BinaryColumn); 
SPEC_RETURN_TO_POOL_IMPL(DateColumn);
SPEC_RETURN_TO_POOL_IMPL(TimestampColumn); 
SPEC_RETURN_TO_POOL_IMPL(DecimalColumn); 
SPEC_RETURN_TO_POOL_IMPL(Decimal32Column);
SPEC_RETURN_TO_POOL_IMPL(Decimal64Column); 
SPEC_RETURN_TO_POOL_IMPL(Decimal128Column);

}