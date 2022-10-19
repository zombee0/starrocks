// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/map_subscript_expr.h"

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "common/object_pool.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

class MapSubscriptExpr final : public Expr {
public:
    explicit MapSubscriptExpr(const TExprNode& node) : Expr(node) {}

    MapSubscriptExpr(const MapSubscriptExpr&) = default;
    MapSubscriptExpr(MapSubscriptExpr&&) = default;

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* chunk) override {
        DCHECK_EQ(2, _children.size());
        DCHECK_EQ(_type, _children[0]->type().children[1]);
        ColumnPtr arg0 = _children[0]->evaluate(context, chunk);
        ColumnPtr arg1 = _children[1]->evaluate(context, chunk);
        size_t num_rows = std::max(arg0->size(), arg1->size());
        // No optimization for const column now.
        arg0 = ColumnHelper::unfold_const_column(_children[0]->type(), num_rows, arg0);
        arg1 = ColumnHelper::unfold_const_column(_children[1]->type(), num_rows, arg1);
        auto* map_column = down_cast<MapColumn*>(get_data_column(arg0.get()));
        auto* map_keys = map_column->keys_column().get();
        auto* map_values = map_column->values_column().get();
        DCHECK_EQ(num_rows, arg0->size());
        DCHECK_EQ(num_rows, arg1->size());
        DCHECK_EQ(num_rows + 1, map_column->offsets_column()->size());

        const uint32_t* offsets = map_column->offsets_column()->get_data().data();

        std::vector<uint8_t> null_flags;
        raw::make_room(&null_flags, num_rows);

        if (auto* nullable = dynamic_cast<NullableColumn*>(arg0.get()); nullable != nullptr) {
            const uint8_t* nulls = nullable->null_column()->raw_data();
            for (size_t i = 0; i < num_rows; i++) {
                null_flags[i] = nulls[i];
            }
        }

        // construct selection list.
        std::vector<uint32_t> selection;
        starrocks::raw::make_room(&selection, num_rows);

        uint32_t idx = 0;
        for (size_t i = 0; i < num_rows; i++) {
            bool matched = false;
            for (size_t j = offsets[i]; j < offsets[i + 1]; j++) {
                if (!map_keys->is_null(j) && map_keys->get(j).convert2DatumKey() == arg1->get(i).convert2DatumKey()) {
                    matched = true;
                    selection[i] = j;
                    idx = j;
                    break;
                }
            }
            if (!matched) {
                null_flags[i] = true;
                selection[i] = idx;
            }
        }

        if (map_values->has_null()) {
            auto* nullable_values = down_cast<NullableColumn*>(map_values);
            const uint8_t* nulls = nullable_values->null_column()->raw_data();
            for (size_t i = 0; i < num_rows; i++) {
                null_flags[i] |= nulls[selection[i]];
            }
        }

        auto* map_values_data = get_data_column(map_values);

        ColumnPtr result = map_values_data->clone_empty();
        NullColumnPtr result_null = NullColumn::create();
        result_null->get_data().swap(null_flags);

        if (!map_values_data->empty()) {
            result->append_selective(*map_values_data, selection.data(), 0, num_rows);
        } else {
            result->append_default(num_rows);
        }
        DCHECK_EQ(result_null->size(), result->size());

        return NullableColumn::create(std::move(result), std::move(result_null));
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new MapSubscriptExpr(*this)); }

private:
    Column* get_data_column(Column* column) { return ColumnHelper::get_data_column(column); }
};

Expr* MapSubscriptExprFactory::from_thrift(const TExprNode& node) {
    DCHECK_EQ(TExprNodeType::MAP_SUBSCRIPT_EXPR, node.node_type);
    return new MapSubscriptExpr(node);
}

} // namespace starrocks::vectorized
