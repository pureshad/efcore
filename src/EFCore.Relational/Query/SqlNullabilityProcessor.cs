// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Query
{
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
    // Whole API surface is going to change. See issue#20204
    public class SqlNullabilityProcessor
    {
        private readonly List<ColumnExpression> _nonNullableColumns;
        private bool _canCache;

        public SqlNullabilityProcessor(
            [NotNull] ISqlExpressionFactory sqlExpressionFactory,
            [NotNull] IReadOnlyDictionary<string, object> parameterValues,
            bool useRelationalNulls)
        {
            Check.NotNull(sqlExpressionFactory, nameof(sqlExpressionFactory));
            Check.NotNull(parameterValues, nameof(parameterValues));

            SqlExpressionFactory = sqlExpressionFactory;
            ParameterValues = parameterValues;
            UseRelationalNulls = useRelationalNulls;
            _canCache = true;
            _nonNullableColumns = new List<ColumnExpression>();
        }

        protected virtual bool UseRelationalNulls { get; }
        protected virtual ISqlExpressionFactory SqlExpressionFactory { get; }
        protected virtual IReadOnlyDictionary<string, object> ParameterValues { get; }

        public virtual SelectExpression Process([NotNull] SelectExpression selectExpression, out bool canCache)
        {
            Check.NotNull(selectExpression, nameof(selectExpression));

            var result = Process(selectExpression);
            canCache = _canCache;

            return result;
        }

        protected virtual void DoNotCache() => _canCache = false;

        protected virtual TableExpressionBase Process([NotNull] TableExpressionBase tableExpressionBase)
        {
            Check.NotNull(tableExpressionBase, nameof(tableExpressionBase));

            switch (tableExpressionBase)
            {
                case CrossApplyExpression crossApplyExpression:
                    return crossApplyExpression.Update(Process(crossApplyExpression.Table));

                case CrossJoinExpression crossJoinExpression:
                    return crossJoinExpression.Update(Process(crossJoinExpression.Table));

                case ExceptExpression exceptExpression:
                {
                    var source1 = Process(exceptExpression.Source1);
                    var source2 = Process(exceptExpression.Source2);

                    return exceptExpression.Update(source1, source2);
                }

                case FromSqlExpression fromSqlExpression:
                    return fromSqlExpression;

                case InnerJoinExpression innerJoinExpression:
                {
                    var newTable = Process(innerJoinExpression.Table);
                    var newJoinPredicate = ProcessJoinPredicate(innerJoinExpression.JoinPredicate);

                    return TryGetBoolConstantValue(newJoinPredicate) == true
                        ? (TableExpressionBase)new CrossJoinExpression(newTable)
                        : innerJoinExpression.Update(newTable, newJoinPredicate);
                }

                case IntersectExpression intersectExpression:
                {
                    var source1 = Process(intersectExpression.Source1);
                    var source2 = Process(intersectExpression.Source2);

                    return intersectExpression.Update(source1, source2);
                }

                case LeftJoinExpression leftJoinExpression:
                {
                    var newTable = Process(leftJoinExpression.Table);
                    var newJoinPredicate = ProcessJoinPredicate(leftJoinExpression.JoinPredicate);

                    return leftJoinExpression.Update(newTable, newJoinPredicate);
                }

                case OuterApplyExpression outerApplyExpression:
                    return outerApplyExpression.Update(Process(outerApplyExpression.Table));

                case SelectExpression selectExpression:
                    return Process(selectExpression);

                case TableValuedFunctionExpression tableValuedFunctionExpression:
                    // See issue#20180
                    return tableValuedFunctionExpression;

                case TableExpression tableExpression:
                    return tableExpression;

                case UnionExpression unionExpression:
                {
                    var source1 = Process(unionExpression.Source1);
                    var source2 = Process(unionExpression.Source2);

                    return unionExpression.Update(source1, source2);
                }

                default:
                    throw new InvalidOperationException(
                        RelationalStrings.UnknownExpressionType(tableExpressionBase, tableExpressionBase.GetType(), nameof(SqlNullabilityProcessor)));
            }
        }

        protected virtual SelectExpression Process([NotNull] SelectExpression selectExpression)
        {
            Check.NotNull(selectExpression, nameof(selectExpression));

            var changed = false;
            var projections = new List<ProjectionExpression>();
            foreach (var item in selectExpression.Projection)
            {
                var updatedProjection = item.Update(Process(item.Expression, out _));
                projections.Add(updatedProjection);
                changed |= updatedProjection != item;
            }

            var tables = new List<TableExpressionBase>();
            foreach (var table in selectExpression.Tables)
            {
                var newTable = Process(table);
                changed |= newTable != table;
                tables.Add(newTable);
            }

            var predicate = Process(selectExpression.Predicate, allowOptimizedExpansion: true, out _);
            changed |= predicate != selectExpression.Predicate;

            if (TryGetBoolConstantValue(predicate) == true)
            {
                predicate = null;
                changed = true;
            }

            var groupBy = new List<SqlExpression>();
            foreach (var groupingKey in selectExpression.GroupBy)
            {
                var newGroupingKey = Process(groupingKey, out _);
                changed |= newGroupingKey != groupingKey;
                groupBy.Add(newGroupingKey);
            }

            var having = Process(selectExpression.Having, allowOptimizedExpansion: true, out _);
            changed |= having != selectExpression.Having;

            if (TryGetBoolConstantValue(having) == true)
            {
                having = null;
                changed = true;
            }

            var orderings = new List<OrderingExpression>();
            foreach (var ordering in selectExpression.Orderings)
            {
                var orderingExpression = Process(ordering.Expression, out _);
                changed |= orderingExpression != ordering.Expression;
                orderings.Add(ordering.Update(orderingExpression));
            }

            var offset = Process(selectExpression.Offset, out _);
            changed |= offset != selectExpression.Offset;

            var limit = Process(selectExpression.Limit, out _);
            changed |= limit != selectExpression.Limit;

            return changed
                ? selectExpression.Update(
                    projections, tables, predicate, groupBy, having, orderings, limit, offset)
                : selectExpression;
        }

        protected virtual SqlExpression Process([CanBeNull] SqlExpression expression, out bool nullable)
            => Process(expression, allowOptimizedExpansion: false, out nullable);

        protected virtual SqlExpression Process([CanBeNull] SqlExpression sqlExpression, bool allowOptimizedExpansion, out bool nullable)
            => Process(sqlExpression, allowOptimizedExpansion, preserveNonNullableColumns: false, out nullable);

        private SqlExpression Process(
            [CanBeNull] SqlExpression sqlExpression, bool allowOptimizedExpansion, bool preserveNonNullableColumns, out bool nullable)
        {
            if (sqlExpression == null)
            {
                nullable = false;
                return sqlExpression;
            }

            var nonNullableColumnsCount = _nonNullableColumns.Count;
            SqlExpression result;
            switch (sqlExpression)
            {
                case CaseExpression caseExpression:
                    result = ProcessCase(caseExpression, allowOptimizedExpansion, out nullable);
                    break;

                case CollateExpression collateExpression:
                    result = ProcessCollate(collateExpression, allowOptimizedExpansion, out nullable);
                    break;

                case ColumnExpression columnExpression:
                    result = ProcessColumn(columnExpression, allowOptimizedExpansion, out nullable);
                    break;

                case ExistsExpression existsExpression:
                    result = ProcessExists(existsExpression, allowOptimizedExpansion, out nullable);
                    break;

                case InExpression inExpression:
                    result = ProcessIn(inExpression, allowOptimizedExpansion, out nullable);
                    break;

                case LikeExpression likeExpression:
                    result = ProcessLike(likeExpression, allowOptimizedExpansion, out nullable);
                    break;

                case RowNumberExpression rowNumberExpression:
                    result = ProcessRowNumber(rowNumberExpression, allowOptimizedExpansion, out nullable);
                    break;

                case ScalarSubqueryExpression scalarSubqueryExpression:
                    result = ProcessScalarSubquery(scalarSubqueryExpression, allowOptimizedExpansion, out nullable);
                    break;

                case SqlBinaryExpression sqlBinaryExpression:
                    result = ProcessSqlBinary(sqlBinaryExpression, allowOptimizedExpansion, out nullable);
                    break;

                case SqlConstantExpression sqlConstantExpression:
                    result = ProcessSqlConstant(sqlConstantExpression, allowOptimizedExpansion, out nullable);
                    break;

                case SqlFragmentExpression sqlFragmentExpression:
                    result = ProcessSqlFragment(sqlFragmentExpression, allowOptimizedExpansion, out nullable);
                    break;

                case SqlFunctionExpression sqlFunctionExpression:
                    result = ProcessSqlFunction(sqlFunctionExpression, allowOptimizedExpansion, out nullable);
                    break;

                case SqlParameterExpression sqlParameterExpression:
                    result = ProcessSqlParameter(sqlParameterExpression, allowOptimizedExpansion, out nullable);
                    break;

                case SqlUnaryExpression sqlUnaryExpression:
                    result = ProcessSqlUnary(sqlUnaryExpression, allowOptimizedExpansion, out nullable);
                    break;

                default:
                    throw new InvalidOperationException(
                        RelationalStrings.UnknownExpressionType(sqlExpression, sqlExpression.GetType(), nameof(SqlNullabilityProcessor)));
            }

            if (!preserveNonNullableColumns)
            {
                RestoreNonNullableColumnsList(nonNullableColumnsCount);
            }

            return result;
        }

        protected virtual SqlExpression ProcessCase([NotNull] CaseExpression caseExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(caseExpression, nameof(caseExpression));

            // if there is no 'else' there is a possibility of null, when none of the conditions are met
            // otherwise the result is nullable if any of the WhenClause results OR ElseResult is nullable
            nullable = caseExpression.ElseResult == null;
            var currentNonNullableColumnsCount = _nonNullableColumns.Count;

            var operand = Process(caseExpression.Operand, out _);
            var whenClauses = new List<CaseWhenClause>();
            var testIsCondition = caseExpression.Operand == null;

            var testEvaluatesToTrue = false;
            foreach (var whenClause in caseExpression.WhenClauses)
            {
                // we can use non-nullable column information we got from visiting Test, in the Result
                var test = Process(whenClause.Test, allowOptimizedExpansion: testIsCondition, preserveNonNullableColumns: true, out _);

                if (TryGetBoolConstantValue(test) is bool testConstantBool)
                {
                    if (testConstantBool)
                    {
                        testEvaluatesToTrue = true;
                    }
                    else
                    {
                        // if test evaluates to 'false' we can remove the WhenClause
                        RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

                        continue;
                    }
                }

                var newResult = Process(whenClause.Result, out var resultNullable);

                nullable |= resultNullable;
                whenClauses.Add(new CaseWhenClause(test, newResult));
                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);

                // if test evaluates to 'true' we can remove every condition that comes after, including ElseResult
                if (testEvaluatesToTrue)
                {
                    break;
                }
            }

            SqlExpression elseResult = null;
            if (!testEvaluatesToTrue)
            {
                elseResult = Process(caseExpression.ElseResult, out var elseResultNullable);
                nullable |= elseResultNullable;
            }

            // if there are no whenClauses left (e.g. their tests evaluated to false):
            // - if there is Else block, return it
            // - if there is no Else block, return null
            if (whenClauses.Count == 0)
            {
                return elseResult ?? SqlExpressionFactory.Constant(null, caseExpression.TypeMapping);
            }

            // if there is only one When clause and it's test evaluates to 'true' AND there is no else block, simply return the result
            return elseResult == null
                && whenClauses.Count == 1
                && TryGetBoolConstantValue(whenClauses[0].Test) == true
                ? whenClauses[0].Result
                : caseExpression.Update(operand, whenClauses, elseResult);
        }

        protected virtual SqlExpression ProcessCollate([NotNull] CollateExpression collateExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(collateExpression, nameof(collateExpression));

            return collateExpression.Update(Process(collateExpression.Operand, out nullable));
        }

        protected virtual SqlExpression ProcessColumn([NotNull] ColumnExpression columnExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(columnExpression, nameof(columnExpression));

            nullable = columnExpression.IsNullable && !_nonNullableColumns.Contains(columnExpression);

            return columnExpression;
        }

        protected virtual SqlExpression ProcessExists([NotNull] ExistsExpression existsExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(existsExpression, nameof(existsExpression));

            var subquery = Process(existsExpression.Subquery);
            nullable = false;

            // if subquery has predicate which evaluates to false, we can simply return false
            return TryGetBoolConstantValue(subquery.Predicate) == false
                ? subquery.Predicate
                : existsExpression.Update(subquery);
        }

        protected virtual SqlExpression ProcessIn([NotNull] InExpression inExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(inExpression, nameof(inExpression));

            var item = Process(inExpression.Item, out var itemNullable);

            if (inExpression.Subquery != null)
            {
                var subquery = Process(inExpression.Subquery);

                // a IN (SELECT * FROM table WHERE false) => false
                if (TryGetBoolConstantValue(subquery.Predicate) == false)
                {
                    nullable = false;

                    return subquery.Predicate;
                }

                // if item is not nullable, and subquery contains a non-nullable column we know the result can never be null
                // note: in this case we could broaden the optimization if we knew the nullability of the projection
                // but we don't keep that information and we want to avoid double visitation
                nullable = !(!itemNullable
                    && subquery.Projection.Count == 1
                    && subquery.Projection[0].Expression is ColumnExpression columnProjection
                    && !columnProjection.IsNullable);

                return inExpression.Update(item, values: null, subquery);
            }

            // for relational null semantics just leave as is
            // same for values we don't know how to properly handle (i.e. other than constant or parameter)
            if (UseRelationalNulls
                || !(inExpression.Values is SqlConstantExpression || inExpression.Values is SqlParameterExpression))
            {
                var values = Process(inExpression.Values, out _);
                nullable = false;

                return inExpression.Update(item, values, subquery: null);
            }

            // for c# null semantics we need to remove nulls from Values and add IsNull/IsNotNull when necessary
            var (inValuesExpression, inValuesList, hasNullValue) = ProcessInExpressionValues(inExpression.Values);

            // either values array is empty or only contains null
            if (inValuesList.Count == 0)
            {
                nullable = false;

                // a IN () -> false
                // non_nullable IN (NULL) -> false
                // a NOT IN () -> true
                // non_nullable NOT IN (NULL) -> true
                // nullable IN (NULL) -> nullable IS NULL
                // nullable NOT IN (NULL) -> nullable IS NOT NULL
                return !hasNullValue || !itemNullable
                    ? (SqlExpression)SqlExpressionFactory.Constant(
                        inExpression.IsNegated,
                        inExpression.TypeMapping)
                    : inExpression.IsNegated
                        ? SqlExpressionFactory.IsNotNull(item)
                        : SqlExpressionFactory.IsNull(item);
            }

            if (!itemNullable
                || (allowOptimizedExpansion && !inExpression.IsNegated && !hasNullValue))
            {
                nullable = false;

                // non_nullable IN (1, 2) -> non_nullable IN (1, 2)
                // non_nullable IN (1, 2, NULL) -> non_nullable IN (1, 2)
                // non_nullable NOT IN (1, 2) -> non_nullable NOT IN (1, 2)
                // non_nullable NOT IN (1, 2, NULL) -> non_nullable NOT IN (1, 2)
                // nullable IN (1, 2) -> nullable IN (1, 2) (optimized)
                return inExpression.Update(item, inValuesExpression, subquery: null);
            }

            nullable = false;

            // nullable IN (1, 2) -> nullable IN (1, 2) AND nullable IS NOT NULL (full)
            // nullable IN (1, 2, NULL) -> nullable IN (1, 2) OR nullable IS NULL (full)
            // nullable NOT IN (1, 2) -> nullable NOT IN (1, 2) OR nullable IS NULL (full)
            // nullable NOT IN (1, 2, NULL) -> nullable NOT IN (1, 2) AND nullable IS NOT NULL (full)
            return inExpression.IsNegated == hasNullValue
                ? SqlExpressionFactory.AndAlso(
                    inExpression.Update(item, inValuesExpression, subquery: null),
                    SqlExpressionFactory.IsNotNull(item))
                : SqlExpressionFactory.OrElse(
                    inExpression.Update(item, inValuesExpression, subquery: null),
                    SqlExpressionFactory.IsNull(item));

            (SqlConstantExpression ProcessedValuesExpression, List<object> ProcessedValuesList, bool HasNullValue) ProcessInExpressionValues(SqlExpression valuesExpression)
            {
                var inValues = new List<object>();
                var hasNullValue = false;
                RelationalTypeMapping typeMapping = null;

                IEnumerable values = null;
                if (valuesExpression is SqlConstantExpression sqlConstant)
                {
                    typeMapping = sqlConstant.TypeMapping;
                    values = (IEnumerable)sqlConstant.Value;
                }
                else if (valuesExpression is SqlParameterExpression sqlParameter)
                {
                    DoNotCache();
                    typeMapping = sqlParameter.TypeMapping;
                    values = (IEnumerable)ParameterValues[sqlParameter.Name];
                }

                foreach (var value in values)
                {
                    if (value == null)
                    {
                        hasNullValue = true;
                        continue;
                    }

                    inValues.Add(value);
                }

                var processedValuesExpression = SqlExpressionFactory.Constant(inValues, typeMapping);

                return (processedValuesExpression, inValues, hasNullValue);
            }
        }

        protected virtual SqlExpression ProcessLike([NotNull] LikeExpression likeExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(likeExpression, nameof(likeExpression));

            var match = Process(likeExpression.Match, out var matchNullable);
            var pattern = Process(likeExpression.Pattern, out var patternNullable);
            var escapeChar = Process(likeExpression.EscapeChar, out var escapeCharNullable);

            nullable = matchNullable || patternNullable || escapeCharNullable;

            return likeExpression.Update(match, pattern, escapeChar);
        }

        protected virtual SqlExpression ProcessRowNumber([NotNull] RowNumberExpression rowNumberExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(rowNumberExpression, nameof(rowNumberExpression));

            var changed = false;
            var partitions = new List<SqlExpression>();
            foreach (var partition in rowNumberExpression.Partitions)
            {
                var newPartition = Process(partition, out _);
                changed |= newPartition != partition;
                partitions.Add(newPartition);
            }

            var orderings = new List<OrderingExpression>();
            foreach (var ordering in rowNumberExpression.Orderings)
            {
                var newOrdering = ordering.Update(Process(ordering.Expression, out _));
                changed |= newOrdering != ordering;
                orderings.Add(newOrdering);
            }

            nullable = false;

            return changed
                ? rowNumberExpression.Update(partitions, orderings)
                : rowNumberExpression;
        }

        protected virtual SqlExpression ProcessScalarSubquery(
            [NotNull] ScalarSubqueryExpression scalarSubqueryExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(scalarSubqueryExpression, nameof(scalarSubqueryExpression));

            nullable = true;

            return scalarSubqueryExpression.Update(Process(scalarSubqueryExpression.Subquery));
        }

        protected virtual SqlExpression ProcessSqlBinary([NotNull] SqlBinaryExpression sqlBinaryExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(sqlBinaryExpression, nameof(sqlBinaryExpression));

            var optimize = allowOptimizedExpansion;

            allowOptimizedExpansion = allowOptimizedExpansion
                && (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso
                    || sqlBinaryExpression.OperatorType == ExpressionType.OrElse);

            var currentNonNullableColumnsCount = _nonNullableColumns.Count;

            var left = Process(sqlBinaryExpression.Left, allowOptimizedExpansion, preserveNonNullableColumns: true, out var leftNullable);

            var leftNonNullableColumns = _nonNullableColumns.Skip(currentNonNullableColumnsCount).ToList();
            if (sqlBinaryExpression.OperatorType != ExpressionType.AndAlso)
            {
                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            var right = Process(sqlBinaryExpression.Right, allowOptimizedExpansion, preserveNonNullableColumns: true, out var rightNullable);

            if (sqlBinaryExpression.OperatorType == ExpressionType.OrElse)
            {
                var intersect = leftNonNullableColumns.Intersect(_nonNullableColumns.Skip(currentNonNullableColumnsCount)).ToList();
                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
                _nonNullableColumns.AddRange(intersect);
            }
            else if (sqlBinaryExpression.OperatorType != ExpressionType.AndAlso)
            {
                // in case of AndAlso we already have what we need as the column information propagates from left to right
                RestoreNonNullableColumnsList(currentNonNullableColumnsCount);
            }

            // nullableStringColumn + a -> COALESCE(nullableStringColumn, "") + a
            if (sqlBinaryExpression.OperatorType == ExpressionType.Add
                && sqlBinaryExpression.Type == typeof(string))
            {
                if (leftNullable)
                {
                    left = AddNullConcatenationProtection(left, sqlBinaryExpression.TypeMapping);
                }

                if (rightNullable)
                {
                    right = AddNullConcatenationProtection(right, sqlBinaryExpression.TypeMapping);
                }

                nullable = false;

                return sqlBinaryExpression.Update(left, right);
            }

            if (sqlBinaryExpression.OperatorType == ExpressionType.Equal
                || sqlBinaryExpression.OperatorType == ExpressionType.NotEqual)
            {
                var updated = sqlBinaryExpression.Update(left, right);

                var optimized = OptimizeComparison(
                    updated,
                    left,
                    right,
                    leftNullable,
                    rightNullable,
                    out nullable);

                if (optimized is SqlUnaryExpression optimizedUnary
                    && optimizedUnary.OperatorType == ExpressionType.NotEqual
                    && optimizedUnary.Operand is ColumnExpression optimizedUnaryColumnOperand)
                {
                    _nonNullableColumns.Add(optimizedUnaryColumnOperand);
                }

                // we assume that NullSemantics rewrite is only needed (on the current level)
                // if the optimization didn't make any changes.
                // Reason is that optimization can/will change the nullability of the resulting expression
                // and that inforation is not tracked/stored anywhere
                // so we can no longer rely on nullabilities that we computed earlier (leftNullable, rightNullable)
                // when performing null semantics rewrite.
                // It should be fine because current optimizations *radically* change the expression
                // (e.g. binary -> unary, or binary -> constant)
                // but we need to pay attention in the future if we introduce more subtle transformations here
                if (optimized.Equals(updated)
                    && (leftNullable || rightNullable)
                    && !UseRelationalNulls)
                {
                    var rewriteNullSemanticsResult = RewriteNullSemantics(
                        updated,
                        updated.Left,
                        updated.Right,
                        leftNullable,
                        rightNullable,
                        optimize,
                        out nullable);

                    return rewriteNullSemanticsResult;
                }

                return optimized;
            }

            nullable = leftNullable || rightNullable;

            var result = sqlBinaryExpression.Update(left, right);

            return result is SqlBinaryExpression sqlBinaryResult
                && (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso
                    || sqlBinaryExpression.OperatorType == ExpressionType.OrElse)
                ? SimplifyLogicalSqlBinaryExpression(sqlBinaryResult)
                : result;

            SqlExpression AddNullConcatenationProtection(SqlExpression argument, RelationalTypeMapping typeMapping)
                => argument is SqlConstantExpression || argument is SqlParameterExpression
                ? (SqlExpression)SqlExpressionFactory.Constant(string.Empty, typeMapping)
                : SqlExpressionFactory.Coalesce(argument, SqlExpressionFactory.Constant(string.Empty, typeMapping));
        }

        protected virtual SqlExpression ProcessSqlConstant([NotNull] SqlConstantExpression sqlConstantExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(sqlConstantExpression, nameof(sqlConstantExpression));

            nullable = sqlConstantExpression.Value == null;

            return sqlConstantExpression;
        }

        protected virtual SqlExpression ProcessSqlFragment([NotNull] SqlFragmentExpression sqlFragmentExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(sqlFragmentExpression, nameof(sqlFragmentExpression));

            nullable = false;

            return sqlFragmentExpression;
        }

        protected virtual SqlExpression ProcessSqlFunction([NotNull] SqlFunctionExpression sqlFunctionExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(sqlFunctionExpression, nameof(sqlFunctionExpression));

            if (sqlFunctionExpression.IsBuiltIn
                && string.Equals(sqlFunctionExpression.Name, "COALESCE", StringComparison.OrdinalIgnoreCase))
            {
                var left = Process(sqlFunctionExpression.Arguments[0], out var leftNullable);
                var right = Process(sqlFunctionExpression.Arguments[1], out var rightNullable);

                nullable = leftNullable && rightNullable;

                return sqlFunctionExpression.Update(sqlFunctionExpression.Instance, new[] { left, right });
            }

            var instance = Process(sqlFunctionExpression.Instance, out _);
            nullable = sqlFunctionExpression.IsNullable;

            if (sqlFunctionExpression.IsNiladic)
            {
                return sqlFunctionExpression.Update(instance, sqlFunctionExpression.Arguments);
            }

            var arguments = new SqlExpression[sqlFunctionExpression.Arguments.Count];
            for (var i = 0; i < arguments.Length; i++)
            {
                arguments[i] = Process(sqlFunctionExpression.Arguments[i], out _);
            }


            return sqlFunctionExpression.Update(instance, arguments);
        }

        protected virtual SqlExpression ProcessSqlParameter([NotNull] SqlParameterExpression sqlParameterExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(sqlParameterExpression, nameof(sqlParameterExpression));

            nullable = ParameterValues[sqlParameterExpression.Name] == null;

            return nullable
                ? SqlExpressionFactory.Constant(null, sqlParameterExpression.TypeMapping)
                : (SqlExpression)sqlParameterExpression;
        }

        protected virtual SqlExpression ProcessSqlUnary([NotNull] SqlUnaryExpression sqlUnaryExpression, bool allowOptimizedExpansion, out bool nullable)
        {
            Check.NotNull(sqlUnaryExpression, nameof(sqlUnaryExpression));

            var operand = Process(sqlUnaryExpression.Operand, out var operandNullable);
            var updated = sqlUnaryExpression.Update(operand);

            if (sqlUnaryExpression.OperatorType == ExpressionType.Equal
                || sqlUnaryExpression.OperatorType == ExpressionType.NotEqual)
            {
                var result = ProcessNullNotNull(updated, operandNullable);

                // result of IsNull/IsNotNull can never be null
                nullable = false;

                if (result is SqlUnaryExpression resultUnary
                    && resultUnary.OperatorType == ExpressionType.NotEqual
                    && resultUnary.Operand is ColumnExpression resultColumnOperand)
                {
                    _nonNullableColumns.Add(resultColumnOperand);
                }

                return result;
            }

            nullable = operandNullable;

            return !operandNullable && sqlUnaryExpression.OperatorType == ExpressionType.Not
                ? OptimizeNonNullableNotExpression(updated)
                : updated;
        }

        private static bool? TryGetBoolConstantValue(SqlExpression expression)
            => expression is SqlConstantExpression constantExpression
            && constantExpression.Value is bool boolValue
            ? boolValue
            : (bool?)null;

        private void RestoreNonNullableColumnsList(int counter)
        {
            if (counter < _nonNullableColumns.Count)
            {
                _nonNullableColumns.RemoveRange(counter, _nonNullableColumns.Count - counter);
            }
        }

        private SqlExpression ProcessJoinPredicate(SqlExpression predicate)
        {
            if (predicate is SqlBinaryExpression sqlBinaryExpression)
            {
                if (sqlBinaryExpression.OperatorType == ExpressionType.Equal)
                {
                    var left = Process(sqlBinaryExpression.Left, allowOptimizedExpansion: true, out var leftNullable);
                    var right = Process(sqlBinaryExpression.Right, allowOptimizedExpansion: true, out var rightNullable);

                    var result = OptimizeComparison(
                        sqlBinaryExpression.Update(left, right),
                        left,
                        right,
                        leftNullable,
                        rightNullable,
                        out _);

                    return result;
                }

                if (sqlBinaryExpression.OperatorType == ExpressionType.AndAlso)
                {
                    return Process(sqlBinaryExpression, allowOptimizedExpansion: true, out _);
                }
            }

            throw new InvalidOperationException(
                RelationalStrings.UnknownExpressionType(predicate, predicate.GetType(), nameof(SqlNullabilityProcessor)));
        }

        private SqlExpression OptimizeComparison(
            SqlBinaryExpression sqlBinaryExpression, SqlExpression left, SqlExpression right,
            bool leftNullable, bool rightNullable, out bool nullable)
        {
            var leftNullValue = leftNullable && (left is SqlConstantExpression || left is SqlParameterExpression);
            var rightNullValue = rightNullable && (right is SqlConstantExpression || right is SqlParameterExpression);

            // a == null -> a IS NULL
            // a != null -> a IS NOT NULL
            if (rightNullValue)
            {
                var result = sqlBinaryExpression.OperatorType == ExpressionType.Equal
                    ? ProcessNullNotNull(SqlExpressionFactory.IsNull(left), leftNullable)
                    : ProcessNullNotNull(SqlExpressionFactory.IsNotNull(left), leftNullable);

                nullable = false;

                return result;
            }

            // null == a -> a IS NULL
            // null != a -> a IS NOT NULL
            if (leftNullValue)
            {
                var result = sqlBinaryExpression.OperatorType == ExpressionType.Equal
                    ? ProcessNullNotNull(SqlExpressionFactory.IsNull(right), rightNullable)
                    : ProcessNullNotNull(SqlExpressionFactory.IsNotNull(right), rightNullable);

                nullable = false;

                return result;
            }

            if (TryGetBoolConstantValue(right) is bool rightBoolValue
                && !leftNullable)
            {
                nullable = leftNullable;

                // only correct in 2-value logic
                // a == true -> a
                // a == false -> !a
                // a != true -> !a
                // a != false -> a
                return sqlBinaryExpression.OperatorType == ExpressionType.Equal ^ rightBoolValue
                    ? OptimizeNonNullableNotExpression(SqlExpressionFactory.Not(left))
                    : left;
            }

            if (TryGetBoolConstantValue(left) is bool leftBoolValue
                && !rightNullable)
            {
                nullable = rightNullable;

                // only correct in 2-value logic
                // true == a -> a
                // false == a -> !a
                // true != a -> !a
                // false != a -> a
                return sqlBinaryExpression.OperatorType == ExpressionType.Equal ^ leftBoolValue
                    ? SqlExpressionFactory.Not(right)
                    : right;
            }

            // only correct in 2-value logic
            // a == a -> true
            // a != a -> false
            if (!leftNullable
                && left.Equals(right))
            {
                nullable = false;

                return SqlExpressionFactory.Constant(
                    sqlBinaryExpression.OperatorType == ExpressionType.Equal,
                    sqlBinaryExpression.TypeMapping);
            }

            if (!leftNullable
                && !rightNullable
                && (sqlBinaryExpression.OperatorType == ExpressionType.Equal || sqlBinaryExpression.OperatorType == ExpressionType.NotEqual))
            {
                var leftUnary = left as SqlUnaryExpression;
                var rightUnary = right as SqlUnaryExpression;

                var leftNegated = leftUnary?.IsLogicalNot() == true;
                var rightNegated = rightUnary?.IsLogicalNot() == true;

                if (leftNegated)
                {
                    left = leftUnary.Operand;
                }

                if (rightNegated)
                {
                    right = rightUnary.Operand;
                }

                // a == b <=> !a == !b -> a == b
                // !a == b <=> a == !b -> a != b
                // a != b <=> !a != !b -> a != b
                // !a != b <=> a != !b -> a == b

                nullable = false;

                return sqlBinaryExpression.OperatorType == ExpressionType.Equal ^ leftNegated == rightNegated
                    ? SqlExpressionFactory.NotEqual(left, right)
                    : SqlExpressionFactory.Equal(left, right);
            }

            nullable = false;

            return sqlBinaryExpression.Update(left, right);
        }

        private SqlExpression RewriteNullSemantics(
            SqlBinaryExpression sqlBinaryExpression, SqlExpression left, SqlExpression right,
            bool leftNullable, bool rightNullable, bool optimize, out bool nullable)
        {
            var leftUnary = left as SqlUnaryExpression;
            var rightUnary = right as SqlUnaryExpression;

            var leftNegated = leftUnary?.IsLogicalNot() == true;
            var rightNegated = rightUnary?.IsLogicalNot() == true;

            if (leftNegated)
            {
                left = leftUnary.Operand;
            }

            if (rightNegated)
            {
                right = rightUnary.Operand;
            }

            var leftIsNull = ProcessNullNotNull(SqlExpressionFactory.IsNull(left), leftNullable);
            var leftIsNotNull = OptimizeNonNullableNotExpression(SqlExpressionFactory.Not(leftIsNull));

            var rightIsNull = ProcessNullNotNull(SqlExpressionFactory.IsNull(right), rightNullable);
            var rightIsNotNull = OptimizeNonNullableNotExpression(SqlExpressionFactory.Not(rightIsNull));

            // optimized expansion which doesn't distinguish between null and false
            if (optimize
                && sqlBinaryExpression.OperatorType == ExpressionType.Equal
                && !leftNegated
                && !rightNegated)
            {
                // when we use optimized form, the result can still be nullable
                if (leftNullable && rightNullable)
                {
                    nullable = true;

                    return SimplifyLogicalSqlBinaryExpression(
                        SqlExpressionFactory.OrElse(
                            SqlExpressionFactory.Equal(left, right),
                            SimplifyLogicalSqlBinaryExpression(
                                SqlExpressionFactory.AndAlso(leftIsNull, rightIsNull))));
                }

                if ((leftNullable && !rightNullable)
                    || (!leftNullable && rightNullable))
                {
                    nullable = true;

                    return SqlExpressionFactory.Equal(left, right);
                }
            }

            // doing a full null semantics rewrite - removing all nulls from truth table
            nullable = false;

            if (sqlBinaryExpression.OperatorType == ExpressionType.Equal)
            {
                if (leftNullable && rightNullable)
                {
                    // ?a == ?b <=> !(?a) == !(?b) -> [(a == b) && (a != null && b != null)] || (a == null && b == null))
                    // !(?a) == ?b <=> ?a == !(?b) -> [(a != b) && (a != null && b != null)] || (a == null && b == null)
                    return leftNegated == rightNegated
                        ? ExpandNullableEqualNullable(left, right, leftIsNull, leftIsNotNull, rightIsNull, rightIsNotNull)
                        : ExpandNegatedNullableEqualNullable(left, right, leftIsNull, leftIsNotNull, rightIsNull, rightIsNotNull);
                }

                if (leftNullable && !rightNullable)
                {
                    // ?a == b <=> !(?a) == !b -> (a == b) && (a != null)
                    // !(?a) == b <=> ?a == !b -> (a != b) && (a != null)
                    return leftNegated == rightNegated
                        ? ExpandNullableEqualNonNullable(left, right, leftIsNotNull)
                        : ExpandNegatedNullableEqualNonNullable(left, right, leftIsNotNull);
                }

                if (rightNullable && !leftNullable)
                {
                    // a == ?b <=> !a == !(?b) -> (a == b) && (b != null)
                    // !a == ?b <=> a == !(?b) -> (a != b) && (b != null)
                    return leftNegated == rightNegated
                        ? ExpandNullableEqualNonNullable(left, right, rightIsNotNull)
                        : ExpandNegatedNullableEqualNonNullable(left, right, rightIsNotNull);
                }
            }

            if (sqlBinaryExpression.OperatorType == ExpressionType.NotEqual)
            {
                if (leftNullable && rightNullable)
                {
                    // ?a != ?b <=> !(?a) != !(?b) -> [(a != b) || (a == null || b == null)] && (a != null || b != null)
                    // !(?a) != ?b <=> ?a != !(?b) -> [(a == b) || (a == null || b == null)] && (a != null || b != null)
                    return leftNegated == rightNegated
                        ? ExpandNullableNotEqualNullable(left, right, leftIsNull, leftIsNotNull, rightIsNull, rightIsNotNull)
                        : ExpandNegatedNullableNotEqualNullable(left, right, leftIsNull, leftIsNotNull, rightIsNull, rightIsNotNull);
                }

                if (leftNullable && !rightNullable)
                {
                    // ?a != b <=> !(?a) != !b -> (a != b) || (a == null)
                    // !(?a) != b <=> ?a != !b -> (a == b) || (a == null)
                    return leftNegated == rightNegated
                        ? ExpandNullableNotEqualNonNullable(left, right, leftIsNull)
                        : ExpandNegatedNullableNotEqualNonNullable(left, right, leftIsNull);
                }

                if (rightNullable && !leftNullable)
                {
                    // a != ?b <=> !a != !(?b) -> (a != b) || (b == null)
                    // !a != ?b <=> a != !(?b) -> (a == b) || (b == null)
                    return leftNegated == rightNegated
                        ? ExpandNullableNotEqualNonNullable(left, right, rightIsNull)
                        : ExpandNegatedNullableNotEqualNonNullable(left, right, rightIsNull);
                }
            }

            return sqlBinaryExpression.Update(left, right);
        }

        private SqlExpression SimplifyLogicalSqlBinaryExpression(SqlBinaryExpression sqlBinaryExpression)
        {
            if (sqlBinaryExpression.Left is SqlUnaryExpression leftUnary
                && sqlBinaryExpression.Right is SqlUnaryExpression rightUnary
                && (leftUnary.OperatorType == ExpressionType.Equal || leftUnary.OperatorType == ExpressionType.NotEqual)
                && (rightUnary.OperatorType == ExpressionType.Equal || rightUnary.OperatorType == ExpressionType.NotEqual)
                && leftUnary.Operand.Equals(rightUnary.Operand))
            {
                // a is null || a is null -> a is null
                // a is not null || a is not null -> a is not null
                // a is null && a is null -> a is null
                // a is not null && a is not null -> a is not null
                // a is null || a is not null -> true
                // a is null && a is not null -> false
                return leftUnary.OperatorType == rightUnary.OperatorType
                    ? (SqlExpression)leftUnary
                    : SqlExpressionFactory.Constant(sqlBinaryExpression.OperatorType == ExpressionType.OrElse, sqlBinaryExpression.TypeMapping);
            }

            // true && a -> a
            // true || a -> true
            // false && a -> false
            // false || a -> a
            if (sqlBinaryExpression.Left is SqlConstantExpression newLeftConstant)
            {
                return sqlBinaryExpression.OperatorType == ExpressionType.AndAlso
                    ? (bool)newLeftConstant.Value
                        ? sqlBinaryExpression.Right
                        : newLeftConstant
                    : (bool)newLeftConstant.Value
                        ? newLeftConstant
                        : sqlBinaryExpression.Right;
            }
            else if (sqlBinaryExpression.Right is SqlConstantExpression newRightConstant)
            {
                // a && true -> a
                // a || true -> true
                // a && false -> false
                // a || false -> a
                return sqlBinaryExpression.OperatorType == ExpressionType.AndAlso
                    ? (bool)newRightConstant.Value
                        ? sqlBinaryExpression.Left
                        : newRightConstant
                    : (bool)newRightConstant.Value
                        ? newRightConstant
                        : sqlBinaryExpression.Left;
            }

            return sqlBinaryExpression;
        }

        private SqlExpression OptimizeNonNullableNotExpression(SqlUnaryExpression sqlUnaryExpression)
        {
            if (sqlUnaryExpression.OperatorType != ExpressionType.Not)
            {
                return sqlUnaryExpression;
            }

            switch (sqlUnaryExpression.Operand)
            {
                // !(true) -> false
                // !(false) -> true
                case SqlConstantExpression constantOperand
                    when constantOperand.Value is bool value:
                {
                    return SqlExpressionFactory.Constant(!value, sqlUnaryExpression.TypeMapping);
                }

                case InExpression inOperand:
                    return inOperand.Negate();

                case SqlUnaryExpression sqlUnaryOperand:
                {
                    switch (sqlUnaryOperand.OperatorType)
                    {
                        // !(!a) -> a
                        case ExpressionType.Not:
                            return sqlUnaryOperand.Operand;

                        //!(a IS NULL) -> a IS NOT NULL
                        case ExpressionType.Equal:
                            return SqlExpressionFactory.IsNotNull(sqlUnaryOperand.Operand);

                        //!(a IS NOT NULL) -> a IS NULL
                        case ExpressionType.NotEqual:
                            return SqlExpressionFactory.IsNull(sqlUnaryOperand.Operand);
                    }
                    break;
                }

                case SqlBinaryExpression sqlBinaryOperand:
                {
                    // optimizations below are only correct in 2-value logic
                    // De Morgan's
                    if (sqlBinaryOperand.OperatorType == ExpressionType.AndAlso
                        || sqlBinaryOperand.OperatorType == ExpressionType.OrElse)
                    {
                        // since entire AndAlso/OrElse expression is non-nullable, both sides of it (left and right) must also be non-nullable
                        // so it's safe to perform recursive optimization here
                        var left = OptimizeNonNullableNotExpression(SqlExpressionFactory.Not(sqlBinaryOperand.Left));
                        var right = OptimizeNonNullableNotExpression(SqlExpressionFactory.Not(sqlBinaryOperand.Right));

                        return SimplifyLogicalSqlBinaryExpression(
                            SqlExpressionFactory.MakeBinary(
                                sqlBinaryOperand.OperatorType == ExpressionType.AndAlso
                                    ? ExpressionType.OrElse
                                    : ExpressionType.AndAlso,
                                left,
                                right,
                                sqlBinaryOperand.TypeMapping));
                    }

                    // !(a == b) -> a != b
                    // !(a != b) -> a == b
                    // !(a > b) -> a <= b
                    // !(a >= b) -> a < b
                    // !(a < b) -> a >= b
                    // !(a <= b) -> a > b
                    if (TryNegate(sqlBinaryOperand.OperatorType, out var negated))
                    {
                        return SqlExpressionFactory.MakeBinary(
                            negated,
                            sqlBinaryOperand.Left,
                            sqlBinaryOperand.Right,
                            sqlBinaryOperand.TypeMapping);
                    }
                }
                break;
            }

            return sqlUnaryExpression;

            static bool TryNegate(ExpressionType expressionType, out ExpressionType result)
            {
                var negated = expressionType switch
                {
                    ExpressionType.Equal => ExpressionType.NotEqual,
                    ExpressionType.NotEqual => ExpressionType.Equal,
                    ExpressionType.GreaterThan => ExpressionType.LessThanOrEqual,
                    ExpressionType.GreaterThanOrEqual => ExpressionType.LessThan,
                    ExpressionType.LessThan => ExpressionType.GreaterThanOrEqual,
                    ExpressionType.LessThanOrEqual => ExpressionType.GreaterThan,
                    _ => (ExpressionType?)null
                };

                result = negated ?? default;

                return negated.HasValue;
            }
        }

        private SqlExpression ProcessNullNotNull(SqlUnaryExpression sqlUnaryExpression, bool operandNullable)
        {
            if (!operandNullable)
            {
                // when we know that operand is non-nullable:
                // not_null_operand is null-> false
                // not_null_operand is not null -> true
                return SqlExpressionFactory.Constant(
                    sqlUnaryExpression.OperatorType == ExpressionType.NotEqual,
                    sqlUnaryExpression.TypeMapping);
            }

            switch (sqlUnaryExpression.Operand)
            {
                case SqlConstantExpression sqlConstantOperand:
                    // null_value_constant is null -> true
                    // null_value_constant is not null -> false
                    // not_null_value_constant is null -> false
                    // not_null_value_constant is not null -> true
                    return SqlExpressionFactory.Constant(
                        sqlConstantOperand.Value == null ^ sqlUnaryExpression.OperatorType == ExpressionType.NotEqual,
                        sqlUnaryExpression.TypeMapping);

                case SqlParameterExpression sqlParameterOperand:
                    // null_value_parameter is null -> true
                    // null_value_parameter is not null -> false
                    // not_null_value_parameter is null -> false
                    // not_null_value_parameter is not null -> true
                    return SqlExpressionFactory.Constant(
                        ParameterValues[sqlParameterOperand.Name] == null ^ sqlUnaryExpression.OperatorType == ExpressionType.NotEqual,
                        sqlUnaryExpression.TypeMapping);

                case ColumnExpression columnOperand
                    when !columnOperand.IsNullable || _nonNullableColumns.Contains(columnOperand):
                {
                    // IsNull(non_nullable_column) -> false
                    // IsNotNull(non_nullable_column) -> true
                    return SqlExpressionFactory.Constant(
                        sqlUnaryExpression.OperatorType == ExpressionType.NotEqual,
                        sqlUnaryExpression.TypeMapping);
                }

                case SqlUnaryExpression sqlUnaryOperand:
                    switch (sqlUnaryOperand.OperatorType)
                    {
                        case ExpressionType.Convert:
                        case ExpressionType.Not:
                        case ExpressionType.Negate:
                            // op(a) is null -> a is null
                            // op(a) is not null -> a is not null
                            return ProcessNullNotNull(
                                sqlUnaryExpression.Update(sqlUnaryOperand.Operand),
                                operandNullable);

                        case ExpressionType.Equal:
                        case ExpressionType.NotEqual:
                            // (a is null) is null -> false
                            // (a is not null) is null -> false
                            // (a is null) is not null -> true
                            // (a is not null) is not null -> true
                            return SqlExpressionFactory.Constant(
                                sqlUnaryOperand.OperatorType == ExpressionType.NotEqual,
                                sqlUnaryOperand.TypeMapping);
                    }
                    break;

                case SqlBinaryExpression sqlBinaryOperand
                    when sqlBinaryOperand.OperatorType != ExpressionType.AndAlso
                        && sqlBinaryOperand.OperatorType != ExpressionType.OrElse:
                {
                    // in general:
                    // binaryOp(a, b) == null -> a == null || b == null
                    // binaryOp(a, b) != null -> a != null && b != null
                    // for AndAlso, OrElse we can't do this optimization
                    // we could do something like this, but it seems too complicated:
                    // (a && b) == null -> a == null && b != 0 || a != 0 && b == null
                    // NOTE: we don't preserve nullabilities of left/right individually so we are using nullability binary expression as a whole
                    // this may lead to missing some optimizations, where one of the operands (left or right) is not nullable and the other one is
                    var left = ProcessNullNotNull(
                        SqlExpressionFactory.MakeUnary(
                            sqlUnaryExpression.OperatorType,
                            sqlBinaryOperand.Left,
                            typeof(bool),
                            sqlUnaryExpression.TypeMapping),
                        operandNullable);

                    var right = ProcessNullNotNull(
                        SqlExpressionFactory.MakeUnary(
                            sqlUnaryExpression.OperatorType,
                            sqlBinaryOperand.Right,
                            typeof(bool),
                            sqlUnaryExpression.TypeMapping),
                        operandNullable);

                    return SimplifyLogicalSqlBinaryExpression(
                        SqlExpressionFactory.MakeBinary(
                            sqlUnaryExpression.OperatorType == ExpressionType.Equal
                                ? ExpressionType.OrElse
                                : ExpressionType.AndAlso,
                            left,
                            right,
                            sqlUnaryExpression.TypeMapping));
                }

                case SqlFunctionExpression sqlFunctionExpression:
                {
                    if (sqlFunctionExpression.IsBuiltIn && string.Equals("COALESCE", sqlFunctionExpression.Name, StringComparison.OrdinalIgnoreCase))
                    {
                        // for coalesce:
                        // (a ?? b) == null -> a == null && b == null
                        // (a ?? b) != null -> a != null || b != null
                        var left = ProcessNullNotNull(
                            SqlExpressionFactory.MakeUnary(
                                sqlUnaryExpression.OperatorType,
                                sqlFunctionExpression.Arguments[0],
                                typeof(bool),
                                sqlUnaryExpression.TypeMapping),
                            operandNullable);

                        var right = ProcessNullNotNull(
                            SqlExpressionFactory.MakeUnary(
                                sqlUnaryExpression.OperatorType,
                                sqlFunctionExpression.Arguments[1],
                                typeof(bool),
                                sqlUnaryExpression.TypeMapping),
                            operandNullable);

                        return SimplifyLogicalSqlBinaryExpression(
                            SqlExpressionFactory.MakeBinary(
                                sqlUnaryExpression.OperatorType == ExpressionType.Equal
                                    ? ExpressionType.AndAlso
                                    : ExpressionType.OrElse,
                                left,
                                right,
                                sqlUnaryExpression.TypeMapping));
                    }

                    if (!sqlFunctionExpression.IsNullable)
                    {
                        // when we know that function can't be nullable:
                        // non_nullable_function() is null-> false
                        // non_nullable_function() is not null -> true
                        return SqlExpressionFactory.Constant(
                            sqlUnaryExpression.OperatorType == ExpressionType.NotEqual,
                            sqlUnaryExpression.TypeMapping);
                    }

                    // see if we can derive function nullability from it's instance and/or arguments
                    // rather than evaluating nullability of the entire function
                    var nullabilityPropagationElements = new List<SqlExpression>();
                    if (sqlFunctionExpression.Instance != null
                        && sqlFunctionExpression.InstancePropagatesNullability == true)
                    {
                        nullabilityPropagationElements.Add(sqlFunctionExpression.Instance);
                    }

                    if (!sqlFunctionExpression.IsNiladic)
                    {
                        for (var i = 0; i < sqlFunctionExpression.Arguments.Count; i++)
                        {
                            if (sqlFunctionExpression.ArgumentsPropagateNullability[i])
                            {
                                nullabilityPropagationElements.Add(sqlFunctionExpression.Arguments[i]);
                            }
                        }
                    }

                    // function(a, b) IS NULL -> a IS NULL || b IS NULL
                    // function(a, b) IS NOT NULL -> a IS NOT NULL && b IS NOT NULL
                    if (nullabilityPropagationElements.Count > 0)
                    {
                        var result = nullabilityPropagationElements
                            .Select(e => ProcessNullNotNull(
                                SqlExpressionFactory.MakeUnary(
                                    sqlUnaryExpression.OperatorType,
                                    e,
                                    sqlUnaryExpression.Type,
                                    sqlUnaryExpression.TypeMapping),
                                operandNullable))
                            .Aggregate((r, e) => SimplifyLogicalSqlBinaryExpression(
                                sqlUnaryExpression.OperatorType == ExpressionType.Equal
                                    ? SqlExpressionFactory.OrElse(r, e)
                                    : SqlExpressionFactory.AndAlso(r, e)));

                        return result;
                    }
                }
                break;
            }

            return sqlUnaryExpression;
        }

        // ?a == ?b -> [(a == b) && (a != null && b != null)] || (a == null && b == null))
        //
        // a | b | F1 = a == b | F2 = (a != null && b != null) | F3 = F1 && F2 |
        //   |   |             |                               |               |
        // 0 | 0 | 1           | 1                             | 1             |
        // 0 | 1 | 0           | 1                             | 0             |
        // 0 | N | N           | 0                             | 0             |
        // 1 | 0 | 0           | 1                             | 0             |
        // 1 | 1 | 1           | 1                             | 1             |
        // 1 | N | N           | 0                             | 0             |
        // N | 0 | N           | 0                             | 0             |
        // N | 1 | N           | 0                             | 0             |
        // N | N | N           | 0                             | 0             |
        //
        // a | b | F4 = (a == null && b == null) | Final = F3 OR F4 |
        //   |   |                               |                  |
        // 0 | 0 | 0                             | 1 OR 0 = 1       |
        // 0 | 1 | 0                             | 0 OR 0 = 0       |
        // 0 | N | 0                             | 0 OR 0 = 0       |
        // 1 | 0 | 0                             | 0 OR 0 = 0       |
        // 1 | 1 | 0                             | 1 OR 0 = 1       |
        // 1 | N | 0                             | 0 OR 0 = 0       |
        // N | 0 | 0                             | 0 OR 0 = 0       |
        // N | 1 | 0                             | 0 OR 0 = 0       |
        // N | N | 1                             | 0 OR 1 = 1       |
        private SqlExpression ExpandNullableEqualNullable(
            SqlExpression left,
            SqlExpression right,
            SqlExpression leftIsNull,
            SqlExpression leftIsNotNull,
            SqlExpression rightIsNull,
            SqlExpression rightIsNotNull)
            => SimplifyLogicalSqlBinaryExpression(
                SqlExpressionFactory.OrElse(
                    SimplifyLogicalSqlBinaryExpression(
                        SqlExpressionFactory.AndAlso(
                            SqlExpressionFactory.Equal(left, right),
                            SimplifyLogicalSqlBinaryExpression(
                                SqlExpressionFactory.AndAlso(leftIsNotNull, rightIsNotNull)))),
                    SimplifyLogicalSqlBinaryExpression(
                        SqlExpressionFactory.AndAlso(leftIsNull, rightIsNull))));

        // !(?a) == ?b -> [(a != b) && (a != null && b != null)] || (a == null && b == null)
        //
        // a | b | F1 = a != b | F2 = (a != null && b != null) | F3 = F1 && F2 |
        //   |   |             |                               |               |
        // 0 | 0 | 0           | 1                             | 0             |
        // 0 | 1 | 1           | 1                             | 1             |
        // 0 | N | N           | 0                             | 0             |
        // 1 | 0 | 1           | 1                             | 1             |
        // 1 | 1 | 0           | 1                             | 0             |
        // 1 | N | N           | 0                             | 0             |
        // N | 0 | N           | 0                             | 0             |
        // N | 1 | N           | 0                             | 0             |
        // N | N | N           | 0                             | 0             |
        //
        // a | b | F4 = (a == null && b == null) | Final = F3 OR F4 |
        //   |   |                               |                  |
        // 0 | 0 | 0                             | 0 OR 0 = 0       |
        // 0 | 1 | 0                             | 1 OR 0 = 1       |
        // 0 | N | 0                             | 0 OR 0 = 0       |
        // 1 | 0 | 0                             | 1 OR 0 = 1       |
        // 1 | 1 | 0                             | 0 OR 0 = 0       |
        // 1 | N | 0                             | 0 OR 0 = 0       |
        // N | 0 | 0                             | 0 OR 0 = 0       |
        // N | 1 | 0                             | 0 OR 0 = 0       |
        // N | N | 1                             | 0 OR 1 = 1       |
        private SqlExpression ExpandNegatedNullableEqualNullable(
            SqlExpression left,
            SqlExpression right,
            SqlExpression leftIsNull,
            SqlExpression leftIsNotNull,
            SqlExpression rightIsNull,
            SqlExpression rightIsNotNull)
            => SimplifyLogicalSqlBinaryExpression(
                SqlExpressionFactory.OrElse(
                    SimplifyLogicalSqlBinaryExpression(
                        SqlExpressionFactory.AndAlso(
                            SqlExpressionFactory.NotEqual(left, right),
                        SimplifyLogicalSqlBinaryExpression(
                            SqlExpressionFactory.AndAlso(leftIsNotNull, rightIsNotNull)))),
                    SimplifyLogicalSqlBinaryExpression(
                        SqlExpressionFactory.AndAlso(leftIsNull, rightIsNull))));

        // ?a == b -> (a == b) && (a != null)
        //
        // a | b | F1 = a == b | F2 = (a != null) | Final = F1 && F2 |
        //   |   |             |                  |                  |
        // 0 | 0 | 1           | 1                | 1                |
        // 0 | 1 | 0           | 1                | 0                |
        // 1 | 0 | 0           | 1                | 0                |
        // 1 | 1 | 1           | 1                | 1                |
        // N | 0 | N           | 0                | 0                |
        // N | 1 | N           | 0                | 0                |
        private SqlExpression ExpandNullableEqualNonNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNotNull)
            => SimplifyLogicalSqlBinaryExpression(
                SqlExpressionFactory.AndAlso(
                    SqlExpressionFactory.Equal(left, right),
                    leftIsNotNull));

        // !(?a) == b -> (a != b) && (a != null)
        //
        // a | b | F1 = a != b | F2 = (a != null) | Final = F1 && F2 |
        //   |   |             |                  |                  |
        // 0 | 0 | 0           | 1                | 0                |
        // 0 | 1 | 1           | 1                | 1                |
        // 1 | 0 | 1           | 1                | 1                |
        // 1 | 1 | 0           | 1                | 0                |
        // N | 0 | N           | 0                | 0                |
        // N | 1 | N           | 0                | 0                |
        private SqlExpression ExpandNegatedNullableEqualNonNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNotNull)
            => SimplifyLogicalSqlBinaryExpression(
                SqlExpressionFactory.AndAlso(
                    SqlExpressionFactory.NotEqual(left, right),
                    leftIsNotNull));

        // ?a != ?b -> [(a != b) || (a == null || b == null)] && (a != null || b != null)
        //
        // a | b | F1 = a != b | F2 = (a == null || b == null) | F3 = F1 || F2 |
        //   |   |             |                               |               |
        // 0 | 0 | 0           | 0                             | 0             |
        // 0 | 1 | 1           | 0                             | 1             |
        // 0 | N | N           | 1                             | 1             |
        // 1 | 0 | 1           | 0                             | 1             |
        // 1 | 1 | 0           | 0                             | 0             |
        // 1 | N | N           | 1                             | 1             |
        // N | 0 | N           | 1                             | 1             |
        // N | 1 | N           | 1                             | 1             |
        // N | N | N           | 1                             | 1             |
        //
        // a | b | F4 = (a != null || b != null) | Final = F3 && F4 |
        //   |   |                               |                  |
        // 0 | 0 | 1                             | 0 && 1 = 0       |
        // 0 | 1 | 1                             | 1 && 1 = 1       |
        // 0 | N | 1                             | 1 && 1 = 1       |
        // 1 | 0 | 1                             | 1 && 1 = 1       |
        // 1 | 1 | 1                             | 0 && 1 = 0       |
        // 1 | N | 1                             | 1 && 1 = 1       |
        // N | 0 | 1                             | 1 && 1 = 1       |
        // N | 1 | 1                             | 1 && 1 = 1       |
        // N | N | 0                             | 1 && 0 = 0       |
        private SqlExpression ExpandNullableNotEqualNullable(
            SqlExpression left,
            SqlExpression right,
            SqlExpression leftIsNull,
            SqlExpression leftIsNotNull,
            SqlExpression rightIsNull,
            SqlExpression rightIsNotNull)
            => SimplifyLogicalSqlBinaryExpression(
                SqlExpressionFactory.AndAlso(
                    SimplifyLogicalSqlBinaryExpression(
                        SqlExpressionFactory.OrElse(
                            SqlExpressionFactory.NotEqual(left, right),
                            SimplifyLogicalSqlBinaryExpression(
                                SqlExpressionFactory.OrElse(leftIsNull, rightIsNull)))),
                    SimplifyLogicalSqlBinaryExpression(
                        SqlExpressionFactory.OrElse(leftIsNotNull, rightIsNotNull))));

        // !(?a) != ?b -> [(a == b) || (a == null || b == null)] && (a != null || b != null)
        //
        // a | b | F1 = a == b | F2 = (a == null || b == null) | F3 = F1 || F2 |
        //   |   |             |                               |               |
        // 0 | 0 | 1           | 0                             | 1             |
        // 0 | 1 | 0           | 0                             | 0             |
        // 0 | N | N           | 1                             | 1             |
        // 1 | 0 | 0           | 0                             | 0             |
        // 1 | 1 | 1           | 0                             | 1             |
        // 1 | N | N           | 1                             | 1             |
        // N | 0 | N           | 1                             | 1             |
        // N | 1 | N           | 1                             | 1             |
        // N | N | N           | 1                             | 1             |
        //
        // a | b | F4 = (a != null || b != null) | Final = F3 && F4 |
        //   |   |                               |                  |
        // 0 | 0 | 1                             | 1 && 1 = 1       |
        // 0 | 1 | 1                             | 0 && 1 = 0       |
        // 0 | N | 1                             | 1 && 1 = 1       |
        // 1 | 0 | 1                             | 0 && 1 = 0       |
        // 1 | 1 | 1                             | 1 && 1 = 1       |
        // 1 | N | 1                             | 1 && 1 = 1       |
        // N | 0 | 1                             | 1 && 1 = 1       |
        // N | 1 | 1                             | 1 && 1 = 1       |
        // N | N | 0                             | 1 && 0 = 0       |
        private SqlExpression ExpandNegatedNullableNotEqualNullable(
            SqlExpression left,
            SqlExpression right,
            SqlExpression leftIsNull,
            SqlExpression leftIsNotNull,
            SqlExpression rightIsNull,
            SqlExpression rightIsNotNull)
            => SimplifyLogicalSqlBinaryExpression(
                SqlExpressionFactory.AndAlso(
                    SimplifyLogicalSqlBinaryExpression(
                        SqlExpressionFactory.OrElse(
                            SqlExpressionFactory.Equal(left, right),
                            SimplifyLogicalSqlBinaryExpression(
                                SqlExpressionFactory.OrElse(leftIsNull, rightIsNull)))),
                    SimplifyLogicalSqlBinaryExpression(
                        SqlExpressionFactory.OrElse(leftIsNotNull, rightIsNotNull))));

        // ?a != b -> (a != b) || (a == null)
        //
        // a | b | F1 = a != b | F2 = (a == null) | Final = F1 OR F2 |
        //   |   |             |                  |                  |
        // 0 | 0 | 0           | 0                | 0                |
        // 0 | 1 | 1           | 0                | 1                |
        // 1 | 0 | 1           | 0                | 1                |
        // 1 | 1 | 0           | 0                | 0                |
        // N | 0 | N           | 1                | 1                |
        // N | 1 | N           | 1                | 1                |
        private SqlExpression ExpandNullableNotEqualNonNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNull)
            => SimplifyLogicalSqlBinaryExpression(
                SqlExpressionFactory.OrElse(
                    SqlExpressionFactory.NotEqual(left, right),
                    leftIsNull));

        // !(?a) != b -> (a == b) || (a == null)
        //
        // a | b | F1 = a == b | F2 = (a == null) | F3 = F1 OR F2 |
        //   |   |             |                  |               |
        // 0 | 0 | 1           | 0                | 1             |
        // 0 | 1 | 0           | 0                | 0             |
        // 1 | 0 | 0           | 0                | 0             |
        // 1 | 1 | 1           | 0                | 1             |
        // N | 0 | N           | 1                | 1             |
        // N | 1 | N           | 1                | 1             |
        private SqlExpression ExpandNegatedNullableNotEqualNonNullable(
            SqlExpression left, SqlExpression right, SqlExpression leftIsNull)
            => SimplifyLogicalSqlBinaryExpression(
                SqlExpressionFactory.OrElse(
                    SqlExpressionFactory.Equal(left, right),
                    leftIsNull));
    }
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
}
