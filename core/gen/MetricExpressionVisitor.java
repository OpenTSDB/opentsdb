// Generated from /Users/chiruvol/Desktop/Desktop/TechOtros/codebases/new_yamas/opentsdb/core/src/main/antlr4/MetricExpression.g4 by ANTLR 4.7
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link MetricExpressionParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface MetricExpressionVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link MetricExpressionParser#prog}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitProg(MetricExpressionParser.ProgContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmetic}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmetic(MetricExpressionParser.ArithmeticContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logical}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogical(MetricExpressionParser.LogicalContext ctx);
	/**
	 * Visit a parse tree produced by the {@code relational}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelational(MetricExpressionParser.RelationalContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ternary}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTernary(MetricExpressionParser.TernaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code paren_logical_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParen_logical_rule(MetricExpressionParser.Paren_logical_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logical_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogical_operands_rule(MetricExpressionParser.Logical_operands_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logical_expr_and_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogical_expr_and_rule(MetricExpressionParser.Logical_expr_and_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logical_expr_or_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogical_expr_or_rule(MetricExpressionParser.Logical_expr_or_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logical_expr_not_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogical_expr_not_rule(MetricExpressionParser.Logical_expr_not_ruleContext ctx);
	/**
	 * Visit a parse tree produced by {@link MetricExpressionParser#logicalOperands}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalOperands(MetricExpressionParser.LogicalOperandsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arith_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArith_operands_rule(MetricExpressionParser.Arith_operands_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code paren_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParen_arith_rule(MetricExpressionParser.Paren_arith_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code minus_metric_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMinus_metric_rule(MetricExpressionParser.Minus_metric_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code mod_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMod_arith_rule(MetricExpressionParser.Mod_arith_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code addsub_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAddsub_arith_rule(MetricExpressionParser.Addsub_arith_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code divmul_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDivmul_arith_rule(MetricExpressionParser.Divmul_arith_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmetic_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#arithmeticOperands}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmetic_operands_rule(MetricExpressionParser.Arithmetic_operands_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code paren_relational_rule}
	 * labeled alternative in {@link MetricExpressionParser#relationalExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParen_relational_rule(MetricExpressionParser.Paren_relational_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code main_relational_rule}
	 * labeled alternative in {@link MetricExpressionParser#relationalExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMain_relational_rule(MetricExpressionParser.Main_relational_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code relational_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#relationalOperands}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelational_operands_rule(MetricExpressionParser.Relational_operands_ruleContext ctx);
	/**
	 * Visit a parse tree produced by {@link MetricExpressionParser#logicop}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicop(MetricExpressionParser.LogicopContext ctx);
	/**
	 * Visit a parse tree produced by {@link MetricExpressionParser#relationalop}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelationalop(MetricExpressionParser.RelationalopContext ctx);
	/**
	 * Visit a parse tree produced by the {@code main_ternary_rule}
	 * labeled alternative in {@link MetricExpressionParser#ternaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMain_ternary_rule(MetricExpressionParser.Main_ternary_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code paren_ternary_rule}
	 * labeled alternative in {@link MetricExpressionParser#ternaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParen_ternary_rule(MetricExpressionParser.Paren_ternary_ruleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code nested_ternary_rule}
	 * labeled alternative in {@link MetricExpressionParser#ternaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNested_ternary_rule(MetricExpressionParser.Nested_ternary_ruleContext ctx);
	/**
	 * Visit a parse tree produced by {@link MetricExpressionParser#ternaryOperands}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTernaryOperands(MetricExpressionParser.TernaryOperandsContext ctx);
	/**
	 * Visit a parse tree produced by {@link MetricExpressionParser#and}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnd(MetricExpressionParser.AndContext ctx);
	/**
	 * Visit a parse tree produced by {@link MetricExpressionParser#or}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOr(MetricExpressionParser.OrContext ctx);
	/**
	 * Visit a parse tree produced by {@link MetricExpressionParser#not}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNot(MetricExpressionParser.NotContext ctx);
	/**
	 * Visit a parse tree produced by {@link MetricExpressionParser#modulo}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitModulo(MetricExpressionParser.ModuloContext ctx);
	/**
	 * Visit a parse tree produced by {@link MetricExpressionParser#metric}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMetric(MetricExpressionParser.MetricContext ctx);
}