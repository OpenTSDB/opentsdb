// Generated from /Users/chiruvol/Desktop/Desktop/TechOtros/codebases/new_yamas/opentsdb/core/src/main/antlr4/MetricExpression.g4 by ANTLR 4.7
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link MetricExpressionParser}.
 */
public interface MetricExpressionListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link MetricExpressionParser#prog}.
	 * @param ctx the parse tree
	 */
	void enterProg(MetricExpressionParser.ProgContext ctx);
	/**
	 * Exit a parse tree produced by {@link MetricExpressionParser#prog}.
	 * @param ctx the parse tree
	 */
	void exitProg(MetricExpressionParser.ProgContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmetic}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterArithmetic(MetricExpressionParser.ArithmeticContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmetic}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitArithmetic(MetricExpressionParser.ArithmeticContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logical}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterLogical(MetricExpressionParser.LogicalContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logical}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitLogical(MetricExpressionParser.LogicalContext ctx);
	/**
	 * Enter a parse tree produced by the {@code relational}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterRelational(MetricExpressionParser.RelationalContext ctx);
	/**
	 * Exit a parse tree produced by the {@code relational}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitRelational(MetricExpressionParser.RelationalContext ctx);
	/**
	 * Enter a parse tree produced by the {@code ternary}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterTernary(MetricExpressionParser.TernaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code ternary}
	 * labeled alternative in {@link MetricExpressionParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitTernary(MetricExpressionParser.TernaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code paren_logical_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void enterParen_logical_rule(MetricExpressionParser.Paren_logical_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code paren_logical_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void exitParen_logical_rule(MetricExpressionParser.Paren_logical_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logical_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogical_operands_rule(MetricExpressionParser.Logical_operands_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logical_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogical_operands_rule(MetricExpressionParser.Logical_operands_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logical_expr_and_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogical_expr_and_rule(MetricExpressionParser.Logical_expr_and_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logical_expr_and_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogical_expr_and_rule(MetricExpressionParser.Logical_expr_and_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logical_expr_or_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogical_expr_or_rule(MetricExpressionParser.Logical_expr_or_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logical_expr_or_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogical_expr_or_rule(MetricExpressionParser.Logical_expr_or_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logical_expr_not_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogical_expr_not_rule(MetricExpressionParser.Logical_expr_not_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logical_expr_not_rule}
	 * labeled alternative in {@link MetricExpressionParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogical_expr_not_rule(MetricExpressionParser.Logical_expr_not_ruleContext ctx);
	/**
	 * Enter a parse tree produced by {@link MetricExpressionParser#logicalOperands}.
	 * @param ctx the parse tree
	 */
	void enterLogicalOperands(MetricExpressionParser.LogicalOperandsContext ctx);
	/**
	 * Exit a parse tree produced by {@link MetricExpressionParser#logicalOperands}.
	 * @param ctx the parse tree
	 */
	void exitLogicalOperands(MetricExpressionParser.LogicalOperandsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arith_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void enterArith_operands_rule(MetricExpressionParser.Arith_operands_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arith_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void exitArith_operands_rule(MetricExpressionParser.Arith_operands_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code paren_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void enterParen_arith_rule(MetricExpressionParser.Paren_arith_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code paren_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void exitParen_arith_rule(MetricExpressionParser.Paren_arith_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code minus_metric_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void enterMinus_metric_rule(MetricExpressionParser.Minus_metric_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code minus_metric_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void exitMinus_metric_rule(MetricExpressionParser.Minus_metric_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code mod_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void enterMod_arith_rule(MetricExpressionParser.Mod_arith_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code mod_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void exitMod_arith_rule(MetricExpressionParser.Mod_arith_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addsub_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void enterAddsub_arith_rule(MetricExpressionParser.Addsub_arith_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addsub_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void exitAddsub_arith_rule(MetricExpressionParser.Addsub_arith_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code divmul_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void enterDivmul_arith_rule(MetricExpressionParser.Divmul_arith_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code divmul_arith_rule}
	 * labeled alternative in {@link MetricExpressionParser#arthmeticExpression}.
	 * @param ctx the parse tree
	 */
	void exitDivmul_arith_rule(MetricExpressionParser.Divmul_arith_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmetic_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#arithmeticOperands}.
	 * @param ctx the parse tree
	 */
	void enterArithmetic_operands_rule(MetricExpressionParser.Arithmetic_operands_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmetic_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#arithmeticOperands}.
	 * @param ctx the parse tree
	 */
	void exitArithmetic_operands_rule(MetricExpressionParser.Arithmetic_operands_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code paren_relational_rule}
	 * labeled alternative in {@link MetricExpressionParser#relationalExpression}.
	 * @param ctx the parse tree
	 */
	void enterParen_relational_rule(MetricExpressionParser.Paren_relational_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code paren_relational_rule}
	 * labeled alternative in {@link MetricExpressionParser#relationalExpression}.
	 * @param ctx the parse tree
	 */
	void exitParen_relational_rule(MetricExpressionParser.Paren_relational_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code main_relational_rule}
	 * labeled alternative in {@link MetricExpressionParser#relationalExpression}.
	 * @param ctx the parse tree
	 */
	void enterMain_relational_rule(MetricExpressionParser.Main_relational_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code main_relational_rule}
	 * labeled alternative in {@link MetricExpressionParser#relationalExpression}.
	 * @param ctx the parse tree
	 */
	void exitMain_relational_rule(MetricExpressionParser.Main_relational_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code relational_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#relationalOperands}.
	 * @param ctx the parse tree
	 */
	void enterRelational_operands_rule(MetricExpressionParser.Relational_operands_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code relational_operands_rule}
	 * labeled alternative in {@link MetricExpressionParser#relationalOperands}.
	 * @param ctx the parse tree
	 */
	void exitRelational_operands_rule(MetricExpressionParser.Relational_operands_ruleContext ctx);
	/**
	 * Enter a parse tree produced by {@link MetricExpressionParser#logicop}.
	 * @param ctx the parse tree
	 */
	void enterLogicop(MetricExpressionParser.LogicopContext ctx);
	/**
	 * Exit a parse tree produced by {@link MetricExpressionParser#logicop}.
	 * @param ctx the parse tree
	 */
	void exitLogicop(MetricExpressionParser.LogicopContext ctx);
	/**
	 * Enter a parse tree produced by {@link MetricExpressionParser#relationalop}.
	 * @param ctx the parse tree
	 */
	void enterRelationalop(MetricExpressionParser.RelationalopContext ctx);
	/**
	 * Exit a parse tree produced by {@link MetricExpressionParser#relationalop}.
	 * @param ctx the parse tree
	 */
	void exitRelationalop(MetricExpressionParser.RelationalopContext ctx);
	/**
	 * Enter a parse tree produced by the {@code main_ternary_rule}
	 * labeled alternative in {@link MetricExpressionParser#ternaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterMain_ternary_rule(MetricExpressionParser.Main_ternary_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code main_ternary_rule}
	 * labeled alternative in {@link MetricExpressionParser#ternaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitMain_ternary_rule(MetricExpressionParser.Main_ternary_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code paren_ternary_rule}
	 * labeled alternative in {@link MetricExpressionParser#ternaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterParen_ternary_rule(MetricExpressionParser.Paren_ternary_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code paren_ternary_rule}
	 * labeled alternative in {@link MetricExpressionParser#ternaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitParen_ternary_rule(MetricExpressionParser.Paren_ternary_ruleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nested_ternary_rule}
	 * labeled alternative in {@link MetricExpressionParser#ternaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterNested_ternary_rule(MetricExpressionParser.Nested_ternary_ruleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nested_ternary_rule}
	 * labeled alternative in {@link MetricExpressionParser#ternaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitNested_ternary_rule(MetricExpressionParser.Nested_ternary_ruleContext ctx);
	/**
	 * Enter a parse tree produced by {@link MetricExpressionParser#ternaryOperands}.
	 * @param ctx the parse tree
	 */
	void enterTernaryOperands(MetricExpressionParser.TernaryOperandsContext ctx);
	/**
	 * Exit a parse tree produced by {@link MetricExpressionParser#ternaryOperands}.
	 * @param ctx the parse tree
	 */
	void exitTernaryOperands(MetricExpressionParser.TernaryOperandsContext ctx);
	/**
	 * Enter a parse tree produced by {@link MetricExpressionParser#and}.
	 * @param ctx the parse tree
	 */
	void enterAnd(MetricExpressionParser.AndContext ctx);
	/**
	 * Exit a parse tree produced by {@link MetricExpressionParser#and}.
	 * @param ctx the parse tree
	 */
	void exitAnd(MetricExpressionParser.AndContext ctx);
	/**
	 * Enter a parse tree produced by {@link MetricExpressionParser#or}.
	 * @param ctx the parse tree
	 */
	void enterOr(MetricExpressionParser.OrContext ctx);
	/**
	 * Exit a parse tree produced by {@link MetricExpressionParser#or}.
	 * @param ctx the parse tree
	 */
	void exitOr(MetricExpressionParser.OrContext ctx);
	/**
	 * Enter a parse tree produced by {@link MetricExpressionParser#not}.
	 * @param ctx the parse tree
	 */
	void enterNot(MetricExpressionParser.NotContext ctx);
	/**
	 * Exit a parse tree produced by {@link MetricExpressionParser#not}.
	 * @param ctx the parse tree
	 */
	void exitNot(MetricExpressionParser.NotContext ctx);
	/**
	 * Enter a parse tree produced by {@link MetricExpressionParser#modulo}.
	 * @param ctx the parse tree
	 */
	void enterModulo(MetricExpressionParser.ModuloContext ctx);
	/**
	 * Exit a parse tree produced by {@link MetricExpressionParser#modulo}.
	 * @param ctx the parse tree
	 */
	void exitModulo(MetricExpressionParser.ModuloContext ctx);
	/**
	 * Enter a parse tree produced by {@link MetricExpressionParser#metric}.
	 * @param ctx the parse tree
	 */
	void enterMetric(MetricExpressionParser.MetricContext ctx);
	/**
	 * Exit a parse tree produced by {@link MetricExpressionParser#metric}.
	 * @param ctx the parse tree
	 */
	void exitMetric(MetricExpressionParser.MetricContext ctx);
}