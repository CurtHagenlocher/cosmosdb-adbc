//! Translate DataFusion filter `Expr`s into Cosmos SQL `WHERE` predicates, and decide
//! which filters Cosmos can evaluate *exactly* (so DataFusion can drop them) versus which
//! must stay in the local plan.
//!
//! ## Correctness contract
//!
//! We only ever return [`TableProviderFilterPushDown::Exact`] for a filter when the Cosmos
//! predicate we generate selects **exactly** the same rows DataFusion's own evaluation
//! would. Claiming `Exact` on a mistranslated filter silently corrupts results, so the
//! supported set is deliberately tiny and every translation is proven row-equivalent below.
//!
//! ### The null / undefined / three-valued-logic trap
//!
//! A Cosmos field can be *undefined* (absent from the document) or the JSON value *null*.
//! After schema projection both decode to an Arrow `null`, and DataFusion applies SQL
//! three-valued logic: a comparison touching a null yields `NULL`, and a row passes `WHERE`
//! only when the predicate is `TRUE` (never `NULL`/`FALSE`).
//!
//! Cosmos SQL does *not* match this naively. `c["x"] != 5` is **true** for a JSON `null`
//! `x` (null and 5 differ), and `c["x"] NOT IN (1,2)` is likewise **true** for `null` — so
//! a raw translation would include rows DataFusion excludes. Range comparisons against a
//! `null`/typed-mismatched value are also implementation-defined.
//!
//! We sidestep all of it by wrapping every column comparison / `IN` leaf in a definedness
//! guard: `IS_DEFINED(c["x"]) AND NOT IS_NULL(c["x"]) AND (<raw comparison>)`. This forces
//! each leaf to a *two-valued* result — `true` only when the column is a real, non-null
//! value satisfying the comparison, which is precisely when DataFusion's comparison is
//! `TRUE`. Two-valued leaves then compose through `AND`/`OR` identically to DataFusion, so
//! the whole predicate is row-equivalent regardless of Cosmos's null-ordering quirks.

use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;

/// A Cosmos property accessor `c["field"]`, with the field name escaped for a
/// double-quoted Cosmos string. Backslash must be escaped before the quote character.
pub(crate) fn cosmos_property(name: &str) -> String {
    let escaped = name.replace('\\', "\\\\").replace('"', "\\\"");
    format!("c[\"{escaped}\"]")
}

/// Per-filter pushdown decision: `Exact` when we fully translate the filter into a Cosmos
/// predicate (DataFusion may then drop it), `Unsupported` otherwise (DataFusion keeps it).
pub(crate) fn pushdown_decisions(filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
    filters
        .iter()
        .map(|f| {
            if translate(f).is_some() {
                TableProviderFilterPushDown::Exact
            } else {
                TableProviderFilterPushDown::Unsupported
            }
        })
        .collect()
}

/// Translate a filter into a Cosmos SQL predicate string, or `None` if it falls outside the
/// conservative supported set. The same function backs [`pushdown_decisions`], so a filter
/// marked `Exact` is guaranteed to translate here.
pub(crate) fn translate(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Alias(a) => translate(&a.expr),
        Expr::BinaryExpr(b) => translate_binary(&b.op, &b.left, &b.right),
        Expr::InList(il) => translate_in_list(&il.expr, &il.list, il.negated),
        Expr::IsNull(inner) => {
            let prop = cosmos_property(as_column(inner)?);
            // Matches Arrow null for both an absent field and a JSON null.
            Some(format!("(NOT IS_DEFINED({prop}) OR IS_NULL({prop}))"))
        }
        Expr::IsNotNull(inner) => {
            let prop = cosmos_property(as_column(inner)?);
            Some(format!("(IS_DEFINED({prop}) AND NOT IS_NULL({prop}))"))
        }
        _ => None,
    }
}

fn translate_binary(op: &Operator, left: &Expr, right: &Expr) -> Option<String> {
    match op {
        Operator::And | Operator::Or => {
            let l = translate(left)?;
            let r = translate(right)?;
            let kw = if *op == Operator::And { "AND" } else { "OR" };
            Some(format!("({l} {kw} {r})"))
        }
        Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt
        | Operator::GtEq => {
            // Accept `column <op> literal` in either order, flipping the operator when the
            // column is on the right so the emitted comparison keeps its meaning.
            let (col, lit, cmp) = if let (Some(c), Some(l)) = (as_column(left), as_literal(right))
            {
                (c, l, *op)
            } else if let (Some(c), Some(l)) = (as_column(right), as_literal(left)) {
                (c, l, flip(*op)?)
            } else {
                return None;
            };
            Some(guarded_comparison(col, cmp_symbol(&cmp)?, &lit))
        }
        _ => None,
    }
}

fn translate_in_list(expr: &Expr, list: &[Expr], negated: bool) -> Option<String> {
    let col = as_column(expr)?;
    if list.is_empty() {
        return None;
    }
    let items: Option<Vec<String>> = list.iter().map(as_literal).collect();
    let joined = items?.join(", ");
    let prop = cosmos_property(col);
    let inner = if negated {
        format!("{prop} NOT IN ({joined})")
    } else {
        format!("{prop} IN ({joined})")
    };
    Some(guard(col, &inner))
}

/// Wrap a raw column comparison in the definedness guard (see module docs).
fn guarded_comparison(col: &str, symbol: &str, lit: &str) -> String {
    let prop = cosmos_property(col);
    guard(col, &format!("{prop} {symbol} {lit}"))
}

/// `(IS_DEFINED(c["x"]) AND NOT IS_NULL(c["x"]) AND (<inner>))` — forces `inner` to a
/// two-valued result matching DataFusion's "null excludes the row" semantics.
fn guard(col: &str, inner: &str) -> String {
    let prop = cosmos_property(col);
    format!("(IS_DEFINED({prop}) AND NOT IS_NULL({prop}) AND ({inner}))")
}

/// The bare column name of a plain column reference (peeking through an alias), else `None`.
fn as_column(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(c) => Some(c.name.as_str()),
        Expr::Alias(a) => as_column(&a.expr),
        _ => None,
    }
}

/// Render a literal to a Cosmos SQL constant, or `None` for types we don't push (dates,
/// decimals, binary, typed nulls, non-finite floats, …).
fn as_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(sv, _) => render_scalar(sv),
        Expr::Alias(a) => as_literal(&a.expr),
        _ => None,
    }
}

fn render_scalar(sv: &ScalarValue) -> Option<String> {
    match sv {
        ScalarValue::Boolean(Some(b)) => Some(if *b { "true".into() } else { "false".into() }),
        ScalarValue::Int8(Some(n)) => Some(n.to_string()),
        ScalarValue::Int16(Some(n)) => Some(n.to_string()),
        ScalarValue::Int32(Some(n)) => Some(n.to_string()),
        ScalarValue::Int64(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt8(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt16(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt32(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt64(Some(n)) => Some(n.to_string()),
        // Cosmos numbers are JSON doubles; Rust's `to_string` round-trips finite floats.
        ScalarValue::Float32(Some(f)) if f.is_finite() => Some((*f as f64).to_string()),
        ScalarValue::Float64(Some(f)) if f.is_finite() => Some(f.to_string()),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(quote_string(s)),
        // Typed nulls, NaN/Inf, and every other type stay in the local plan.
        _ => None,
    }
}

/// A single-quoted Cosmos string literal with JSON-style escaping.
fn quote_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        match ch {
            '\'' => out.push_str("\\'"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

fn cmp_symbol(op: &Operator) -> Option<&'static str> {
    Some(match op {
        Operator::Eq => "=",
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        _ => return None,
    })
}

/// Flip a comparison operator for `literal <op> column` → `column <flipped> literal`.
fn flip(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Eq => Operator::Eq,
        Operator::NotEq => Operator::NotEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::TableProviderFilterPushDown as PD;
    use datafusion::prelude::{col, lit};

    fn qcol(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }

    #[test]
    fn property_escaping() {
        assert_eq!(cosmos_property("name"), r#"c["name"]"#);
        assert_eq!(cosmos_property(r#"we"ird"#), r#"c["we\"ird"]"#);
        assert_eq!(cosmos_property(r"back\slash"), r#"c["back\\slash"]"#);
    }

    #[test]
    fn simple_comparison_is_guarded() {
        let e = qcol("mergeOrder").gt(lit(25i64));
        assert_eq!(
            translate(&e).unwrap(),
            r#"(IS_DEFINED(c["mergeOrder"]) AND NOT IS_NULL(c["mergeOrder"]) AND (c["mergeOrder"] > 25))"#
        );
    }

    #[test]
    fn literal_on_left_flips_operator() {
        // 25 < mergeOrder  ==>  mergeOrder > 25
        let e = lit(25i64).lt(qcol("mergeOrder"));
        assert_eq!(
            translate(&e).unwrap(),
            r#"(IS_DEFINED(c["mergeOrder"]) AND NOT IS_NULL(c["mergeOrder"]) AND (c["mergeOrder"] > 25))"#
        );
    }

    #[test]
    fn not_equal_and_string_literal() {
        let e = qcol("pk").not_eq(lit("group1"));
        assert_eq!(
            translate(&e).unwrap(),
            r#"(IS_DEFINED(c["pk"]) AND NOT IS_NULL(c["pk"]) AND (c["pk"] != 'group1'))"#
        );
    }

    #[test]
    fn string_literal_is_escaped() {
        let e = qcol("name").eq(lit("O'Brien\\x"));
        assert_eq!(
            translate(&e).unwrap(),
            r#"(IS_DEFINED(c["name"]) AND NOT IS_NULL(c["name"]) AND (c["name"] = 'O\'Brien\\x'))"#
        );
    }

    #[test]
    fn boolean_literal() {
        let e = qcol("active").eq(lit(true));
        assert_eq!(
            translate(&e).unwrap(),
            r#"(IS_DEFINED(c["active"]) AND NOT IS_NULL(c["active"]) AND (c["active"] = true))"#
        );
    }

    #[test]
    fn in_list_positive_and_negated() {
        let pos = col("pk").in_list(vec![lit("a"), lit("b")], false);
        assert_eq!(
            translate(&pos).unwrap(),
            r#"(IS_DEFINED(c["pk"]) AND NOT IS_NULL(c["pk"]) AND (c["pk"] IN ('a', 'b')))"#
        );
        let neg = col("pk").in_list(vec![lit("a"), lit("b")], true);
        assert_eq!(
            translate(&neg).unwrap(),
            r#"(IS_DEFINED(c["pk"]) AND NOT IS_NULL(c["pk"]) AND (c["pk"] NOT IN ('a', 'b')))"#
        );
    }

    #[test]
    fn is_null_and_is_not_null() {
        let n = qcol("x").is_null();
        assert_eq!(
            translate(&n).unwrap(),
            r#"(NOT IS_DEFINED(c["x"]) OR IS_NULL(c["x"]))"#
        );
        let nn = qcol("x").is_not_null();
        assert_eq!(
            translate(&nn).unwrap(),
            r#"(IS_DEFINED(c["x"]) AND NOT IS_NULL(c["x"]))"#
        );
    }

    #[test]
    fn and_or_compose() {
        let e = qcol("a").gt(lit(1i64)).and(qcol("b").lt(lit(2i64)));
        assert_eq!(
            translate(&e).unwrap(),
            r#"((IS_DEFINED(c["a"]) AND NOT IS_NULL(c["a"]) AND (c["a"] > 1)) AND (IS_DEFINED(c["b"]) AND NOT IS_NULL(c["b"]) AND (c["b"] < 2)))"#
        );
        let o = qcol("a").eq(lit(1i64)).or(qcol("b").eq(lit(2i64)));
        assert!(translate(&o).unwrap().contains(" OR "));
    }

    #[test]
    fn unsupported_predicates_are_rejected() {
        // column-vs-column comparison
        assert!(translate(&qcol("a").eq(qcol("b"))).is_none());
        // LIKE
        assert!(translate(&qcol("a").like(lit("x%"))).is_none());
        // NOT over a column
        assert!(translate(&!qcol("a")).is_none());
        // BETWEEN (outside the supported set)
        assert!(translate(&qcol("a").between(lit(1i64), lit(9i64))).is_none());
        // an AND where one arm is unsupported taints the whole predicate
        let mixed = qcol("a").gt(lit(1i64)).and(qcol("a").eq(qcol("b")));
        assert!(translate(&mixed).is_none());
    }

    #[test]
    fn pushdown_decisions_track_translatability() {
        let ok = qcol("a").gt(lit(1i64));
        let bad = qcol("a").eq(qcol("b"));
        let decisions = pushdown_decisions(&[&ok, &bad]);
        assert_eq!(decisions, vec![PD::Exact, PD::Unsupported]);
    }
}
