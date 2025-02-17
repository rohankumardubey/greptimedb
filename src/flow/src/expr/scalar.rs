// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, BTreeSet};

use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use serde::{Deserialize, Serialize};

use crate::expr::error::{
    EvalError, InvalidArgumentSnafu, OptimizeSnafu, UnsupportedTemporalFilterSnafu,
};
use crate::expr::func::{BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};

/// A scalar expression, which can be evaluated to a value.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    /// Extra type info to know original type even when it is null
    Literal(Value, ConcreteDataType),
    /// A call to an unmaterializable function.
    ///
    /// These functions cannot be evaluated by `ScalarExpr::eval`. They must
    /// be transformed away by a higher layer.
    CallUnmaterializable(UnmaterializableFunc),
    CallUnary {
        func: UnaryFunc,
        expr: Box<ScalarExpr>,
    },
    CallBinary {
        func: BinaryFunc,
        expr1: Box<ScalarExpr>,
        expr2: Box<ScalarExpr>,
    },
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<ScalarExpr>,
    },
    /// Conditionally evaluated expressions.
    ///
    /// It is important that `then` and `els` only be evaluated if
    /// `cond` is true or not, respectively. This is the only way
    /// users can guard execution (other logical operator do not
    /// short-circuit) and we need to preserve that.
    If {
        cond: Box<ScalarExpr>,
        then: Box<ScalarExpr>,
        els: Box<ScalarExpr>,
    },
}

impl ScalarExpr {
    pub fn call_unary(self, func: UnaryFunc) -> Self {
        ScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        ScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    pub fn eval(&self, values: &[Value]) -> Result<Value, EvalError> {
        match self {
            ScalarExpr::Column(index) => Ok(values[*index].clone()),
            ScalarExpr::Literal(row_res, _ty) => Ok(row_res.clone()),
            ScalarExpr::CallUnmaterializable(f) => OptimizeSnafu {
                reason: "Can't eval unmaterializable function".to_string(),
            }
            .fail(),
            ScalarExpr::CallUnary { func, expr } => func.eval(values, expr),
            ScalarExpr::CallBinary { func, expr1, expr2 } => func.eval(values, expr1, expr2),
            ScalarExpr::CallVariadic { func, exprs } => func.eval(values, exprs),
            ScalarExpr::If { cond, then, els } => match cond.eval(values) {
                Ok(Value::Boolean(true)) => then.eval(values),
                Ok(Value::Boolean(false)) => els.eval(values),
                _ => InvalidArgumentSnafu {
                    reason: "if condition must be boolean".to_string(),
                }
                .fail(),
            },
        }
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute(&mut self, permutation: &[usize]) {
        self.visit_mut_post_nolimit(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[*old_i];
            }
        });
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute_map(&mut self, permutation: &BTreeMap<usize, usize>) {
        self.visit_mut_post_nolimit(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[old_i];
            }
        });
    }

    /// Returns the set of columns that are referenced by `self`.
    pub fn get_all_ref_columns(&self) -> BTreeSet<usize> {
        let mut support = BTreeSet::new();
        self.visit_post_nolimit(&mut |e| {
            if let ScalarExpr::Column(i) = e {
                support.insert(*i);
            }
        });
        support
    }

    pub fn as_literal(&self) -> Option<Value> {
        if let ScalarExpr::Literal(lit, _column_type) = self {
            Some(lit.clone())
        } else {
            None
        }
    }

    pub fn is_literal(&self) -> bool {
        matches!(self, ScalarExpr::Literal(..))
    }

    pub fn is_literal_true(&self) -> bool {
        Some(Value::Boolean(true)) == self.as_literal()
    }

    pub fn is_literal_false(&self) -> bool {
        Some(Value::Boolean(false)) == self.as_literal()
    }

    pub fn is_literal_null(&self) -> bool {
        Some(Value::Null) == self.as_literal()
    }

    pub fn literal_null() -> Self {
        ScalarExpr::Literal(Value::Null, ConcreteDataType::null_datatype())
    }

    pub fn literal(res: Value, typ: ConcreteDataType) -> Self {
        ScalarExpr::Literal(res, typ)
    }

    pub fn literal_false() -> Self {
        ScalarExpr::Literal(Value::Boolean(false), ConcreteDataType::boolean_datatype())
    }

    pub fn literal_true() -> Self {
        ScalarExpr::Literal(Value::Boolean(true), ConcreteDataType::boolean_datatype())
    }
}

impl ScalarExpr {
    /// visit post-order without stack call limit, but may cause stack overflow
    fn visit_post_nolimit<F>(&self, f: &mut F)
    where
        F: FnMut(&Self),
    {
        self.visit_children(|e| e.visit_post_nolimit(f));
        f(self);
    }

    fn visit_children<F>(&self, mut f: F)
    where
        F: FnMut(&Self),
    {
        match self {
            ScalarExpr::Column(_)
            | ScalarExpr::Literal(_, _)
            | ScalarExpr::CallUnmaterializable(_) => (),
            ScalarExpr::CallUnary { expr, .. } => f(expr),
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            ScalarExpr::If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
        }
    }

    fn visit_mut_post_nolimit<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit_mut_children(|e: &mut Self| e.visit_mut_post_nolimit(f));
        f(self);
    }

    fn visit_mut_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Self),
    {
        match self {
            ScalarExpr::Column(_)
            | ScalarExpr::Literal(_, _)
            | ScalarExpr::CallUnmaterializable(_) => (),
            ScalarExpr::CallUnary { expr, .. } => f(expr),
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            ScalarExpr::If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
        }
    }
}

impl ScalarExpr {
    /// if expr contains function `Now`
    pub fn contains_temporal(&self) -> bool {
        let mut contains = false;
        self.visit_post_nolimit(&mut |e| {
            if let ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now) = e {
                contains = true;
            }
        });
        contains
    }

    /// extract lower or upper bound of `Now` for expr, where `lower bound <= expr < upper bound`
    ///
    /// returned bool indicates whether the bound is upper bound:
    ///
    /// false for lower bound, true for upper bound
    /// TODO(discord9): allow simple transform like `now() + a < b` to `now() < b - a`
    pub fn extract_bound(&self) -> Result<(Option<Self>, Option<Self>), EvalError> {
        let unsupported_err = |msg: &str| {
            UnsupportedTemporalFilterSnafu {
                reason: msg.to_string(),
            }
            .fail()
        };

        let Self::CallBinary {
            mut func,
            mut expr1,
            mut expr2,
        } = self.clone()
        else {
            return unsupported_err("Not a binary expression");
        };

        // TODO: support simple transform like `now() + a < b` to `now() < b - a`

        let expr1_is_now = *expr1 == ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now);
        let expr2_is_now = *expr2 == ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now);

        if !(expr1_is_now ^ expr2_is_now) {
            return unsupported_err("None of the sides of the comparison is `now()`");
        }

        if expr2_is_now {
            std::mem::swap(&mut expr1, &mut expr2);
            func = BinaryFunc::reverse_compare(&func)?;
        }

        let step = |expr: ScalarExpr| expr.call_unary(UnaryFunc::StepTimestamp);
        match func {
            // now == expr2 -> now <= expr2 && now < expr2 + 1
            BinaryFunc::Eq => Ok((Some(*expr2.clone()), Some(step(*expr2)))),
            // now < expr2 -> now < expr2
            BinaryFunc::Lt => Ok((None, Some(*expr2))),
            // now <= expr2 -> now < expr2 + 1
            BinaryFunc::Lte => Ok((None, Some(step(*expr2)))),
            // now > expr2 -> now >= expr2 + 1
            BinaryFunc::Gt => Ok((Some(step(*expr2)), None)),
            // now >= expr2 -> now >= expr2
            BinaryFunc::Gte => Ok((Some(*expr2), None)),
            _ => unreachable!("Already checked"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_extract_bound() {
        let test_list: [(ScalarExpr, Result<_, EvalError>); 5] = [
            // col(0) == now
            (
                ScalarExpr::CallBinary {
                    func: BinaryFunc::Eq,
                    expr1: Box::new(ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)),
                    expr2: Box::new(ScalarExpr::Column(0)),
                },
                Ok((
                    Some(ScalarExpr::Column(0)),
                    Some(ScalarExpr::CallUnary {
                        func: UnaryFunc::StepTimestamp,
                        expr: Box::new(ScalarExpr::Column(0)),
                    }),
                )),
            ),
            // now < col(0)
            (
                ScalarExpr::CallBinary {
                    func: BinaryFunc::Lt,
                    expr1: Box::new(ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)),
                    expr2: Box::new(ScalarExpr::Column(0)),
                },
                Ok((None, Some(ScalarExpr::Column(0)))),
            ),
            // now <= col(0)
            (
                ScalarExpr::CallBinary {
                    func: BinaryFunc::Lte,
                    expr1: Box::new(ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)),
                    expr2: Box::new(ScalarExpr::Column(0)),
                },
                Ok((
                    None,
                    Some(ScalarExpr::CallUnary {
                        func: UnaryFunc::StepTimestamp,
                        expr: Box::new(ScalarExpr::Column(0)),
                    }),
                )),
            ),
            // now > col(0) -> now >= col(0) + 1
            (
                ScalarExpr::CallBinary {
                    func: BinaryFunc::Gt,
                    expr1: Box::new(ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)),
                    expr2: Box::new(ScalarExpr::Column(0)),
                },
                Ok((
                    Some(ScalarExpr::CallUnary {
                        func: UnaryFunc::StepTimestamp,
                        expr: Box::new(ScalarExpr::Column(0)),
                    }),
                    None,
                )),
            ),
            // now >= col(0)
            (
                ScalarExpr::CallBinary {
                    func: BinaryFunc::Gte,
                    expr1: Box::new(ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)),
                    expr2: Box::new(ScalarExpr::Column(0)),
                },
                Ok((Some(ScalarExpr::Column(0)), None)),
            ),
        ];
        for (expr, expected) in test_list.into_iter() {
            let actual = expr.extract_bound();
            // EvalError is not Eq, so we need to compare the error message
            match (actual, expected) {
                (Ok(l), Ok(r)) => assert_eq!(l, r),
                (Err(l), Err(r)) => assert!(matches!(l, r)),
                (l, r) => panic!("expected: {:?}, actual: {:?}", r, l),
            }
        }
    }
}
