use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::AppError;
use crate::workflow::definition::{Comparison, Condition, ValueExpr};
use crate::workflow::{ConditionBackend, WorkflowState};

impl ValueExpr {
    pub(crate) fn evaluate<C: ConditionBackend>(
        &self,
        state: &WorkflowState,
        bindings: &[C],
    ) -> Result<Value, AppError> {
        self.evaluate_value(state, bindings)?.into_json()
    }

    fn evaluate_value<C: ConditionBackend>(
        &self,
        state: &WorkflowState,
        bindings: &[C],
    ) -> Result<ExpressionValue, AppError> {
        match self {
            Self::Literal { value } => ExpressionValue::from_json(value.clone()),
            Self::State { path } => {
                crate::workflow::definition::JsonBoundary::at_path(state.values(), path)
                    .cloned()
                    .ok_or_else(|| {
                        AppError::InvalidInput(
                            "workflow expression references missing state".to_owned(),
                        )
                    })
                    .and_then(ExpressionValue::from_json)
            }
            Self::Length { value } => {
                let length = match value.evaluate(state, bindings)? {
                    Value::Array(value) => value.len(),
                    Value::Object(value) => value.len(),
                    Value::String(value) => value.chars().count(),
                    _ => {
                        return Err(AppError::InvalidInput(
                            "length expression requires an array, object, or string".to_owned(),
                        ));
                    }
                };
                Ok(ExpressionValue::Number(Scalar::Integer(length as i128)))
            }
            Self::Positive { value } => value
                .evaluate_value(state, bindings)?
                .into_number()
                .map(ExpressionValue::Number),
            Self::Negative { value } => value
                .evaluate_value(state, bindings)?
                .into_number()?
                .negate()
                .map(ExpressionValue::Number),
            Self::Not { value } => Ok(ExpressionValue::Json(Value::Bool(
                !value.evaluate_value(state, bindings)?.is_truthy(),
            ))),
            Self::And { values } | Self::Or { values } => {
                let (first, rest) = values.split_first().ok_or_else(|| {
                    AppError::InvalidInput("boolean expression cannot be empty".to_owned())
                })?;
                let mut current = first.evaluate_value(state, bindings)?;
                for value in rest {
                    if current.is_truthy() == matches!(self, Self::Or { .. }) {
                        return Ok(current);
                    }
                    current = value.evaluate_value(state, bindings)?;
                }
                Ok(current)
            }
            Self::Conditional {
                when,
                then,
                otherwise,
            } => {
                if when.evaluate_unvalidated(state, bindings)? {
                    then.evaluate_value(state, bindings)
                } else {
                    otherwise.evaluate_value(state, bindings)
                }
            }
            Self::Attribute { value, name } => {
                value.evaluate_value(state, bindings)?.attribute(name)
            }
            Self::Index { value, index } => {
                let value = value.evaluate_value(state, bindings)?;
                let index = index.evaluate_value(state, bindings)?;
                value.index(index)
            }
            Self::Slice {
                value,
                start,
                stop,
                stride,
            } => {
                let value = SliceValue::new(value.evaluate_value(state, bindings)?)?;
                // The pin rejects the container before evaluating bounds, then
                // evaluates stride before start and stop, including on empty input.
                let stride = match stride {
                    Some(stride) => stride.evaluate_value(state, bindings)?.into_index()?,
                    None => 1,
                };
                if stride == 0 {
                    return Err(AppError::InvalidInput(
                        "slice step cannot be zero".to_owned(),
                    ));
                }
                let start = start
                    .as_ref()
                    .map(|value| value.evaluate_value(state, bindings)?.into_index())
                    .transpose()?;
                let stop = stop
                    .as_ref()
                    .map(|value| value.evaluate_value(state, bindings)?.into_index())
                    .transpose()?;
                let bounds = SliceRange::new(value.len(), start, stop, stride)?;
                value.slice(bounds)
            }
            Self::Add { left, right } => {
                Self::arithmetic(left, right, state, bindings, Arithmetic::Add)
            }
            Self::Subtract { left, right } => {
                Self::arithmetic(left, right, state, bindings, Arithmetic::Subtract)
            }
            Self::Multiply { left, right } => {
                Self::arithmetic(left, right, state, bindings, Arithmetic::Multiply)
            }
            Self::Divide { left, right } => {
                Self::arithmetic(left, right, state, bindings, Arithmetic::Divide)
            }
            Self::Remainder { left, right } => {
                Self::arithmetic(left, right, state, bindings, Arithmetic::Remainder)
            }
            Self::FloorDivide { left, right } => {
                Self::arithmetic(left, right, state, bindings, Arithmetic::FloorDivide)
            }
            Self::Power { left, right } => {
                Self::arithmetic(left, right, state, bindings, Arithmetic::Power)
            }
        }
    }

    pub(super) fn evaluate_map<C: ConditionBackend>(
        expressions: &BTreeMap<String, Self>,
        state: &WorkflowState,
        bindings: &[C],
    ) -> Result<Map<String, Value>, AppError> {
        let mut output = Map::new();
        for (key, expression) in expressions {
            output.insert(key.clone(), expression.evaluate(state, bindings)?);
        }
        Ok(output)
    }

    fn arithmetic<C: ConditionBackend>(
        left: &Self,
        right: &Self,
        state: &WorkflowState,
        bindings: &[C],
        operation: Arithmetic,
    ) -> Result<ExpressionValue, AppError> {
        let left = left.evaluate_value(state, bindings)?;
        let right = right.evaluate_value(state, bindings)?;
        operation
            .apply(left.into_number()?, right.into_number()?)
            .map(ExpressionValue::Number)
    }
}

// Intermediate integers and IEEE results stay outside JSON until the final
// value boundary; converting every nested operation would lose rurr's i128
// arithmetic and the distinction between integral floats and integers.
enum ExpressionValue {
    Number(Scalar),
    Json(Value),
}

// Strings use Unicode scalar positions, as in the pinned evaluator; arrays
// keep their original JSON members without conversion to another value tree.
enum SliceValue {
    Array(Vec<Value>),
    Text(Vec<char>),
}

impl SliceValue {
    fn new(value: ExpressionValue) -> Result<Self, AppError> {
        match value {
            ExpressionValue::Json(Value::Array(value)) => Ok(Self::Array(value)),
            ExpressionValue::Json(Value::String(value)) => Ok(Self::Text(value.chars().collect())),
            _ => Err(AppError::InvalidInput(
                "slice requires an array or string".to_owned(),
            )),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Array(value) => value.len(),
            Self::Text(value) => value.len(),
        }
    }

    fn slice(self, mut bounds: SliceRange) -> Result<ExpressionValue, AppError> {
        let value = match self {
            Self::Array(value) => {
                let mut output = Vec::new();
                while let Some(index) = bounds.next_index()? {
                    output.push(value[index].clone());
                }
                Value::Array(output)
            }
            Self::Text(value) => {
                let mut output = String::new();
                while let Some(index) = bounds.next_index()? {
                    output.push(value[index]);
                }
                Value::String(output)
            }
        };
        Ok(ExpressionValue::Json(value))
    }
}

struct SliceRange {
    current: i128,
    stop: i128,
    stride: i128,
}

impl SliceRange {
    fn new(
        length: usize,
        start: Option<i128>,
        stop: Option<i128>,
        stride: i128,
    ) -> Result<Self, AppError> {
        let length = length as i128;
        let (minimum, maximum, first, last) = if stride > 0 {
            (0, length, 0, length)
        } else {
            (-1, length - 1, length - 1, -1)
        };
        let mut bounds = [first, last];
        for (bound, input) in bounds.iter_mut().zip([start, stop]) {
            if let Some(mut input) = input {
                if input < 0 {
                    input = input.checked_add(length).ok_or_else(Arithmetic::overflow)?;
                }
                *bound = input.clamp(minimum, maximum);
            }
        }
        Ok(Self {
            current: bounds[0],
            stop: bounds[1],
            stride,
        })
    }

    fn next_index(&mut self) -> Result<Option<usize>, AppError> {
        if (self.stride > 0 && self.current >= self.stop)
            || (self.stride < 0 && self.current <= self.stop)
        {
            return Ok(None);
        }
        let index = usize::try_from(self.current).map_err(|_| Arithmetic::overflow())?;
        // Rurr checks the advance even after its final selected element.
        self.current = self
            .current
            .checked_add(self.stride)
            .ok_or_else(Arithmetic::overflow)?;
        Ok(Some(index))
    }
}

#[derive(Clone, Copy)]
enum Scalar {
    Integer(i128),
    Float(f64),
}

impl ExpressionValue {
    fn is_truthy(&self) -> bool {
        match self {
            Self::Number(Scalar::Integer(value)) => *value != 0,
            Self::Number(Scalar::Float(value)) => *value != 0.0,
            Self::Json(Value::Null) => false,
            Self::Json(Value::Bool(value)) => *value,
            Self::Json(Value::Number(value)) => value.as_f64().is_some_and(|value| value != 0.0),
            Self::Json(Value::String(value)) => !value.is_empty(),
            Self::Json(Value::Array(value)) => !value.is_empty(),
            Self::Json(Value::Object(value)) => !value.is_empty(),
        }
    }

    fn into_index(self) -> Result<i128, AppError> {
        match self {
            Self::Number(Scalar::Integer(value)) => Ok(value),
            Self::Json(Value::Bool(value)) => Ok(i128::from(value)),
            _ => Err(AppError::InvalidInput(
                "list indices must be integers".to_owned(),
            )),
        }
    }

    fn attribute(self, name: &str) -> Result<Self, AppError> {
        let Self::Json(Value::Object(mut values)) = self else {
            return Err(AppError::InvalidInput(
                "attribute access requires an object".to_owned(),
            ));
        };
        values
            .remove(name)
            .ok_or_else(|| AppError::InvalidInput(format!("attribute '{name}' does not exist")))
            .and_then(Self::from_json)
    }

    fn index(self, index: Self) -> Result<Self, AppError> {
        match self {
            Self::Json(Value::Object(mut values)) => {
                let Self::Json(Value::String(key)) = index else {
                    return Err(AppError::InvalidInput(
                        "dict indices must be strings".to_owned(),
                    ));
                };
                values
                    .remove(&key)
                    .ok_or_else(|| AppError::InvalidInput(format!("key '{key}' does not exist")))
                    .and_then(Self::from_json)
            }
            Self::Json(Value::Array(values)) => {
                let mut index = index.into_index()?;
                let length = values.len() as i128;
                if index < 0 {
                    index = index.checked_add(length).ok_or_else(Arithmetic::overflow)?;
                }
                usize::try_from(index)
                    .ok()
                    .and_then(|index| values.get(index))
                    .cloned()
                    .ok_or_else(|| AppError::InvalidInput("list index out of range".to_owned()))
                    .and_then(Self::from_json)
            }
            _ => Err(AppError::InvalidInput(
                "subscript requires an array or object".to_owned(),
            )),
        }
    }

    fn from_json(value: Value) -> Result<Self, AppError> {
        match value {
            Value::Number(number) => {
                let scalar = if let Some(integer) = number.as_i128() {
                    Scalar::Integer(integer)
                } else {
                    Scalar::Float(number.as_f64().ok_or_else(|| {
                        AppError::InvalidInput("unsupported numeric value".to_owned())
                    })?)
                };
                Ok(Self::Number(scalar))
            }
            value => Ok(Self::Json(value)),
        }
    }

    fn into_number(self) -> Result<Scalar, AppError> {
        match self {
            Self::Number(value) => Ok(value),
            Self::Json(Value::Bool(value)) => Ok(Scalar::Integer(i128::from(value))),
            Self::Json(_) => Err(AppError::InvalidInput(
                "arithmetic operand must be numeric".to_owned(),
            )),
        }
    }

    fn number(&self) -> Option<Scalar> {
        match self {
            Self::Number(value) => Some(*value),
            Self::Json(Value::Bool(value)) => Some(Scalar::Integer(i128::from(*value))),
            Self::Json(_) => None,
        }
    }

    fn equals(&self, other: &Self) -> Result<bool, AppError> {
        if let (Some(left), Some(right)) = (self.number(), other.number()) {
            return left.equals(right);
        }
        match (self, other) {
            (Self::Json(left), Self::Json(right)) => Self::json_equals(left, right),
            _ => Ok(false),
        }
    }

    fn json_equals(left: &Value, right: &Value) -> Result<bool, AppError> {
        if matches!(left, Value::Number(_) | Value::Bool(_))
            && matches!(right, Value::Number(_) | Value::Bool(_))
        {
            return Self::from_json(left.clone())?.equals(&Self::from_json(right.clone())?);
        }
        match (left, right) {
            (Value::Array(left), Value::Array(right)) => {
                if left.len() != right.len() {
                    return Ok(false);
                }
                for (left, right) in left.iter().zip(right) {
                    if !Self::json_equals(left, right)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (Value::Object(left), Value::Object(right)) => {
                if left.len() != right.len() {
                    return Ok(false);
                }
                for (key, left) in left {
                    let Some(right) = right.get(key) else {
                        return Ok(false);
                    };
                    if !Self::json_equals(left, right)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(left == right),
        }
    }

    fn compare(&self, other: &Self) -> Result<Ordering, AppError> {
        if let (Some(left), Some(right)) = (self.number(), other.number()) {
            return left.compare(right);
        }
        if let (Self::Json(Value::String(left)), Self::Json(Value::String(right))) = (self, other) {
            return Ok(left.cmp(right));
        }
        Err(AppError::InvalidInput(
            "ordered comparison requires numeric operands or two strings".to_owned(),
        ))
    }

    fn contains(&self, item: &Self) -> Result<bool, AppError> {
        match self {
            Self::Json(Value::String(text)) => {
                let Self::Json(Value::String(pattern)) = item else {
                    return Err(AppError::InvalidInput(
                        "string membership requires a string member".to_owned(),
                    ));
                };
                Ok(text.contains(pattern))
            }
            Self::Json(Value::Array(values)) => {
                for value in values {
                    if Self::from_json(value.clone())?.equals(item)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Self::Json(Value::Object(values)) => {
                Ok(matches!(item, Self::Json(Value::String(key)) if values.contains_key(key)))
            }
            _ => Err(AppError::InvalidInput(
                "membership requires a string, array, or object container".to_owned(),
            )),
        }
    }

    fn identical(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Json(Value::Null), Self::Json(Value::Null)) => true,
            (Self::Json(Value::Bool(left)), Self::Json(Value::Bool(right))) => left == right,
            _ => false,
        }
    }

    fn into_json(self) -> Result<Value, AppError> {
        match self {
            Self::Json(value) => Ok(value),
            Self::Number(value) => value.into_json(),
        }
    }
}

impl Scalar {
    fn negate(self) -> Result<Self, AppError> {
        match self {
            Self::Integer(value) => value
                .checked_neg()
                .map(Self::Integer)
                .ok_or_else(Arithmetic::overflow),
            Self::Float(value) => Ok(Self::Float(-value)),
        }
    }
    fn equals(self, other: Self) -> Result<bool, AppError> {
        if matches!(self, Self::Float(value) if value.is_nan())
            || matches!(other, Self::Float(value) if value.is_nan())
        {
            return Ok(false);
        }
        self.compare(other).map(|ordering| ordering.is_eq())
    }

    fn compare(self, other: Self) -> Result<Ordering, AppError> {
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => Ok(left.cmp(&right)),
            (Self::Float(left), Self::Float(right)) => left
                .partial_cmp(&right)
                .ok_or_else(|| AppError::InvalidInput("cannot order NaN values".to_owned())),
            (Self::Integer(integer), Self::Float(float)) => {
                Self::compare_integer_float(integer, float)
            }
            (Self::Float(float), Self::Integer(integer)) => {
                Self::compare_integer_float(integer, float).map(Ordering::reverse)
            }
        }
    }

    fn compare_integer_float(integer: i128, float: f64) -> Result<Ordering, AppError> {
        if float.is_nan() {
            return Err(AppError::InvalidInput("cannot order NaN values".to_owned()));
        }
        if float.is_infinite() {
            return Ok(if float.is_sign_positive() {
                Ordering::Less
            } else {
                Ordering::Greater
            });
        }
        // Keep the pin's integer/IEEE decomposition instead of rounding the
        // integer to f64, which would change equality above 2^53.
        let bits = float.to_bits();
        let negative = bits >> 63 != 0;
        let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
        let fraction = bits & ((1_u64 << 52) - 1);
        if exponent_bits == 0 && fraction == 0 {
            return Ok(integer.cmp(&0));
        }
        let (mantissa, exponent) = if exponent_bits == 0 {
            (fraction, -1074)
        } else {
            ((1_u64 << 52) | fraction, exponent_bits - 1075)
        };
        if exponent >= 0 {
            if exponent >= 127 {
                return Ok(if negative {
                    Ordering::Greater
                } else {
                    Ordering::Less
                });
            }
            let signed = if negative {
                -i128::from(mantissa)
            } else {
                i128::from(mantissa)
            };
            return Ok(match signed.checked_shl(exponent as u32) {
                Some(value) => integer.cmp(&value),
                None => {
                    if negative {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
            });
        }
        let shift = (-exponent) as u32;
        let (whole, has_fraction) = if shift >= 64 {
            (0, mantissa != 0)
        } else {
            (mantissa >> shift, mantissa & ((1_u64 << shift) - 1) != 0)
        };
        let whole = i128::from(whole);
        if !negative {
            Ok(if has_fraction {
                if integer <= whole {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            } else {
                integer.cmp(&whole)
            })
        } else if !has_fraction {
            Ok(integer.cmp(&-whole))
        } else {
            Ok(if integer <= -(whole + 1) {
                Ordering::Less
            } else {
                Ordering::Greater
            })
        }
    }

    fn as_float(self) -> f64 {
        match self {
            Self::Integer(value) => value as f64,
            Self::Float(value) => value,
        }
    }

    fn into_json(self) -> Result<Value, AppError> {
        let number = match self {
            Self::Integer(value) => serde_json::Number::from_i128(value),
            Self::Float(value) => serde_json::Number::from_f64(value),
        };
        number.map(Value::Number).ok_or_else(|| {
            AppError::InvalidInput(
                "expression result cannot be represented as a finite JSON number".to_owned(),
            )
        })
    }
}

enum Arithmetic {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    FloorDivide,
    Power,
}

impl Arithmetic {
    fn apply(&self, left: Scalar, right: Scalar) -> Result<Scalar, AppError> {
        match (left, right) {
            (Scalar::Integer(left), Scalar::Integer(right)) => self.integer(left, right),
            (left, right) => self.float(left.as_float(), right.as_float()),
        }
    }

    fn integer(&self, left: i128, right: i128) -> Result<Scalar, AppError> {
        if right == 0 && matches!(self, Self::Divide | Self::FloorDivide | Self::Remainder) {
            return Err(Self::division_by_zero());
        }
        let value = match self {
            Self::Add => left.checked_add(right),
            Self::Subtract => left.checked_sub(right),
            Self::Multiply => left.checked_mul(right),
            Self::Divide => return self.float(left as f64, right as f64),
            Self::FloorDivide => {
                let quotient = left.checked_div(right).ok_or_else(Self::overflow)?;
                let remainder = left.checked_rem(right).ok_or_else(Self::overflow)?;
                if remainder != 0 && (remainder < 0) != (right < 0) {
                    quotient.checked_sub(1)
                } else {
                    Some(quotient)
                }
            }
            Self::Remainder => {
                if left == i128::MIN && right == -1 {
                    Some(0)
                } else {
                    let remainder = left.checked_rem(right).ok_or_else(Self::overflow)?;
                    if remainder != 0 && (remainder < 0) != (right < 0) {
                        remainder.checked_add(right)
                    } else {
                        Some(remainder)
                    }
                }
            }
            Self::Power => {
                if right < 0 {
                    return self.float(left as f64, right as f64);
                }
                let exponent = u32::try_from(right).map_err(|_| Self::overflow())?;
                left.checked_pow(exponent)
            }
        };
        value.map(Scalar::Integer).ok_or_else(Self::overflow)
    }

    fn float(&self, left: f64, right: f64) -> Result<Scalar, AppError> {
        if (matches!(self, Self::Divide | Self::FloorDivide | Self::Remainder) && right == 0.0)
            || (matches!(self, Self::Power) && left == 0.0 && right < 0.0)
        {
            return Err(Self::division_by_zero());
        }
        let value = match self {
            Self::Add => left + right,
            Self::Subtract => left - right,
            Self::Multiply => left * right,
            Self::Divide => left / right,
            Self::FloorDivide => (left / right).floor(),
            Self::Power => left.powf(right),
            Self::Remainder => {
                let remainder = left % right;
                if remainder != 0.0 && remainder.is_sign_negative() != right.is_sign_negative() {
                    remainder + right
                } else {
                    remainder
                }
            }
        };
        Ok(Scalar::Float(value))
    }

    fn overflow() -> AppError {
        AppError::InvalidInput("workflow arithmetic integer overflow".to_owned())
    }

    fn division_by_zero() -> AppError {
        AppError::InvalidInput("workflow arithmetic division by zero".to_owned())
    }
}

impl Comparison {
    fn evaluate(&self, left: &ExpressionValue, right: &ExpressionValue) -> Result<bool, AppError> {
        match self {
            Self::Equal => left.equals(right),
            Self::NotEqual => left.equals(right).map(|equal| !equal),
            Self::LessThan => left.compare(right).map(|ordering| ordering.is_lt()),
            Self::LessThanOrEqual => left.compare(right).map(|ordering| !ordering.is_gt()),
            Self::GreaterThan => left.compare(right).map(|ordering| ordering.is_gt()),
            Self::GreaterThanOrEqual => left.compare(right).map(|ordering| !ordering.is_lt()),
            Self::In => right.contains(left),
            Self::NotIn => right.contains(left).map(|contains| !contains),
            Self::Contains => left.contains(right),
            Self::Is => Ok(left.identical(right)),
            Self::IsNot => Ok(!left.identical(right)),
        }
    }
}

impl Condition {
    pub(crate) fn evaluate<C: ConditionBackend>(
        &self,
        state: &WorkflowState,
        bindings: &[C],
    ) -> Result<bool, AppError> {
        // Rurr Condition validates the entire declared read set before boolean
        // short circuiting. Only root presence is required; nested lookups and
        // optional App existence predicates retain their evaluation semantics.
        let mut missing = false;
        let resolved = self.visit_reads(bindings, &mut |key, required| {
            missing |= required && !state.values().contains_key(key);
        });
        if !resolved {
            return Err(AppError::InvalidInput(
                "workflow condition key and revision are not configured".into(),
            ));
        }
        if missing {
            return Err(AppError::InvalidInput(
                "condition is missing required state keys".to_owned(),
            ));
        }
        self.evaluate_unvalidated(state, bindings)
    }

    fn evaluate_unvalidated<C: ConditionBackend>(
        &self,
        state: &WorkflowState,
        bindings: &[C],
    ) -> Result<bool, AppError> {
        match self {
            Self::Custom { .. } => {
                let binding = self.binding(bindings).ok_or_else(|| {
                    AppError::InvalidInput(
                        "workflow condition key and revision are not configured".into(),
                    )
                })?;
                // Bare expression short circuiting skips uninvoked predicates;
                // an invoked leaf still owns its literal declared-read check.
                if binding
                    .declaration()
                    .reads
                    .iter()
                    .any(|key| !state.values().contains_key(key))
                {
                    return Err(AppError::InvalidInput(
                        "condition is missing required state keys".into(),
                    ));
                }
                binding
                    .evaluate(state)
                    .map_err(|_| AppError::Integration("caller condition evaluation failed".into()))
            }
            Self::Constant { value } => Ok(*value),
            Self::Exists { path } => Ok(crate::workflow::definition::JsonBoundary::at_path(
                state.values(),
                path,
            )
            .is_some()),
            Self::Equal { left, right } => {
                Self::compare(left, right, state, bindings, Comparison::Equal)
            }
            Self::NotEqual { left, right } => {
                Self::compare(left, right, state, bindings, Comparison::NotEqual)
            }
            Self::LessThan { left, right } => {
                Self::compare(left, right, state, bindings, Comparison::LessThan)
            }
            Self::LessThanOrEqual { left, right } => {
                Self::compare(left, right, state, bindings, Comparison::LessThanOrEqual)
            }
            Self::GreaterThan { left, right } => {
                Self::compare(left, right, state, bindings, Comparison::GreaterThan)
            }
            Self::GreaterThanOrEqual { left, right } => {
                Self::compare(left, right, state, bindings, Comparison::GreaterThanOrEqual)
            }
            Self::In { left, right } => Self::compare(left, right, state, bindings, Comparison::In),
            Self::NotIn { left, right } => {
                Self::compare(left, right, state, bindings, Comparison::NotIn)
            }
            Self::Contains { left, right } => {
                Self::compare(left, right, state, bindings, Comparison::Contains)
            }
            Self::Is { left, right } => Self::compare(left, right, state, bindings, Comparison::Is),
            Self::IsNot { left, right } => {
                Self::compare(left, right, state, bindings, Comparison::IsNot)
            }
            Self::Compare { values, operators } => {
                let mut values = values.iter();
                let mut left = values
                    .next()
                    .ok_or_else(|| {
                        AppError::InvalidInput("comparison chain requires operands".to_owned())
                    })?
                    .evaluate_value(state, bindings)?;
                for (operator, next) in operators.iter().zip(values) {
                    let right = next.evaluate_value(state, bindings)?;
                    if !operator.evaluate(&left, &right)? {
                        return Ok(false);
                    }
                    left = right;
                }
                Ok(true)
            }
            Self::Not { condition } => Ok(!condition.evaluate_unvalidated(state, bindings)?),
            Self::All { conditions } => {
                for condition in conditions {
                    if !condition.evaluate_unvalidated(state, bindings)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Self::Any { conditions } => {
                for condition in conditions {
                    if condition.evaluate_unvalidated(state, bindings)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    fn compare<C: ConditionBackend>(
        left: &ValueExpr,
        right: &ValueExpr,
        state: &WorkflowState,
        bindings: &[C],
        operator: Comparison,
    ) -> Result<bool, AppError> {
        let left = left.evaluate_value(state, bindings)?;
        let right = right.evaluate_value(state, bindings)?;
        operator.evaluate(&left, &right)
    }
}
