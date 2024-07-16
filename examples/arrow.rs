use std::iter::Sum;

use arrow::{
    array::{
        ArrayAccessor, ArrayIter, Float32Array, Int32Array, PrimitiveArray, StringArray,
        TimestampNanosecondArray,
    },
    datatypes::ArrowPrimitiveType,
};

fn sum<T>(array: &PrimitiveArray<T>) -> T::Native
where
    T: ArrowPrimitiveType,
    T::Native: Sum,
{
    array.iter().map(|v| v.unwrap_or_default()).sum()
}

fn min<T: ArrayAccessor>(array: T) -> Option<T::Item>
where
    T::Item: Ord,
{
    ArrayIter::new(array).flatten().min()
}

fn main() {
    let result = sum(&Float32Array::from(vec![1.1, 2.9, 3.]));
    assert_eq!(result, 7.);
    assert_eq!(sum(&TimestampNanosecondArray::from(vec![1, 2, 3])), 6);

    assert_eq!(min(&Int32Array::from(vec![4, 2, 1, 6])), Some(1));
    assert_eq!(min(&StringArray::from(vec!["b", "a", "c"])), Some("a"));
}
