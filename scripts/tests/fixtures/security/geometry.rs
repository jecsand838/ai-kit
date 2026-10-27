use geo::{
    Area, BooleanOps, BoundingRect, Buffer, Contains, Intersects, LineString, Point, Polygon,
    algorithm::buffer::{BufferStyle, LineCap},
};

struct GeometryRegression;

impl GeometryRegression {
    fn rectangle(min: (f64, f64), max: (f64, f64)) -> Polygon<f64> {
        Polygon::new(
            LineString::from(vec![min, (max.0, min.1), max, (min.0, max.1), min]),
            vec![],
        )
    }

    fn with_hole() -> Polygon<f64> {
        Polygon::new(
            Self::rectangle((0.0, 0.0), (10.0, 10.0)).exterior().clone(),
            vec![LineString::from(vec![
                (3.0, 3.0),
                (3.0, 7.0),
                (7.0, 7.0),
                (7.0, 3.0),
                (3.0, 3.0),
            ])],
        )
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() <= 1e-8,
            "expected {expected}, got {actual}"
        );
    }
}

#[test]
fn overlapping_rectangles_preserve_boolean_areas_and_regions() {
    let left = GeometryRegression::rectangle((0.0, 0.0), (4.0, 4.0));
    let right = GeometryRegression::rectangle((2.0, 0.0), (6.0, 4.0));
    let union = left.union(&right);
    let intersection = left.intersection(&right);
    let difference = left.difference(&right);

    GeometryRegression::assert_close(union.unsigned_area(), 24.0);
    GeometryRegression::assert_close(intersection.unsigned_area(), 8.0);
    GeometryRegression::assert_close(difference.unsigned_area(), 8.0);
    assert!(union.contains(&Point::new(5.0, 2.0)));
    assert!(intersection.contains(&Point::new(3.0, 2.0)));
    assert!(!intersection.contains(&Point::new(1.0, 2.0)));
    assert!(difference.contains(&Point::new(1.0, 2.0)));
    assert!(!difference.contains(&Point::new(3.0, 2.0)));
}

#[test]
fn polygon_holes_and_boundaries_preserve_contains_and_intersects() {
    let polygon = GeometryRegression::with_hole();
    GeometryRegression::assert_close(polygon.unsigned_area(), 84.0);
    for (point, contained, intersects) in [
        (Point::new(1.0, 1.0), true, true),
        (Point::new(5.0, 5.0), false, false),
        (Point::new(0.0, 5.0), false, true),
        (Point::new(3.0, 5.0), false, true),
        (Point::new(11.0, 5.0), false, false),
    ] {
        assert_eq!(polygon.contains(&point), contained, "point {point:?}");
        assert_eq!(polygon.intersects(&point), intersects, "point {point:?}");
    }
    let island = GeometryRegression::rectangle((4.0, 4.0), (6.0, 6.0));
    GeometryRegression::assert_close(polygon.union(&island).unsigned_area(), 88.0);
    assert!(polygon.intersection(&island).0.is_empty());
    GeometryRegression::assert_close(polygon.difference(&island).unsigned_area(), 84.0);
}

#[test]
fn square_capped_buffer_preserves_extents_and_area() {
    let line = LineString::from(vec![(0.0, 0.0), (4.0, 0.0)]);
    let buffer = line.buffer_with_style(BufferStyle::new(1.0).line_cap(LineCap::Square));
    let bounds = buffer.bounding_rect().expect("nonempty buffered line");
    GeometryRegression::assert_close(bounds.min().x, -1.0);
    GeometryRegression::assert_close(bounds.min().y, -1.0);
    GeometryRegression::assert_close(bounds.max().x, 5.0);
    GeometryRegression::assert_close(bounds.max().y, 1.0);
    GeometryRegression::assert_close(buffer.unsigned_area(), 12.0);
    assert!(buffer.contains(&Point::new(2.0, 0.5)));
    assert!(!buffer.contains(&Point::new(2.0, 1.5)));
}
