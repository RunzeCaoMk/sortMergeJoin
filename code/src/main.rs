use std::cmp::min_by_key;
use std::fs::File;
use std::io::Write;
use std::ops::Deref;
use std::time::Instant;
use join::join::*;
use join::common::*;
use rand::Rng;

// function to creat number of tuples for benchmark
pub fn create_vec_tuple(tuple_number: usize, width: usize, range: usize) -> Vec<Tuple> {
    let mut rng = rand::thread_rng();

    let mut tuple_data = Vec::new();

    // create tuples based on the tuple number
    for _ in 0..tuple_number {
        let mut tuple = Vec::new();
        // create fields in each tuple, base on the tuple's width
        for _ in 0..width {
            tuple.push(rng.gen_range((range-1000)..range) as i32);
        }
        tuple_data.push(tuple);
    }

    let mut res = Vec::new();
    for item in &tuple_data {
        let fields = item.iter().map(|i| Field::IntField(*i)).collect();
        res.push(Tuple::new(fields));
    }
    res
}
/// Creates a new table schema for a table with width number of IntFields.
pub fn get_int_table_schema(width: usize) -> TableSchema {
    let mut attrs = Vec::new();
    for _ in 0..width {
        attrs.push(Attribute::new(String::new(), DataType::Int))
    }
    TableSchema::new(attrs)
}

// helper method to benchmark 5k tuples with at least 10% are same
fn dis_10(mut file: &File) {
    file.write_all("10%:\n".as_ref());

    let WIDTH = 2;
    let mut left_child = create_vec_tuple(1843 as usize, WIDTH, 1000);
    let mut right_child = create_vec_tuple(1843 as usize, WIDTH, 1000);
    let mut common = create_vec_tuple(205 as usize, WIDTH, 1000);
    left_child.append(&mut common);
    right_child.append(&mut common);
    let schema = get_int_table_schema(WIDTH);

    let s1 = Box::new(TupleIterator::new(left_child.clone(), schema.clone()));
    let s2 = Box::new(TupleIterator::new(right_child.clone(), schema.clone()));
    let s1_1 = Box::new(TupleIterator::new(left_child, schema.clone()));
    let s2_1 = Box::new(TupleIterator::new(right_child, schema.clone()));
    let mut op1 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1, s2, 1));
    let mut op2 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1_1, s2_1, 2));

    // M-way
    file.write_all("m-way:\n".as_ref());
    let now = Instant::now();
    op1.open();
    op1.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // M-way
    file.write_all("m-pass:\n".as_ref());
    let now = Instant::now();
    op2.open();
    op2.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark 5k tuples with at least 30% are same
fn dis_30(mut file: &File) {
    file.write_all("30%:\n".as_ref());

    let width = 2;
    let mut left_child = create_vec_tuple(1434 as usize, width, 1000);
    let mut right_child = create_vec_tuple(1434 as usize, width, 1000);
    let mut common = create_vec_tuple(614 as usize, width, 1000);
    left_child.append(&mut common);
    right_child.append(&mut common);
    let schema1 = get_int_table_schema(width);
    let schema2 = get_int_table_schema(width);

    let s1 = Box::new(TupleIterator::new(left_child.clone(), schema1.clone()));
    let s2 = Box::new(TupleIterator::new(right_child.clone(), schema2.clone()));
    let s1_1 = Box::new(TupleIterator::new(left_child, schema1));
    let s2_1 = Box::new(TupleIterator::new(right_child, schema2));
    let mut op1 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1, s2, 1));
    let mut op2 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1_1, s2_1, 2));


    // M-way
    file.write_all("m-way:\n".as_ref());
    let now = Instant::now();
    op1.open();
    op1.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // M-way
    file.write_all("m-pass:\n".as_ref());
    let now = Instant::now();
    op2.open();
    op2.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

}
// helper method to benchmark 5k tuples with at least 50% are same
fn dis_50(mut file: &File) {
    file.write_all("50%:\n".as_ref());

    let width = 2;
    let mut left_child = create_vec_tuple(1024 as usize, width, 1000);
    let mut right_child = create_vec_tuple(1024 as usize, width, 1000);
    let mut common = create_vec_tuple(1024 as usize, width, 1000);
    left_child.append(&mut common);
    right_child.append(&mut common);
    let schema1 = get_int_table_schema(width);
    let schema2 = get_int_table_schema(width);

    let s1 = Box::new(TupleIterator::new(left_child.clone(), schema1.clone()));
    let s2 = Box::new(TupleIterator::new(right_child.clone(), schema2.clone()));
    let s1_1 = Box::new(TupleIterator::new(left_child, schema1));
    let s2_1 = Box::new(TupleIterator::new(right_child, schema2));
    let mut op1 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1, s2, 1));
    let mut op2 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1_1, s2_1, 2));


    // M-way
    file.write_all("m-way:\n".as_ref());
    let now = Instant::now();
    op1.open();
    op1.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // M-way
    file.write_all("m-pass:\n".as_ref());
    let now = Instant::now();
    op2.open();
    op2.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

}
// method to benchmark different cardinality with 12 permutations
fn distribution(mut file: &File) {
    file.write_all("Micro-benchmark with different distribution\n".as_ref());
    dis_10(file);
    // dis_30(file);
    // dis_50(file);
}

// helper method to benchmark 2^11 = 2048 tuples
fn c_11(mut file: &File) {
    file.write_all("2^11 = 2048:\n".as_ref());
    let width1 = 2;
    let width2 = 3;
    let mut left_child = create_vec_tuple(2048 as usize, width1, 1000);
    let mut right_child = create_vec_tuple(2048 as usize, width2, 1000);
    let schema1 = get_int_table_schema(width1);
    let schema2 = get_int_table_schema(width1);

    let s1 = Box::new(TupleIterator::new(left_child.clone(), schema1.clone()));
    let s2 = Box::new(TupleIterator::new(right_child.clone(), schema2.clone()));
    let s1_1 = Box::new(TupleIterator::new(left_child, schema1));
    let s2_1 = Box::new(TupleIterator::new(right_child, schema2));
    let mut op1 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1, s2, 1));
    let mut op2 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1_1, s2_1, 2));

    // M-way
    file.write_all("m-way:\n".as_ref());
    let now = Instant::now();
    op1.open();
    op1.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // M-way
    file.write_all("m-pass:\n".as_ref());
    let now = Instant::now();
    op2.open();
    op2.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark 2^15 = 32768 tuples
fn c_15(mut file: &File) {
    file.write_all("2^15 = 32768:\n".as_ref());
    let width1 = 2;
    let width2 = 3;
    let mut left_child = create_vec_tuple(32768 as usize, width1, 1000);
    let mut right_child = create_vec_tuple(32768 as usize, width2, 1000);
    let schema1 = get_int_table_schema(width1);
    let schema2 = get_int_table_schema(width1);

    let s1 = Box::new(TupleIterator::new(left_child.clone(), schema1.clone()));
    let s2 = Box::new(TupleIterator::new(right_child.clone(), schema2.clone()));
    let s1_1 = Box::new(TupleIterator::new(left_child, schema1));
    let s2_1 = Box::new(TupleIterator::new(right_child, schema2));
    let mut op1 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1, s2, 1));
    let mut op2 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1_1, s2_1, 2));

    // M-way
    file.write_all("m-way:\n".as_ref());
    let now = Instant::now();
    op1.open();
    op1.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // M-way
    file.write_all("m-pass:\n".as_ref());
    let now = Instant::now();
    op2.open();
    op2.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark 2^17 = 131072 tuples
fn c_17(mut file: &File) {
    file.write_all("2^17 = 131072:\n".as_ref());
    let width1 = 2;
    let width2 = 3;
    let mut left_child = create_vec_tuple(131072 as usize, width1, 1000);
    let mut right_child = create_vec_tuple(131072 as usize, width2, 1000);
    let schema1 = get_int_table_schema(width1);
    let schema2 = get_int_table_schema(width1);

    let s1 = Box::new(TupleIterator::new(left_child.clone(), schema1.clone()));
    let s2 = Box::new(TupleIterator::new(right_child.clone(), schema2.clone()));
    let s1_1 = Box::new(TupleIterator::new(left_child, schema1));
    let s2_1 = Box::new(TupleIterator::new(right_child, schema2));
    let mut op1 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1, s2, 1));
    let mut op2 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1_1, s2_1, 2));

    // M-way
    file.write_all("m-way:\n".as_ref());
    let now = Instant::now();
    op1.open();
    op1.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // M-way
    file.write_all("m-pass:\n".as_ref());
    let now = Instant::now();
    op2.open();
    op2.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// method to benchmark different cardinality
fn cardinality(mut file: &File) {
    file.write_all("Micro-benchmark with different cardinality\n".as_ref());
    // c_11(file);
    // c_15(file);
    c_17(file);
}

// helper method to benchmark 2048 tuples with 4000-5000
fn r_5000(mut file: &File) {
    file.write_all("4000-5000:\n".as_ref());
    let width1 = 2;
    let width2 = 3;
    let mut left_child = create_vec_tuple(2048 as usize, width1, 5000);
    let mut right_child = create_vec_tuple(2048 as usize, width2, 5000);
    let schema1 = get_int_table_schema(width1);
    let schema2 = get_int_table_schema(width1);

    let s1 = Box::new(TupleIterator::new(left_child.clone(), schema1.clone()));
    let s2 = Box::new(TupleIterator::new(right_child.clone(), schema2.clone()));
    let s1_1 = Box::new(TupleIterator::new(left_child, schema1));
    let s2_1 = Box::new(TupleIterator::new(right_child, schema2));
    let mut op1 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1, s2, 1));
    let mut op2 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1_1, s2_1, 2));

    // M-way
    file.write_all("m-way:\n".as_ref());
    let now = Instant::now();
    op1.open();
    op1.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // M-way
    file.write_all("m-pass:\n".as_ref());
    let now = Instant::now();
    op2.open();
    op2.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark 2048 tuples with 9000-10000
fn r_10000(mut file: &File) {
    file.write_all("9000-10000:\n".as_ref());
    let width1 = 2;
    let width2 = 3;
    let mut left_child = create_vec_tuple(2048 as usize, width1, 10000);
    let mut right_child = create_vec_tuple(2048 as usize, width2, 10000);
    let schema1 = get_int_table_schema(width1);
    let schema2 = get_int_table_schema(width1);

    let s1 = Box::new(TupleIterator::new(left_child.clone(), schema1.clone()));
    let s2 = Box::new(TupleIterator::new(right_child.clone(), schema2.clone()));
    let s1_1 = Box::new(TupleIterator::new(left_child, schema1));
    let s2_1 = Box::new(TupleIterator::new(right_child, schema2));
    let mut op1 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1, s2, 1));
    let mut op2 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1_1, s2_1, 2));

    // M-way
    file.write_all("m-way:\n".as_ref());
    let now = Instant::now();
    op1.open();
    op1.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // M-way
    file.write_all("m-pass:\n".as_ref());
    let now = Instant::now();
    op2.open();
    op2.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}
// helper method to benchmark 2048 tuples with 99000-100000
fn r_100000(mut file: &File) {
    file.write_all("99000-100000:\n".as_ref());
    let width1 = 2;
    let width2 = 3;
    let mut left_child = create_vec_tuple(2048 as usize, width1, 100000);
    let mut right_child = create_vec_tuple(2048 as usize, width2, 100000);
    let schema1 = get_int_table_schema(width1);
    let schema2 = get_int_table_schema(width1);

    let s1 = Box::new(TupleIterator::new(left_child.clone(), schema1.clone()));
    let s2 = Box::new(TupleIterator::new(right_child.clone(), schema2.clone()));
    let s1_1 = Box::new(TupleIterator::new(left_child, schema1));
    let s2_1 = Box::new(TupleIterator::new(right_child, schema2));
    let mut op1 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1, s2, 1));
    let mut op2 = Box::new(SortMergeJoin::new(SimplePredicateOp::Equals, 1, 1, s1_1, s2_1, 2));

    // M-way
    file.write_all("m-way:\n".as_ref());
    let now = Instant::now();
    op1.open();
    op1.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());

    // M-way
    file.write_all("m-pass:\n".as_ref());
    let now = Instant::now();
    op2.open();
    op2.next();
    file.write_all(now.elapsed().as_secs_f64().to_string().as_ref());
    file.write_all("\n".as_ref());
}

// method to benchmark different cardinality
fn range(mut file: &File) {
    file.write_all("Micro-benchmark with different range\n".as_ref());
    r_5000(file);
    r_10000(file);
    r_100000(file);
}

fn main() {
    let mut file = File::create("res_dis.txt").unwrap();
    // cardinality(&file);
    distribution(&file);
    // range(&file);
}
