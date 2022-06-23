use std::cmp::{max, min, min_by_key};
use std::collections::HashMap;
use std::{thread, vec};
use serde_cbor::Value::Null;
use crate::common::{CrustyError, Field, SimplePredicateOp, TableSchema, Tuple, OpIterator,
                    TupleIterator};
use crate::common::Constraint::NotNull;

/// Compares the fields of two tuples using a predicate. (You can add any other fields that you think are neccessary)
#[derive(Clone, Copy)]
pub struct JoinPredicate {
    /// Operation to comapre the fields with.
    op: SimplePredicateOp,
    /// Index of the field of the left table (tuple).
    left_index: usize,
    /// Index of the field of the right table (tuple).
    right_index: usize,
}

impl JoinPredicate {
    /// Constructor that determines if two tuples satisfy the join condition.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation to compare the two fields with.
    /// * `left_index` - Index of the field to compare in the left tuple.
    /// * `right_index` - Index of the field to compare in the right tuple.
    fn new(op: SimplePredicateOp, left_index: usize, right_index: usize) -> Self {
        Self {
            op,
            left_index,
            right_index,
        }
    }

    // Compare fields of two tuples on some predicate and return result
    fn cmp(&self, left_tuple: &Tuple, right_tuple: &Tuple) -> bool {
        let left_field = left_tuple.get_field(self.left_index).unwrap();
        let right_field = right_tuple.get_field(self.right_index).unwrap();
        self.op.compare(left_field, right_field)
    }

    fn clone(&self) -> Self {
        Self{
            op: self.op,
            left_index: self.left_index,
            right_index: self.right_index
        }
    }
}

/// Nested loop join implementation. (You can add any other fields that you think are neccessary)
pub struct Join {
    /// Join condition.
    predicate: JoinPredicate,
    /// Left child node.
    left_child: Box<dyn OpIterator>,
    /// Right child node.
    right_child: Box<dyn OpIterator>,
    /// Schema of the result.
    schema: TableSchema,

    open: bool,
    left_tuple_cur: Tuple, // Current left tuple being used (for outer loop)
}

impl Join {
    /// Join constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        Self {
            predicate: JoinPredicate::new(op, left_index, right_index),
            schema: left_child.get_schema().merge(right_child.get_schema()),
            left_child,
            right_child,
            open: false,
            left_tuple_cur: Tuple::new(Vec::new()),
        }
    }
}

impl OpIterator for Join {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        self.left_child.open()?;
        self.left_tuple_cur = self.left_child.next()?.unwrap();
        self.right_child.open()
    }

    /// Calculates the next tuple for a nested loop join.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }

        // Find next right child tuple to merge with current left tuple
        let left_tuple = &self.left_tuple_cur;
        while let Some(t) = self.right_child.next()? {
            if self.predicate.cmp(left_tuple, &t) {
                return Ok(Some(left_tuple.merge(&t)));
            }
        }

        // If no right tuple match, update left tuple and try from right child's start
        match self.left_child.next()? {
            None => Ok(None),
            Some(t) => {
                self.left_tuple_cur = t;
                self.right_child.rewind()?;
                self.next()
            }
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.left_child.close()?;
        self.right_child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        // Rewind children, get first left (outer loop) tuple to join with
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.left_tuple_cur = self.left_child.next()?.unwrap();
        Ok(())
    }

    /// return schema of the result
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// Hash equi-join implementation. (You can add any other fields that you think are neccessary)
pub struct HashEqJoin {
    predicate: JoinPredicate,

    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,

    schema: TableSchema,

    open: bool,
    // Map attribute values to all tuples containing that value
    ht: HashMap<Field, Vec<Tuple>>,
    field_cur: Field,       // Current field being used as ht key
    index_cur: usize,       // Current index in ht[field_cur]
    right_tuple_cur: Tuple, // Current tuple from right child being used in joins
}

impl HashEqJoin {
    /// Constructor for a hash equi-join operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    #[allow(dead_code)]
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        Self {
            predicate: JoinPredicate::new(op, left_index, right_index),
            schema: left_child.get_schema().merge(right_child.get_schema()),
            left_child,
            right_child,
            open: false,
            ht: HashMap::new(),
            field_cur: Field::IntField(0),
            index_cur: 0,
            right_tuple_cur: Tuple::new(Vec::new()),
        }
    }

    // Find first right child tuple that will be used in the join result
    fn partial_open(&mut self) -> Result<(), CrustyError> {
        let right_index = self.predicate.right_index;
        while let Some(t) = self.right_child.next()? {
            let field = t.get_field(right_index).unwrap();
            if self.ht.contains_key(field) {
                self.field_cur = field.clone();
                self.index_cur = 0;
                self.right_tuple_cur = t;
                return Ok(());
            }
        }
        Ok(())
    }
}

impl OpIterator for HashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;

        // Build hash table from left child
        self.left_child.open()?;
        let left_index = self.predicate.left_index;
        while let Some(t) = self.left_child.next()? {
            let field = t.get_field(left_index).unwrap();
            if let Some(vec) = self.ht.get_mut(field) {
                vec.push(t);
            } else {
                self.ht.insert(field.clone(), vec![t]);
            }
        }

        // Get first right child tuple to use in next()
        self.right_child.open()?;
        self.partial_open()
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }

        // Try to use current right child tuple again
        if let Some(t) = self.ht[&self.field_cur].get(self.index_cur) {
            self.index_cur += 1;
            return Ok(Some(t.merge(&self.right_tuple_cur)));
        }

        // If no match, find new right tuple and return first match with it
        let right_index = self.predicate.right_index;
        while let Some(t) = self.right_child.next()? {
            let field = t.get_field(right_index).unwrap();
            if let Some(vec) = self.ht.get(field) {
                self.field_cur = field.clone();
                self.index_cur = 1;
                self.right_tuple_cur = t;
                return Ok(Some(vec[0].merge(&self.right_tuple_cur)));
            }
        }
        // Out of right tuples
        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        // Close children, empty hash table
        self.left_child.close()?;
        self.right_child.close()?;
        self.ht.clear();
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        // Keep hash table
        // Rewind right child and get first tuple to use from it
        self.right_child.rewind()?;
        self.partial_open()
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}


/// Sort-merge join implementation
pub struct SortMergeJoin {
    /// Join condition.
    predicate: JoinPredicate,
    /// Left child node.
    left_child: Box<dyn OpIterator + Send>,
    /// Right child node.
    right_child: Box<dyn OpIterator + Send>,
    /// Schema of the result.
    schema: TableSchema,
    /// Join status
    open: bool,
    /// level 3 method: 1 for m-way; 2 for m-pass
    sort_merge_method: isize,
    /// left level 3 runs
    pub l3_runs_l: Vec<Vec<Tuple>>,
    /// right level 3 runs
    pub l3_runs_r: Vec<Vec<Tuple>>,
    /// right global minimum
    min_r: Tuple,
    /// right global maximum
    max_r: Tuple,
}

impl SortMergeJoin {
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator + Send>,
        right_child: Box<dyn OpIterator + Send>,
        sort_merge_method: isize,
    ) -> Self {
        Self {
            predicate: JoinPredicate::new(op, left_index, right_index),
            schema: left_child.get_schema().merge(right_child.get_schema()),
            left_child,
            right_child,
            open: false,
            sort_merge_method,
            l3_runs_l: Vec::new(),
            l3_runs_r: Vec::new(),
            min_r: Tuple::new(vec![Field::IntField(999999), Field::IntField(999999), Field::IntField(999999), Field::IntField(999999)]),
            max_r: Tuple::new(vec![]),
        }
    }
}

// helper method to find min/max tuple
fn compare_min(a: Tuple, b: Tuple, index: usize) -> Tuple {
    if a.get_field(index) < b.get_field(index) {
        return a;
    } else {
        return b;
    }
}
fn compare_max(a: Tuple, b: Tuple, index: usize) -> Tuple {
    if a.get_field(index) > b.get_field(index) {
        return a;
    } else {
        return b;
    }
}

// helper method to sort level 1 run
fn sort_run_l1(mut run: Vec<Tuple>, index: usize) -> Vec<Tuple> {
    let mut temp = Tuple::new(vec![]);
    temp = compare_min(run[0].clone(), run[1].clone(), index);
    run[1] = compare_max(run[0].clone(), run[1].clone(), index);
    run[0] = temp.clone();
    temp = compare_min(run[2].clone(), run[3].clone(), index);
    run[3] = compare_max(run[2].clone(), run[3].clone(), index);
    run[2] = temp.clone();

    temp = compare_min(run[0].clone(), run[2].clone(), index);
    run[2] = compare_max(run[0].clone(), run[2].clone(), index);
    run[0] = temp;
    temp = compare_min(run[1].clone(), run[3].clone(), index);
    run[3] = compare_max(run[1].clone(), run[3].clone(), index);
    run[1] = temp;

    temp = compare_min(run[1].clone(), run[2].clone(), index);
    run[2] = compare_max(run[1].clone(), run[2].clone(), index);
    run[1] = temp;
    return run;
}
// helper method to sort level 2 run
fn sort_run_l2(mut run: Vec<Tuple>, index: usize) -> Vec<Tuple> {
    // let mut temp = Tuple::new(vec![]);
    // temp = min_tuple(run[3].clone(), run[7].clone(), index);
    // run[7] = max_tuple(run[3].clone(), run[7].clone(), index);
    // run[3] = temp.clone();
    // temp = min_tuple(run[2].clone(), run[6].clone(), index);
    // run[6] = max_tuple(run[2].clone(), run[6].clone(), index);
    // run[2] = temp.clone();
    // temp = min_tuple(run[1].clone(), run[5].clone(), index);
    // run[5] = max_tuple(run[1].clone(), run[5].clone(), index);
    // run[1] = temp;
    // temp = min_tuple(run[0].clone(), run[4].clone(), index);
    // run[4] = max_tuple(run[0].clone(), run[4].clone(), index);
    // run[0] = temp;
    //
    // temp = min_tuple(run[0].clone(), run[2].clone(), index);
    // run[2] = max_tuple(run[0].clone(), run[2].clone(), index);
    // run[0] = temp.clone();
    // temp = min_tuple(run[5].clone(), run[7].clone(), index);
    // run[7] = max_tuple(run[5].clone(), run[7].clone(), index);
    // run[5] = temp.clone();
    // temp = min_tuple(run[1].clone(), run[3].clone(), index);
    // run[3] = max_tuple(run[1].clone(), run[3].clone(), index);
    // run[1] = temp;
    // temp = min_tuple(run[4].clone(), run[6].clone(), index);
    // run[6] = max_tuple(run[4].clone(), run[6].clone(), index);
    // run[4] = temp;
    //
    // temp = min_tuple(run[0].clone(), run[1].clone(), index);
    // run[1] = max_tuple(run[0].clone(), run[1].clone(), index);
    // run[0] = temp.clone();
    // temp = min_tuple(run[2].clone(), run[3].clone(), index);
    // run[3] = max_tuple(run[2].clone(), run[3].clone(), index);
    // run[2] = temp.clone();
    // temp = min_tuple(run[4].clone(), run[5].clone(), index);
    // run[5] = max_tuple(run[4].clone(), run[5].clone(), index);
    // run[4] = temp;
    // temp = min_tuple(run[6].clone(), run[7].clone(), index);
    // run[7] = max_tuple(run[6].clone(), run[7].clone(), index);
    // run[6] = temp;

    // second way of doing sorting
    if compare_max(run[3].clone(), run[7].clone(), index) == run[3].clone() {
        run.swap(3, 7);
    }
    if compare_max(run[2].clone(), run[6].clone(), index) == run[2].clone() {
        run.swap(2, 6);
    }
    if compare_max(run[1].clone(), run[5].clone(), index) == run[1].clone() {
        run.swap(1, 5);
    }
    if compare_max(run[0].clone(), run[4].clone(), index) == run[0].clone() {
        run.swap(0, 4);
    }

    if compare_max(run[0].clone(), run[2].clone(), index) == run[0].clone() {
        run.swap(0, 2);
    }
    if compare_max(run[5].clone(), run[7].clone(), index) == run[5].clone() {
        run.swap(5, 7);
    }
    if compare_max(run[1].clone(), run[3].clone(), index) == run[1].clone() {
        run.swap(1, 3);
    }
    if compare_max(run[4].clone(), run[6].clone(), index) == run[4].clone() {
        run.swap(4, 6);
    }

    if compare_max(run[0].clone(), run[1].clone(), index) == run[0].clone() {
        run.swap(0, 1);
    }
    if compare_max(run[2].clone(), run[3].clone(), index) == run[2].clone() {
        run.swap(2, 3);
    }
    if compare_max(run[4].clone(), run[5].clone(), index) == run[4].clone() {
        run.swap(4, 5);
    }
    if compare_max(run[6].clone(), run[7].clone(), index) == run[6].clone() {
        run.swap(6, 7);
    }
    return run;
}
// helper method to sort each run in runs
fn sort_runs(mut runs: Vec<Vec<Tuple>>, index: usize, level: usize) -> Vec<Vec<Tuple>> {
    let mut handles = Vec::new();
    if level == 1 {
        for mut run in runs {
            let handle = thread::spawn(move || {
                let new_run = sort_run_l1(run.clone(), index.clone());
                new_run
            });
            handles.push(handle);
        }
    } else {
        for mut run in runs {
            let handle = thread::spawn(move || {
                let new_run = sort_run_l2(run.clone(), index.clone());
                new_run
            });
            handles.push(handle);
        }
    }

    let mut res = Vec::new();
    for handle in handles {
        res.push(handle.join().unwrap().clone());
    }

    res
}

// helper method to merge level 1 runs into level 2 runs
fn merge_1_to_2(mut runs: Vec<Vec<Tuple>>) -> Vec<Vec<Tuple>> {
    let mut counter = 1;
    let mut temp = Vec::new();
    let mut res = Vec::new();
    for mut run in runs.clone() {
        if counter % 2 != 0 {
            temp.append(&mut run);
            counter += 1;
        } else {
            run.reverse();
            temp.append(&mut run);
            counter += 1;
            res.push(temp.clone());
            temp = Vec::new();
        }
    }
    res
}

// sort-merge runs by multi-way method
fn sort_m_way_l3(mut runs: Vec<Vec<Tuple>>, min: Tuple, max: Tuple, index: usize) -> Vec<Vec<Tuple>> {
    // redistribute runs into 3 runs (4 physical thread - 1)
    let mut res_1 = Vec::new();
    let mut res_2 = Vec::new();
    let mut res_3 = Vec::new();

    let min_val = min.get_field(index).unwrap().unwrap_int_field();
    let max_val = max.get_field(index).unwrap().unwrap_int_field();

    let one_third = (min_val + (max_val - min_val) / 3) as isize;
    let two_third = (min_val + (max_val - min_val) * 2 / 3) as isize;

    // redistribute tuples based on the range partition
    for run in &runs {
        for t in run {
            if *t.get_field(index).unwrap() <= Field::IntField(one_third as i32) {
                res_1.push(t.clone());
            } else if *t.get_field(index).unwrap() <= Field::IntField(two_third as i32) {
                res_2.push(t.clone());
            } else {
                res_3.push(t.clone());
            }
        }
    }

    res_1.sort_by(|a,b| a.get_field(index).unwrap().cmp(b.get_field(index).unwrap()));
    res_2.sort_by(|a,b| a.get_field(index).unwrap().cmp(b.get_field(index).unwrap()));
    res_3.sort_by(|a,b| a.get_field(index).unwrap().cmp(b.get_field(index).unwrap()));

    return vec![res_1, res_2, res_3];
}

// join the left run with right runs for m-way
fn join_m_way(mut run: Vec<Tuple>, right_run: Vec<Tuple>, pre: JoinPredicate) -> Vec<Tuple> {
    let mut res = Vec::new();
    // loop through each tuple in the run
    for t in &run {
        // try to match with tuple in each right run
        for t_r in &right_run {
            // if right tuple bigger than current tuple then break
            if *t_r.get_field(pre.right_index).unwrap() > *t.get_field(pre.left_index).unwrap() {
                break;
            } else if pre.cmp(t, t_r) {
                res.push(t.merge(t_r));
            }
        }
    }
    res
}
// join the left run with right runs for m-pass
fn join_m_pass(mut run: Vec<Tuple>, right_runs: Vec<Vec<Tuple>>, pre: JoinPredicate) -> Vec<Tuple> {
    let mut res = Vec::new();
    // loop through each tuple in the run
    for t in &run {
        // try to match with tuple in each right run
        for right_run in &right_runs {
            for t_r in right_run {
                // if right tuple bigger than current tuple then break
                if *t_r.get_field(pre.right_index).unwrap() > *t.get_field(pre.left_index).unwrap() {
                    break;
                } else if pre.cmp(t, t_r) {
                    res.push(t.merge(t_r));
                }
            }
        }
    }
    res
}

impl OpIterator for SortMergeJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        self.left_child.open()?;
        self.right_child.open()?;

        let left_index = self.predicate.left_index;
        let right_index = self.predicate.right_index;

        // initialize the runs for level 1 sorting
        let mut l1_runs_l = Vec::new();
        let mut l1_runs_r = Vec::new();
        // split children into level 1 runs
        let mut l1_temp = Vec::new();

        while let Some(t) = &self.left_child.next()? {
            // each run contains 4 Tuples in order to fit into the register
            if l1_temp.len() == 4 {
                l1_runs_l.push(l1_temp.clone());
                l1_temp = Vec::new();
                l1_temp.push(t.clone());
            } else {
                l1_temp.push(t.clone());
            }
        }
        l1_runs_l.push(l1_temp.clone());
        l1_temp = Vec::new();
        while let Some(t) = &self.right_child.next()? {
            // each run contains 4 Tuples in order to fit into the register
            if l1_temp.len() == 4 {
                l1_runs_r.push(l1_temp.clone());
                l1_temp = Vec::new();
                l1_temp.push(t.clone());
            } else {
                l1_temp.push(t.clone());
            }
        }
        l1_runs_r.push(l1_temp.clone());


        // parallel sorting level 1 runs
        l1_runs_l = sort_runs(l1_runs_l, left_index, 1);
        l1_runs_r = sort_runs(l1_runs_r, right_index, 1);

        // merge and sort into level 2 runs
        let mut l2_runs_l = merge_1_to_2(l1_runs_l.clone());
        let mut l2_runs_r = merge_1_to_2(l1_runs_r.clone());

        // parallel sorting level 2 runs
        l2_runs_l = sort_runs(l2_runs_l, left_index, 2);
        l2_runs_r = sort_runs(l2_runs_r, right_index, 2);

        // level 3 m-way/m-pass
        if self.sort_merge_method == 1 {
            // find right child's min/max
            for run in l2_runs_r.clone() {
                for t in run {
                    if compare_max(t.clone(), self.max_r.clone(), right_index) == t {
                        self.max_r = t.clone();
                    }
                    if compare_min(t.clone(), self.min_r.clone(), right_index) == t {
                        self.min_r = t.clone();
                    }
                }
            }

            self.l3_runs_l = sort_m_way_l3(l2_runs_l, self.min_r.clone(), self.max_r.clone(), left_index);
            self.l3_runs_r = sort_m_way_l3(l2_runs_r, self.min_r.clone(), self.max_r.clone(), right_index);
        } else {
            self.l3_runs_l = l2_runs_l;
            self.l3_runs_r = l2_runs_r;
        }
        // assert_eq!(self.l3_runs_l, vec![vec![Tuple::new(vec![Field::StringField(String::from("Here"))])]]);

        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }

        let mut handles = Vec::new();
        let predicate = self.predicate.clone();

        // M-Way
        if self.sort_merge_method == 1 {
            let mut run_counter = 0;
            // loop through each run in left
            for run_l in self.l3_runs_l.clone() {
                let right_runs = self.l3_runs_r.clone();
                let handle = thread::spawn(move || {
                    let new_run = join_m_way(
                        run_l.clone(),
                        right_runs[run_counter].clone(),
                        predicate);
                    new_run
                });
                handles.push(handle);
                run_counter += 1;
            }
        } else {
        // Join M-Pass
            for run in self.l3_runs_l.clone() {
                let right_runs = self.l3_runs_r.clone();
                let handle = thread::spawn(move || {
                    let new_run = join_m_pass(
                        run.clone(),
                        right_runs.clone(),
                        predicate);
                    new_run
                });
                handles.push(handle);
            }
        }

        let mut joined_left_runs = Vec::new();
        for handle in handles {
            joined_left_runs.push(handle.join().unwrap());
        }
        self.l3_runs_l = joined_left_runs;

        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.left_child.close()?;
        self.right_child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        // Rewind children
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.l3_runs_l = Vec::new();
        self.l3_runs_r = Vec::new();
        self.min_r = Tuple::new(vec![Field::IntField(999999), Field::IntField(999999), Field::IntField(999999), Field::IntField(999999)]);
        self.max_r = Tuple::new(vec![]);
        Ok(())
    }

    /// return schema of the result
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}


#[cfg(test)]
mod test {
    use std::ops::Deref;
    use crate::common::*;
    use super::*;

    /// Creates a Vec of tuples containing IntFields given a 2D Vec of i32 's
    pub fn create_tuple_list(tuple_data: Vec<Vec<i32>>) -> Vec<Tuple> {
        let mut tuples = Vec::new();
        for item in &tuple_data {
            let fields = item.iter().map(|i| Field::IntField(*i)).collect();
            tuples.push(Tuple::new(fields));
        }
        tuples
    }
    /// Creates a new table schema for a table with width number of IntFields.
    pub fn get_int_table_schema(width: usize) -> TableSchema {
        let mut attrs = Vec::new();
        for _ in 0..width {
            attrs.push(Attribute::new(String::new(), DataType::Int))
        }
        TableSchema::new(attrs)
    }
    #[allow(dead_code)]
    /// Asserts that iter1 and iter2 contain all the same tuples
    pub fn match_all_tuples(
        mut iter1: Box<dyn OpIterator>,
        mut iter2: Box<dyn OpIterator>,
    ) -> Result<(), CrustyError> {
        while let Some(t1) = iter1.next()? {
            let t2 = iter2.next()?.unwrap();
            assert_eq!(t1, t2);
        }
        // assert_eq!(iter2.next()?.unwrap(), Tuple::new(vec![]));
        assert!(iter2.next()?.is_none());
        Ok(())
    }

    const WIDTH1: usize = 2;
    const WIDTH2: usize = 3;
    enum JoinType {
        NestedLoop,
        HashEq,
        SortMerge,
    }

    pub fn scan1() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 4], vec![3, 3], vec![5, 6], vec![7, 8],
            vec![1, 1], vec![3, 7], vec![5, 2], vec![7, 5]]);
        let ts = get_int_table_schema(WIDTH1);
        TupleIterator::new(tuples, ts)
    }

    pub fn scan2() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 3], vec![2, 3, 4], vec![3, 4, 5], vec![4, 5, 6],
            vec![5, 9, 7], vec![1, 10, 3], vec![2, 7, 4], vec![3, 6, 5],
        ]);
        let ts = get_int_table_schema(WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![5, 2, 1, 2, 3],
            vec![3, 3, 2, 3, 4],
            vec![1, 4, 3, 4, 5],
            vec![7, 5, 4, 5, 6],
            vec![5, 6, 3, 6, 5],
            vec![3, 7, 2, 7, 4],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    fn construct_join(
        ty: JoinType,
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        l3_method: isize,
    ) -> Box<dyn OpIterator> {
        let s1 = Box::new(scan1());
        let s2 = Box::new(scan2());
        match ty {
            JoinType::NestedLoop => Box::new(Join::new(op, left_index, right_index, s1, s2)),
            JoinType::HashEq => Box::new(HashEqJoin::new(op, left_index, right_index, s1, s2)),
            JoinType::SortMerge => Box::new(SortMergeJoin::new(op, left_index, right_index, s1, s2, l3_method)),
        }
    }

    fn test_get_schema(join_type: JoinType, l3_method: isize) {
        let op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0, l3_method);
        let expected = get_int_table_schema(WIDTH1 + WIDTH2);
        let actual = op.get_schema();
        assert_eq!(&expected, actual);
    }

    fn test_next_not_open(join_type: JoinType, l3_method: isize) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0, l3_method);
        op.next().unwrap();
    }

    fn test_rewind_not_open(join_type: JoinType, l3_method: isize) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0, l3_method);
        op.rewind().unwrap();
    }

    fn test_rewind(join_type: JoinType, l3_method: isize) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 1, 1, l3_method);
        op.open()?;
        while op.next()?.is_some() {}
        op.rewind()?;
        assert_eq!(op.next(), Ok(None));
        Ok(())
    }

    fn test_join_m_way() -> Result<(), CrustyError> {
        // left run
        let left_run = create_tuple_list(vec![
            vec![5, 1], vec![3, 8], vec![1, 10], vec![1, 20]]);
        // right runs
        let mut right_run = create_tuple_list(vec![
            vec![5, 1], vec![3, 2], vec![7, 3], vec![1, 4],
            vec![1, 5], vec![3, 6], vec![5, 7], vec![7, 8]]);
        // join predicate
        let pre = JoinPredicate::new(SimplePredicateOp::Equals, 1, 1);

        // join the result
        let res = join_m_way(left_run, right_run, pre);
        // expected
        let target = create_tuple_list(vec![
            vec![5, 1, 5, 1],
            vec![3, 8, 7, 8],
        ]);

        let ts = get_int_table_schema(4);

        let mut target_op = Box::new(TupleIterator::new(target, ts.clone()));
        let mut res_op = Box::new(TupleIterator::new(res, ts.clone()));
        target_op.open()?;
        res_op.open()?;
        match_all_tuples(target_op, res_op)
    }

    fn test_join_m_pass() -> Result<(), CrustyError> {
        // left run
        let left_run = create_tuple_list(vec![
            vec![5, 17], vec![3, 18], vec![1, 20], vec![1, 30]]);
        // right runs
        let mut right_run1 = create_tuple_list(vec![
            vec![5, 1], vec![3, 2], vec![7, 3], vec![1, 4],
            vec![1, 5], vec![3, 6], vec![5, 7], vec![7, 8]]);
        let mut right_run2 = create_tuple_list(vec![
            vec![5, 9], vec![3, 10], vec![7, 11], vec![1, 12],
            vec![1, 13], vec![3, 14], vec![5, 15], vec![7, 16]]);
        let mut right_run3 = create_tuple_list(vec![
            vec![6, 17], vec![5, 18], vec![7, 19], vec![1, 20],
            vec![1, 21], vec![3, 22], vec![5, 23], vec![7, 24]]);
        let right_runs = vec![right_run1, right_run2, right_run3];
        // join predicate
        let pre = JoinPredicate::new(SimplePredicateOp::Equals, 1, 1);

        // join the result
        let res = join_m_pass(left_run, right_runs, pre);
        // expected
        let target = create_tuple_list(vec![
            vec![5, 17, 6, 17],
            vec![3, 18, 5, 18],
            vec![1, 20, 1, 20],
        ]);

        let ts = get_int_table_schema(4);

        let mut target_op = Box::new(TupleIterator::new(target, ts.clone()));
        let mut res_op = Box::new(TupleIterator::new(res, ts.clone()));
        target_op.open()?;
        res_op.open()?;
        match_all_tuples(target_op, res_op)
    }

    fn test_sort_m_way_l3(){
        let mut run1 = create_tuple_list(vec![
            vec![5, 17], vec![3, 18], vec![7, 19], vec![1, 20],
            vec![1, 21], vec![3, 22], vec![5, 23], vec![7, 24]]);
        let mut run2 = create_tuple_list(vec![
            vec![5, 9], vec![3, 10], vec![7, 11], vec![1, 12],
            vec![1, 13], vec![3, 14], vec![5, 15], vec![7, 16]]);
        let mut run3 = create_tuple_list(vec![
            vec![5, 1], vec![3, 2], vec![7, 3], vec![1, 4],
            vec![1, 5], vec![3, 6], vec![5, 7], vec![7, 8]]);
        // let tuples = vec![run1, run2, run3];
        let tuples = vec![run1];
        let res = sort_m_way_l3(
            tuples,
            Tuple::new(vec![Field::IntField(5), Field::IntField(17)]),
            Tuple::new(vec![Field::IntField(7), Field::IntField(24)]),
            1);
        // assert_eq!(
        //     create_tuple_list(vec![
        //         vec![5, 1], vec![3, 2], vec![7, 3], vec![1, 4],
        //         vec![1, 5], vec![3, 6], vec![5, 7], vec![7, 8]]),
        //     *res.get(0).unwrap());
        // assert_eq!(
        //     create_tuple_list(vec![
        //         vec![5, 9], vec![3, 10], vec![7, 11], vec![1, 12],
        //         vec![1, 13], vec![3, 14], vec![5, 15], vec![7, 16]]),
        //     *res.get(1).unwrap());
        // assert_eq!(
        //     create_tuple_list(vec![
        //         vec![5, 17], vec![3, 18], vec![7, 19], vec![1, 20],
        //         vec![1, 21], vec![3, 22], vec![5, 23], vec![7, 24]]),
        //     *res.get(2).unwrap());
        assert_eq!(
            create_tuple_list(vec![vec![5, 17], vec![3, 18], vec![7, 19],]),
            *res.get(0).unwrap());
        assert_eq!(
            create_tuple_list(vec![vec![1, 20], vec![1, 21]]),
            *res.get(1).unwrap());
        assert_eq!(
            create_tuple_list(vec![vec![3, 22], vec![5, 23], vec![7, 24]]),
            *res.get(2).unwrap());
    }

    fn test_merge_1_to_2() {
        let mut run1 = create_tuple_list(vec![
            vec![5, 17], vec![3, 18], vec![7, 19], vec![1, 20]]);
        let mut run2 = create_tuple_list(vec![
            vec![5, 9], vec![3, 10], vec![7, 11], vec![1, 12]]);
        let tuples = vec![run1, run2];
        let res = merge_1_to_2(tuples);
        let mut expected = Vec::new();
        expected.push(create_tuple_list(vec![
            vec![5, 17], vec![3, 18], vec![7, 19], vec![1, 20],
            vec![1, 12], vec![7, 11], vec![3, 10], vec![5, 9]]));
        assert_eq!(res, expected);
    }

    fn test_level_one_sort() {
        let mut tuples = create_tuple_list(vec![vec![1, 8], vec![3, 2], vec![5, 1], vec![7, 4]]);
        tuples = sort_run_l1(tuples, 1);
        assert_eq!(create_tuple_list(vec![vec![5, 1], vec![3, 2], vec![7, 4], vec![1, 8]]),
                   tuples);
    }

    fn test_level_two_sort() {
        let mut tuples = create_tuple_list(vec![
            vec![5, 1], vec![3, 2], vec![7, 4], vec![1, 8],
            vec![1, 9], vec![3, 7], vec![5, 5], vec![7, 0]]);
        tuples = sort_run_l2(tuples, 1);
        assert_eq!(
            create_tuple_list(vec![vec![7, 0], vec![5, 1], vec![3, 2], vec![7, 4],
                                   vec![5, 5], vec![3, 7], vec![1, 8], vec![1, 9]]),
            tuples);
    }

    fn test_final(
        ty: JoinType,
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        l3_method: isize,
    ) {
        let s1 = Box::new(scan1());
        let s2 = Box::new(scan2());
        let mut opI = match ty {
            JoinType::SortMerge => Box::new(SortMergeJoin::new(op, left_index, right_index, s1, s2, l3_method)),
            JoinType::NestedLoop => Box::new(SortMergeJoin::new(op, left_index, right_index, s1, s2, l3_method)),
            JoinType::HashEq => Box::new(SortMergeJoin::new(op, left_index, right_index, s1, s2, l3_method)),
        };
        opI.open();
        opI.next();
        let res = opI.deref().l3_runs_l.clone();
        if l3_method == 1 {
            assert_eq!(res, vec![
                create_tuple_list(vec![vec![5, 2, 1, 2, 3], vec![3, 3, 2, 3, 4], vec![1, 4, 3, 4, 5]]),
                create_tuple_list(vec![vec![7, 5, 4, 5, 6], vec![5, 6, 3, 6, 5], vec![3, 7, 2, 7, 4],]),
                create_tuple_list(vec![]),
            ]);
        } else {
            assert_eq!(res,
                       vec![create_tuple_list(vec![
                           vec![5, 2, 1, 2, 3],
                           vec![3, 3, 2, 3, 4],
                           vec![1, 4, 3, 4, 5],
                           vec![7, 5, 4, 5, 6],
                           vec![5, 6, 3, 6, 5],
                           vec![3, 7, 2, 7, 4],
                       ])]);
        }

    }

    mod sort_merge_join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::SortMerge, 1);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::SortMerge, 1);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::SortMerge, 1);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::SortMerge, 1)
        }

        #[test]
        fn eq_join_m_way() {
            // test_eq_join(JoinType::SortMerge, 1)
            test_final(JoinType::SortMerge, SimplePredicateOp::Equals, 1, 1, 1);
        }

        #[test]
        fn eq_join_m_pass() {
            // test_eq_join(JoinType::SortMerge, 2)
            test_final(JoinType::SortMerge, SimplePredicateOp::Equals, 1, 1, 2);
        }

        #[test]
        fn sort_m_way() {
            test_sort_m_way_l3();
        }

        #[test]
        fn sort_l1() {
            test_level_one_sort();
        }

        #[test]
        fn sort_l2() {
            test_level_two_sort();
        }

        #[test]
        fn merge_1_2() {
            test_merge_1_to_2();
        }

        #[test]
        fn join_mway() -> Result<(), CrustyError> {
            test_join_m_way()
        }

        #[test]
        fn join_mpass() -> Result<(), CrustyError> {
            test_join_m_pass()
        }
    }
}
