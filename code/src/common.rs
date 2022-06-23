use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, io};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::error::Error;

/// Predicate expression.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PredExpr {
    Literal(Field),
    Ident(FieldIdentifier),
}
impl PredExpr {
    /// Get the field identifier from the predicate expression.
    pub fn ident(&self) -> Option<&FieldIdentifier> {
        match self {
            PredExpr::Ident(i) => Some(i),
            _ => None,
        }
    }
}

/// Simple predicate
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SimplePredicate {
    pub left: PredExpr,
    pub op: SimplePredicateOp,
    pub right: PredExpr,
}
/// The operations which can be used in a simple predicate
impl SimplePredicateOp {
    /// Do predicate comparison.
    ///
    /// # Arguments
    ///
    /// * `left_field` - Left field of the predicate.
    /// * `right_field` - Right field of the predicate.
    pub fn compare<T: Ord>(&self, left_field: &T, right_field: &T) -> bool {
        match self {
            SimplePredicateOp::Equals => left_field == right_field,
            SimplePredicateOp::GreaterThan => left_field > right_field,
            SimplePredicateOp::LessThan => left_field < right_field,
            SimplePredicateOp::LessThanOrEq => left_field <= right_field,
            SimplePredicateOp::GreaterThanOrEq => left_field >= right_field,
            SimplePredicateOp::NotEq => left_field != right_field,
            SimplePredicateOp::All => true,
        }
    }

    /// Flip the operator.
    pub fn flip(&self) -> Self {
        match self {
            SimplePredicateOp::GreaterThan => SimplePredicateOp::LessThan,
            SimplePredicateOp::LessThan => SimplePredicateOp::GreaterThan,
            SimplePredicateOp::LessThanOrEq => SimplePredicateOp::GreaterThanOrEq,
            SimplePredicateOp::GreaterThanOrEq => SimplePredicateOp::LessThanOrEq,
            op => *op,
        }
    }
}
/// Operators for simple predicates
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum SimplePredicateOp {
    Equals,
    GreaterThan,
    LessThan,
    LessThanOrEq,
    GreaterThanOrEq,
    NotEq,
    All,
}

/// Predicate operators.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum PredicateOp {
    Equals,
    GreaterThan,
    LessThan,
    LessThanOrEq,
    GreaterThanOrEq,
    NotEq,
    All,
}
impl PredicateOp {
    /// Do predicate comparison.
    ///
    /// # Arguments
    ///
    /// * `left_field` - Left field of the predicate.
    /// * `right_field` - Right field of the predicate.
    pub fn compare<T: Ord>(&self, left_field: &T, right_field: &T) -> bool {
        match self {
            PredicateOp::Equals => left_field == right_field,
            PredicateOp::GreaterThan => left_field > right_field,
            PredicateOp::LessThan => left_field < right_field,
            PredicateOp::LessThanOrEq => left_field <= right_field,
            PredicateOp::GreaterThanOrEq => left_field >= right_field,
            PredicateOp::NotEq => left_field != right_field,
            PredicateOp::All => true,
        }
    }

    /// Flip the operator.
    pub fn flip(&self) -> Self {
        match self {
            PredicateOp::GreaterThan => PredicateOp::LessThan,
            PredicateOp::LessThan => PredicateOp::GreaterThan,
            PredicateOp::LessThanOrEq => PredicateOp::GreaterThanOrEq,
            PredicateOp::GreaterThanOrEq => PredicateOp::LessThanOrEq,
            op => *op,
        }
    }
}


/// Custom error type.
#[derive(Debug, Clone, PartialEq)]
pub enum CrustyError {
    /// IO Errors.
    IOError(String),
    /// Custom errors.
    CrustyError(String),
    /// Validation errors.
    ValidationError(String),
    /// Execution errors.
    ExecutionError(String),
    /// Transaction aborted.
    TransactionAbortedError,
}
impl fmt::Display for CrustyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                CrustyError::ValidationError(s) => format!("Validation Error: {}", s),
                CrustyError::ExecutionError(s) => format!("Execution Error: {}", s),
                CrustyError::CrustyError(s) => format!("Crusty Error: {}", s),
                CrustyError::IOError(s) => s.to_string(),
                CrustyError::TransactionAbortedError => String::from("Transaction Aborted Error"),
            }
        )
    }
}
// Implement std::convert::From for AppError; from io::Error
impl From<io::Error> for CrustyError {
    fn from(error: io::Error) -> Self {
        CrustyError::IOError(error.to_string())
    }
}
impl Error for CrustyError {}


/// Enumerate the supported dtypes.
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub enum DataType {
    Int,
    String,
}


/// For each of the dtypes, make sure that there is a corresponding field type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord, Clone, Hash)]
pub enum Field {
    IntField(i32),
    StringField(String),
}
impl Field {
    /// Function to convert a Tuple field into bytes for serialization
    ///
    /// This function always uses least endian byte ordering and stores strings in the format |string length|string contents|.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Field::IntField(x) => x.to_le_bytes().to_vec(),
            Field::StringField(s) => {
                let s_len: usize = s.len();
                let mut result = s_len.to_le_bytes().to_vec();
                let mut s_bytes = s.clone().into_bytes();
                let padding_len: usize = 128 - s_bytes.len();
                let pad = vec![0; padding_len];
                s_bytes.extend(&pad);
                result.extend(s_bytes);
                result
            }
        }
    }

    /// Unwraps integer fields.
    pub fn unwrap_int_field(&self) -> i32 {
        match self {
            Field::IntField(i) => *i,
            _ => panic!("Expected i32"),
        }
    }

    /// Unwraps string fields.
    pub fn unwrap_string_field(&self) -> &str {
        match self {
            Field::StringField(s) => &s,
            _ => panic!("Expected String"),
        }
    }
}
impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Field::IntField(x) => write!(f, "{}", x),
            Field::StringField(x) => write!(f, "{}", x),
        }
    }
}

/// Aggregation operations.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum AggOp {
    Avg,
    Count,
    Max,
    Min,
    Sum,
}
impl fmt::Display for AggOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op_str = match self {
            AggOp::Avg => "avg",
            AggOp::Count => "count",
            AggOp::Max => "max",
            AggOp::Min => "min",
            AggOp::Sum => "sum",
        };
        write!(f, "{}", op_str)
    }
}
impl AggOp {
    // Get the aggregate value for a field in a new group, based on the AggOp
    pub fn new_field(&self, field: &Field) -> Field {
        match self {
            AggOp::Count => Field::IntField(1),
            AggOp::Max | AggOp::Min => field.clone(),
            _ => {
                // Sum and average need an Int
                match field {
                    Field::IntField(_) => field.clone(),
                    _ => panic!("Field is not a IntField"),
                }
            }
        }
    }

    // Modify an existing aggregate by merging in a new value, based on the AggOp
    pub fn merge_field(&self, field: &Field, agg: &mut Field) {
        *agg = match self {
            AggOp::Count => Field::IntField(agg.unwrap_int_field() + 1),
            AggOp::Max => max(agg.clone(), field.clone()),
            AggOp::Min => min(agg.clone(), field.clone()),
            _ => Field::IntField(field.unwrap_int_field() + agg.unwrap_int_field()),
        }
    }
}


/// Represents a field identifier.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FieldIdentifier {
    /// The name of table that column is present in.
    table: String,
    /// The name of the column being referenced.
    column: String,
    /// The alias given to the output field.
    alias: Option<String>,
    /// An aggregate operation performed on column.
    op: Option<AggOp>,
}
impl FieldIdentifier {
    /// Create a new field identifier.
    ///
    /// # Arguments
    ///
    /// * `table` - Table of the field.
    /// * `column` - Column.
    pub fn new(table: &str, column: &str) -> Self {
        Self {
            table: table.to_string(),
            column: column.to_string(),
            alias: None,
            op: None,
        }
    }

    /// Creates a new field identifier with alias.
    ///
    /// # Arguments
    ///
    /// * `table` - Table of the field.
    /// * `column` - Original column name.
    /// * `alias` - Column name alias.
    pub fn new_column_alias(table: &str, column: &str, alias: &str) -> Self {
        let mut id = Self::new(table, column);
        id.alias = Some(alias.to_string());
        id
    }

    /// Returns the table.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns the original column name.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Returns the field identifier alias.
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    /// Returns the aggregate operator.
    pub fn agg_op(&self) -> Option<AggOp> {
        self.op.clone()
    }

    /// Set an alias for the field identifier.
    ///
    /// # Argument
    ///
    /// * `alias` - Alias to set.
    pub fn set_alias(&mut self, alias: String) {
        self.alias = Some(alias);
    }

    /// If an op is some, sets the alias to a default alias>
    pub fn default_alias(&mut self) {
        if let Some(op) = self.op {
            self.alias = Some(format!("{}_{}", op, self.column));
        }
    }

    /// Set an aggregation operation.
    ///
    /// # Arguments
    ///
    /// * `op` - Aggregation operation to set.
    pub fn set_op(&mut self, op: AggOp) {
        self.op = Some(op);
    }
}

/// Tuple type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Tuple {
    /// Tuple data.
    pub field_vals: Vec<Field>,
}
impl Tuple {
    /// Create a new tuple with the given data.
    ///
    /// # Arguments
    ///
    /// * `field_vals` - Field values of the tuple.
    pub fn new(field_vals: Vec<Field>) -> Self {
        Self { field_vals }
    }

    /// Get the field at index.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the field.
    pub fn get_field(&self, i: usize) -> Option<&Field> {
        self.field_vals.get(i)
    }

    /// Update the index at field.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the value to insert.
    /// * `f` - Value to add.
    ///
    /// # Panics
    ///
    /// Panics if the index is out-of-bounds.
    pub fn set_field(&mut self, i: usize, f: Field) {
        self.field_vals[i] = f;
    }

    /// Returns an iterator over the field values.
    pub fn field_vals(&self) -> impl Iterator<Item = &Field> {
        self.field_vals.iter()
    }

    /// Return the length of the tuple.
    pub fn size(&self) -> usize {
        self.field_vals.len()
    }

    /// Append another tuple with self.
    ///
    /// # Arguments
    ///
    /// * `other` - Other tuple to append.
    pub fn merge(&self, other: &Self) -> Self {
        let mut fields = self.field_vals.clone();
        fields.append(&mut other.field_vals.clone());
        Self::new(fields)
    }

    pub fn get_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(&self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        serde_cbor::from_slice(bytes).unwrap()
    }

    pub fn to_csv(&self) -> String {
        let mut res = Vec::new();
        for field in &self.field_vals {
            let val = match field {
                Field::IntField(i) => i.to_string(),
                Field::StringField(s) => s.to_string(),
            };
            res.push(val);
        }
        res.join(",")
    }
}
impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut res = String::new();
        for field in &self.field_vals {
            let val = match field {
                Field::IntField(i) => i.to_string(),
                Field::StringField(s) => s.to_string(),
            };
            res.push_str(&val);
            res.push('\t');
        }
        write!(f, "{}", res)
    }
}


pub type ContainerId = u16;
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum Constraint {
    None,
    PrimaryKey,
    Unique,
    NotNull,
    UniqueNotNull,
    ForeignKey(ContainerId), // Points to other table. Infer PK
    NotNullFKey(ContainerId),
}


/// Handle attributes. Pairs the name with the dtype.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Attribute {
    /// Attribute name.
    pub name: String,
    /// Attribute dtype.
    pub dtype: DataType,
    /// Attribute constraint
    pub constraint: Constraint,
}
impl Attribute {
    /// Create a new attribute with the given name and dtype.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute.
    /// * `dtype` - Dtype of the attribute.
    // pub fn new(name: String, dtype: DataType) -> Self { Self { name, dtype, is_pk: false } }

    pub fn new(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            constraint: Constraint::None,
        }
    }

    pub fn new_with_constraint(name: String, dtype: DataType, constraint: Constraint) -> Self {
        Self {
            name,
            dtype,
            constraint,
        }
    }

    pub fn new_pk(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            constraint: Constraint::PrimaryKey,
        }
    }

    /// Returns the name of the attribute.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the dtype of the attribute.
    pub fn dtype(&self) -> &DataType {
        &self.dtype
    }

    // TODO(williamma12): Where does the 132 come from?
    /// Returns the length of the dtype in bytes.
    pub fn get_byte_len(&self) -> usize {
        match self.dtype {
            DataType::Int => 4,
            DataType::String => 132,
        }
    }
}


/// Handle schemas.
#[derive(PartialEq, Clone, Debug)]
pub struct TableSchema {
    /// Attributes of the schema.
    attributes: Vec<Attribute>,
    /// Mapping from attribute name to order in the schema.
    name_map: HashMap<String, usize>,
}
impl Serialize for TableSchema {
    /// Custom serialize to avoid serializing name_map.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        self.attributes.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for TableSchema {
    /// Custom deserialize to avoid serializing name_map.
    fn deserialize<D>(deserializer: D) -> Result<TableSchema, D::Error>
        where
            D: Deserializer<'de>,
    {
        let attrs = Vec::deserialize(deserializer)?;
        Ok(TableSchema::new(attrs))
    }
}
impl TableSchema {
    /// Create a new schema.
    ///
    /// # Arguments
    ///
    /// * `attributes` - Attributes of the schema in the order that they are in the schema.
    pub fn new(attributes: Vec<Attribute>) -> Self {
        let mut name_map = HashMap::new();
        for (i, attr) in attributes.iter().enumerate() {
            name_map.insert(attr.name().to_string(), i);
        }
        Self {
            attributes,
            name_map,
        }
    }

    /// Create a new schema with the given names and dtypes.
    ///
    /// # Arguments
    ///
    /// * `names` - Names of the new schema.
    /// * `dtypes` - Dypes of the new schema.
    pub fn from_vecs(names: Vec<&str>, dtypes: Vec<DataType>) -> Self {
        let mut attrs = Vec::new();
        for (name, dtype) in names.iter().zip(dtypes.iter()) {
            attrs.push(Attribute::new(name.to_string(), dtype.clone()));
        }
        TableSchema::new(attrs)
    }

    /// Get the attribute from the given index.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the attribute to look for.
    pub fn get_attribute(&self, i: usize) -> Option<&Attribute> {
        self.attributes.get(i)
    }

    /// Get the index of the attribute.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute to get the index for.
    pub fn get_field_index(&self, name: &str) -> Option<&usize> {
        self.name_map.get(name)
    }

    /// Returns attribute(s) that are primary keys
    ///
    ///
    pub fn get_pks(&self) -> Vec<Attribute> {
        let mut pk_attributes: Vec<Attribute> = Vec::new();
        for attribute in &self.attributes {
            if attribute.constraint == Constraint::PrimaryKey {
                pk_attributes.push(attribute.clone());
            }
        }
        pk_attributes
    }

    /// Check if the attribute name is in the schema.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute to look for.
    pub fn contains(&self, name: &str) -> bool {
        self.name_map.contains_key(name)
    }

    /// Get an iterator of the attributes.
    pub fn attributes(&self) -> impl Iterator<Item = &Attribute> {
        self.attributes.iter()
    }

    /// Merge two schemas into one.
    ///
    /// The other schema is appended to the current schema.
    ///
    /// # Arguments
    ///
    /// * `other` - Other schema to add to current schema.
    pub fn merge(&self, other: &Self) -> Self {
        let mut attrs = self.attributes.clone();
        attrs.append(&mut other.attributes.clone());
        Self::new(attrs)
    }

    /// Returns the length of the schema.
    pub fn size(&self) -> usize {
        self.attributes.len()
    }

    /// Returns the size of the schema in bytes.
    pub fn byte_size(&self) -> usize {
        let mut total: usize = 0;
        for attr in self.attributes.iter() {
            total += attr.get_byte_len();
        }
        total
    }
}


pub trait OpIterator {
    /// Opens the iterator. This must be called before any of the other methods.
    fn open(&mut self) -> Result<(), CrustyError>;

    /// Advances the iterator and returns the next tuple from the operator.
    ///
    /// Returns None when iteration is finished.
    ///
    /// # Panics
    ///
    /// Panic if iterator is not open.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError>;

    /// Closes the iterator.
    fn close(&mut self) -> Result<(), CrustyError>;

    /// Returns the iterator to the start.
    ///
    /// Returns None when iteration is finished.
    ///
    /// # Panics
    ///
    /// Panic if iterator is not open.
    fn rewind(&mut self) -> Result<(), CrustyError>;

    /// Returns the schema associated with this OpIterator.
    fn get_schema(&self) -> &TableSchema;
}


/// Iterator over a Vec of tuples, mainly used for testing.
pub struct TupleIterator {
    /// Tuples to iterate over.
    tuples: Vec<Tuple>,
    /// Schema of the output.
    schema: TableSchema,
    /// Current tuple in iteration.
    index: Option<usize>,
}
impl TupleIterator {
    /// Create a new tuple iterator over a set of results.
    ///
    /// # Arguments
    ///
    /// * `tuples` - Tuples to iterate over.
    /// * `schema` - Schema of the output results.
    pub fn new(tuples: Vec<Tuple>, schema: TableSchema) -> Self {
        Self {
            index: None,
            tuples,
            schema,
        }
    }
}
impl OpIterator for TupleIterator {
    /// Opens the iterator without returning a tuple.
    fn open(&mut self) -> Result<(), CrustyError> {
        self.index = Some(0);
        Ok(())
    }

    /// Retrieves the next tuple in the iterator.
    ///
    /// # Panics
    ///
    /// Panics if the TupleIterator has not been opened.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        let i = match self.index {
            None => panic!("Operator has not been opened"),
            Some(i) => i,
        };
        let tuple = self.tuples.get(i);
        self.index = Some(i + 1);
        Ok(tuple.cloned())
    }

    /// Closes the tuple iterator.
    fn close(&mut self) -> Result<(), CrustyError> {
        self.index = None;
        Ok(())
    }

    /// Make iterator point to the first tuple again.
    ///
    /// # Panics
    ///
    /// Panics if the TupleIterator has not been opened.
    fn rewind(&mut self) -> Result<(), CrustyError> {
        if self.index.is_none() {
            panic!("Operator has not been opened")
        }
        self.close()?;
        self.open()
    }

    /// Returns the schema of the tuples.
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}
