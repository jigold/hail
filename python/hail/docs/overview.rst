.. _sec-overview:

.. py:currentmodule:: hail

========
Overview
========

-------------
Why use Hail?
-------------

Hail is...

  - what problems can Hail solve
  - what problems can't Hail solve

-----
Types
-----

In Python, ``5`` is of type `int` while ``"hello"`` is of type `string`. Hail has
basic types such as TBoolean,
TInt32, TInt64, TFloat32, TFloat64, TArray, TSet, TDict as well as two types for
genetic objects: TCall for the genotype call and TLocus for the genomic locus.

Hail also has container types such as TArray, TSet, and TDict, which all have
corresponding element types. For example, a list of integers
in Python would have the type TArray[TInt32] where TInt32 is the element type. Unlike
dictionaries in Python, all keys in a Hail dictionary must have the same type
and the same for values. A dictionary of ``{"a": 1, 2: "b"}`` would be an invalid
dictionary in Hail, but ``{"a": 1, "b": 2}`` would be because the keys are all
strings and the values are all ints. The type of this dictionary would be
TDict[TString, TInt32].

One way to combine types together to form more complicated structures is with the
TStruct type. TStruct is a container with an ordered set of fields. A
field consists of a name and a type. You can think of this as a list of tuple
pairs in Python where the first element of the tuple is a string corresponding to
the field name and the second element is the Hail type. The field names in a TStruct
must be unique. An example valid TStruct is
TStruct["a": TString, "b": TBoolean, "c": TInt32]. A Python object that corresponds
to this type is ``{"a": "foo", "b": True, "c": 5}``.

More complex types can be created by nesting TStructs. For example, the type
TStruct["a": TStruct["foo": TInt, "baz": TString], "b": TStruct["bar": TArray[TInt32]]] consists
of two fields "a" and "b" each with the type TStruct, but with different fields.
A valid Python object for this type is ``{"a": {"foo": 3, "baz": "hello"}, "b":
{"bar": [1, 2, 3]}}``.

TStructs are used throughout Hail to create complex schemas representing
the structure of data.

-----------
Expressions
-----------
  - capture / broadcast
  - basic operations depending on type
  - if else
  - bind
  - can add 5 + ds.AC or ds.AC + 5 => IntExpression
  - boolean
  - propogation of missingness
  - debugging methods
  - How are these different than Hail objects?
  - composable x = mt.DP + mt.GQ
    x + 5
  - define null expressions

---------
Functions
---------
  - min, max, count, etc.
  - randomness (pcoin, etc) -- plus note on why this isn't stable
  - statistical tests
  - aggregators

-----
Table
-----

A :class:`~hail.Table` is the Hail equivalent of a SQL table, a Pandas Dataframe, an R Dataframe,
a dyplr Tibble, or a Spark Dataframe. It consists of rows of data conforming to
a given schema where each column (row field) in the dataset is of a specific type.

An example of a table is below:

+---------+---------+-------+
| Sample  | Status  | qPhen |
+---------+---------+-------+
| String  | String  | Int32 |
+---------+---------+-------+
| HG00096 | CASE    | 27704 |
| HG00097 | CASE    | 16636 |
| HG00099 | CASE    |  7256 |
| HG00100 | CASE    | 28574 |
| HG00101 | CASE    | 12088 |
| HG00102 | CASE    | 19740 |
| HG00103 | CASE    |  1861 |
| HG00105 | CASE    | 22278 |
| HG00106 | CASE    | 26484 |
| HG00107 | CASE    | 29726 |
+---------+---------+-------+

It's schema is

.. code-block::text

    TStruct(Sample=TString, Status=TString, qPhen = TInt32)


Global Fields
=============

In addition to row fields, Hail tables also have global fields. You can think of globals as
extra fields in the table whose values are identical for every row. For example,
the same table above with the global field ``X = 5`` can be thought of as

+---------+---------+-------+-------+
| Sample  | Status  | qPhen |     X |
+---------+---------+-------+-------+
| String  | String  | Int32 | Int32 |
+---------+---------+-------+-------+
| HG00096 | CASE    | 27704 |     5 |
| HG00097 | CASE    | 16636 |     5 |
| HG00099 | CASE    |  7256 |     5 |
| HG00100 | CASE    | 28574 |     5 |
| HG00101 | CASE    | 12088 |     5 |
| HG00102 | CASE    | 19740 |     5 |
| HG00103 | CASE    |  1861 |     5 |
| HG00105 | CASE    | 22278 |     5 |
| HG00106 | CASE    | 26484 |     5 |
| HG00107 | CASE    | 29726 |     5 |
+---------+---------+-------+-------+

but the value ``5`` is only stored once for the entire dataset and NOT once per
row of the table. The output of `describe` lists what all of the row
fields and global fields are.

.. code-block::text

    Global fields:
        'X': Int32

    Row fields:
        'Sample': String
        'Status': String
        'qPhen': Int32


Keys
====

Row fields can be specified to be the keys of the table with the method `key_by`.
Keys are important for joining tables together (discussed below).

Referencing Fields
==================

Each :class:`.Table` object has all of its row fields and global fields as
attributes in its namespace. This means that the row field `Sample` can be accessed
from table `t` with ``t.Sample`` or ``t['Sample']``. If `t` also had a global field `X`,
then it could be accessed by either ``t.X`` or ``t['X']``. Both row fields and global
fields are top level fields. Be aware that accessing a field with the `dot` notation will not work
if the field name has special characters or periods in it. The Python type of each
attribute is an :class:`.Expression` that also contains context about its type and source,
in this case a row field of table `t`.

    >>> t

.. code-block:: text

    is.hail.table.Table@42dd544f

    >>> t.Sample

.. code-block:: text

    <hail.expr.expression.StringExpression object at 0x10b498290>
      Type: String
      Index:
        row of is.hail.table.Table@42dd544f

Import
======

Hail has functions to create tables from a variety of data sources.
The most common use case is to load data from a TSV or CSV file, which can be
done with the `import_table` function.

.. doctest::

    t = methods.import_table("data/kt_example1.tsv", impute=True)

A table can also be created from Python
objects with `parallelize`. For example, a table with only the first two rows
above could be created from Python objects.

.. doctest::

    rows = [{"Sample": "HG00096", "Status": "CASE", "qPhen": 27704},
            {"Sample": "HG00097", "Status": "CASE", "qPhen": 16636}]

    schema = TStruct(["Sample", "Status", "qPhen"], [TString(), TString(), TInt32()])

    t_new = Table.parallelize(rows, schema)

Examples of genetics-specific import methods are
`import_interval_list`, `import_fam`, and `import_bed`. Many Hail methods also
return tables.

Common Operations
=================

The main operations on a table are `select` and `drop` to add or remove row fields,
`filter` to either keep or remove rows based on a condition, and `annotate` to add
new row fields or update the values of existing row fields. For example, extending
the example table above, we can filter the table to only contain rows where
``qPhen < 15000``, add a new row field `SampleInt` which is the integer component of the row
field `Sample`, add a new global field `foo`, and select only the row fields `SampleInt` and
`qPhen` as well as define a new row field `bar` which is the product of `qPhen` and `SampleInt`.
Lastly, we can use `show` to view the first 10 rows of the new table.


# FIXME: add transmute and explode

.. doctest::

    t_new = t.filter(t['qPhen'] < 15000)
    t_new = t_new.annotate(SampleInt = t.Sample.replace("HG", "").to_int32())
    t_new = t_new.annotate_globals(foo = 131)
    t_new = t_new.select(t['SampleInt'], t['qPhen'], bar = t['qPhen'] * t['SampleInt'])
    t_new.show()

The final output is

.. code-block:: text

    +-----------+-------+---------+
    | SampleInt | qPhen |     bar |
    +-----------+-------+---------+
    |     Int32 | Int32 |   Int32 |
    +-----------+-------+---------+
    |        99 |  7256 |  718344 |
    |       101 | 12088 | 1220888 |
    |       103 |  1861 |  191683 |
    |       113 |  8845 |  999485 |
    |       116 | 12742 | 1478072 |
    |       121 |  4832 |  584672 |
    |       124 |  2691 |  333684 |
    |       125 | 14466 | 1808250 |
    |       127 | 10224 | 1298448 |
    |       128 |  2807 |  359296 |
    +-----------+-------+---------+

with the following schema:

.. code-block:: text

    Global fields:
        'foo': Int32

    Row fields:
        'SampleInt': Int32
        'qPhen': Int32
        'bar': Int32

Aggregations
============

A commonly used operation is to compute an aggregate statistic over the rows of
the dataset. Hail provides an `aggregate`
method along with many `aggregator functions` to return the result of a query.
For example, to compute the fraction of rows with ``Status == "CASE"`` and the
mean value for `qPhen`, we can run the following command:

.. doctest::

    result = t.aggregate(frac_case = agg.fraction(t.Status == "CASE"),
                         mean_qPhen = agg.mean(t.qPhen))
    result

.. code-block:: text

    Struct(frac_case=0.41, mean_qPhen=17594.625)

We also might want to compute the mean value of `qPhen` for each unique value of `Status`.
To do this, we need to first create a :class:`.GroupedTable` using the `group_by` method. This
will expose the method `aggregate` which can be used to compute new row fields
over the grouped-by rows.

.. doctest::

    t_agg = (t.group_by('Status')
              .aggregate(mean = agg.mean(t['qPhen'])))
    t_agg.show()


.. code-block:: text

    +--------+-------------+
    | Status |        mean |
    +--------+-------------+
    | String |     Float64 |
    +--------+-------------+
    | CASE   | 1.83183e+04 |
    | CTRL   | 1.70995e+04 |
    +--------+-------------+

Joins
=====

To join the row fields of two tables together, Hail provides a `join` method with
options for how to join the rows together (left, right, inner, outer). The tables are
joined by the row fields designated as keys. The number of keys and their types
must be identical between the two tables. However, the names of the keys do not
need to be identical. Use the `key` attribute to view the current
table row keys and the `key_by` method to change the table keys. If top level
row field names overlap between the two tables, the second table's field names
will be appended with a unique identifier "_N".

.. doctest::

    t1 = t.key_by('Sample')
    t2 = (functions.import_table("data/kt_example2.tsv", impute=True)
                   .key_by('Sample'))

    t_join = t1.join(t2)
    t_join.show()

.. code-block:: text

    +---------+--------+-------+-------------+--------+
    | Sample  | Status | qPhen |      qPhen2 | qPhen3 |
    +---------+--------+-------+-------------+--------+
    | String  | String | Int32 |     Float64 |  Int32 |
    +---------+--------+-------+-------------+--------+
    | HG00097 | CASE   | 16636 | 3.32720e+03 |  16626 |
    | HG00128 | CASE   |  2807 | 5.61400e+02 |   2797 |
    | HG00111 | CASE   | 30065 | 6.01300e+03 |  30055 |
    | HG00122 | CASE   |    NA | 0.00000e+00 |    -10 |
    | HG00107 | CASE   | 29726 | 5.94520e+03 |  29716 |
    | HG00136 | CASE   | 12348 | 2.46960e+03 |  12338 |
    | HG00113 | CASE   |  8845 | 1.76900e+03 |   8835 |
    | HG00103 | CASE   |  1861 | 3.72200e+02 |   1851 |
    | HG00120 | CASE   | 19599 | 3.91980e+03 |  19589 |
    | HG00114 | CASE   | 31255 | 6.25100e+03 |  31245 |
    +---------+--------+-------+-------------+--------+

In addition to using the `join` method, Hail provides an additional join syntax
using Python's bracket notation. For example, below we add the column `qPhen2` from table
2 to table 1 by joining on the row field `Sample`:

.. doctest::

    t1 = t1.annotate(qPhen2 = t2[t.Sample].qPhen2)
    t1.show()

.. code-block:: text

    +---------+--------+-------+-------------+
    | Sample  | Status | qPhen |      qPhen2 |
    +---------+--------+-------+-------------+
    | String  | String | Int32 |     Float64 |
    +---------+--------+-------+-------------+
    | HG00180 | CTRL   | 27337 |          NA |
    | HG00160 | CTRL   | 29590 |          NA |
    | HG00141 | CTRL   | 25689 |          NA |
    | HG00097 | CASE   | 16636 | 3.32720e+03 |
    | HG00145 | CTRL   |  7641 |          NA |
    | HG00158 | CTRL   | 12369 |          NA |
    | HG00243 | CTRL   | 18065 |          NA |
    | HG00128 | CASE   |  2807 | 5.61400e+02 |
    | HG00234 | CTRL   | 18268 |          NA |
    | HG00111 | CASE   | 30065 | 6.01300e+03 |
    +---------+--------+-------+-------------+

The general format of the key word argument to `annotate` is

.. code-block:: text

    new_field_name = <other table> [<this table's keys >].<field to insert>

Note that both `t1` and `t2` have been keyed by the column `Sample` with the same
type TString. This syntax for joining can be extended to add new row fields
from many tables simultaneously.

If both `t1` and `t2` have the same schema, but different rows, the rows
of the two tables can be combined with `union`.


Interacting with Tables Locally
===============================

Hail has many useful methods for interacting with tables locally such as in an
iPython notebook. Use the `show` method to see the first 10 rows of a table.

`take` will collect the first `n` rows of a table into a local Python list

.. doctest::

    x = t.take(3)
    x

.. code-block:: text

    [Struct(Sample=HG00096, Status=CASE, qPhen=27704),
     Struct(Sample=HG00097, Status=CASE, qPhen=16636),
     Struct(Sample=HG00099, Status=CASE, qPhen=7256)]

Note that each element of the list is a Struct whose elements can be accessed using
Python's get attribute notation

.. doctest::

    x[0].qPhen

.. code-block:: text

    27704

When testing pipelines, it is helpful to subset the dataset to the first `n` rows
with the `head` method. The result of `head` is a new Table rather than a local
list of Struct elements as with `take` or a printed representation with `show`.
`sample` will return a randomly sampled fraction of the dataset. This is useful
for having a smaller, but random subset of the data.

`describe` is a useful method for showing all of the fields of the table and their
types. The complete table schemas can be accessed with `schema` and `global_schema`.
The row fields that are keys can be accessed with `key`. Lastly, the `num_columns`
attribute returns the number of row fields and the `count` method returns the
number of rows in the table.

Export
======

Hail provides multiple functions to export data to other formats. Tables
can be exported to TSV files with the `export` method or written to disk in Hail's
on-disk format with `write` and read back in with `read_table`. Tables can also be exported to Pandas tables with
`to_pandas` or to Spark Dataframes with `to_spark`. Lastly, tables can be converted
to a Hail :class:`.MatrixTable` with `to_matrix_table`, which is the subject of the next
section.

-----------
MatrixTable
-----------

A :class:`.MatrixTable` is a distributed two-dimensional dataset consisting of
four components: a two-dimensional matrix where each entry is indexed by row
key(s) and column key(s), a corresponding rows table that stores all of the row
fields which are constant for every column in the dataset, a corresponding
columns table that stores all of the column fields that are constant for every
row in the dataset, and a set of global fields that are constant for every entry
in the dataset.

Unlike a :class:`.Table` which has two schemas, a matrix table has four schemas
that define the structure of the dataset. The rows table has a `row_schema`, the
columns table has a `col_schema`, each entry in the matrix follows the schema
defined by `entry_schema`, and the global fields have a `global_schema`.

In addition, there are different operations on the matrix for each dimension
of the data. For example, instead of just `filter` for tables, matrix tables
have `filter_rows`, `filter_cols`, and `filter_entries`.

One equivalent way of representing this data is in one combined table encompassing
all row, column, and global fields with one row in the table per entry in the matrix (coordinate form).
Hail does not store the data in this format as it is inefficient when computing
results and the on-disk representation would be massive as constant values are
repeated per entry in the dataset.

Keys
====

Analogous to tables, matrix tables also have keys. However, instead of one key, matrix
tables have two keys: one for the rows table and the other for the columns table.  Entries
are indexed by both the row keys and column keys. The keys
can be accessed with the attributes `row_key` and `col_key` and set with the methods
`key_rows_by` and `key_cols_by`. Keys are used for joining tables together (discussed below).

In addition, each matrix table has a `partition_key`. This key is used for specifying
the ordering of the matrix table along the row dimension, which is important for
performance.


Referencing Fields
==================

All fields (row, column, global, entry)
are top-level and exposed as attributes on the :class:`.MatrixTable` object.
For example, if the matrix table `mt` had a row field `locus`, this field
could be referenced with either ``mt.locus`` or ``mt['locus']``. The former
access pattern does not work with field names with special characters or periods
in it.

The result of referencing a field from a matrix table is an :class:`Expression` which knows its type
and knows its source as well as whether it is a row field, column field, entry field, or global field.
Hail uses this context to know which operations are allowed for a given expression.

When evaluated in a Python interpreter, we can see ``mt.locus`` is a :class:`.LocusExpression`
with type `Locus(GRCh37)` and it is a row field of the MatrixTable `mt`.

    >>> mt

.. code-block:: text

    <hail.matrixtable.MatrixTable at 0x10a6a3e50>

    >>> mt.locus

.. code-block:: text

    <hail.expr.expression.LocusExpression object at 0x10b17f790>
      Type: Locus(GRCh37)
      Index:
        row of <hail.matrixtable.MatrixTable object at 0x10a6a3e50>

Likewise, ``mt.DP`` would be an :class:`.Int32Expression` with type `Int32` and
is an entry field of `mt`. It is indexed by both rows and columns as denoted
by its indices when printing the expression.

    >>> mt.DP

.. code-block:: text

    <hail.expr.expression.Int32Expression object at 0x10b2cec10>
      Type: Int32
      Indices:
        column of <hail.matrixtable.MatrixTable object at 0x10a6a3e50>
        row of <hail.matrixtable.MatrixTable object at 0x10a6a3e50>


Import
======

Hail provides four functions to import genetic datasets as matrix tables from a
variety of file formats: `import_vcf`, `import_plink`, `import_bgen`, and
`import_gen`. We will be adding a function to import a matrix table from a TSV
file in the future.

An example of importing data from a VCF file to a matrix table follows:

    >>> mt = methods.import_vcf('data/example2.vcf.bgz')

The `describe` method shows the schemas for the global fields, column fields,
row fields, entry fields, as well as the column key(s), the row key(s), and the
partition key.

    >>> mt.describe()
    ----------------------------------------
    Global fields:
        None
    ----------------------------------------
    Column fields:
        's': String
    ----------------------------------------
    Row fields:
        'locus': Locus(GRCh37)
        'alleles': Array[String]
        'rsid': String
        'qual': Float64
        'filters': Set[String]
        'info': Struct {
            NEGATIVE_TRAIN_SITE: Boolean,
            HWP: Float64,
            AC: Array[Int32],
            culprit: String,
            .
            .
            .
        }
    ----------------------------------------
    Entry fields:
        'GT': Call
        'AD': Array[+Int32]
        'DP': Int32
        'GQ': Int32
        'PL': Array[+Int32]
    ----------------------------------------
    Column key:
        's': String
    Row key:
        'locus': Locus(GRCh37)
        'alleles': Array[String]
    Partition key:
        'locus': Locus(GRCh37)
    ----------------------------------------


Common Operations
=================

Like tables, Hail provides a number of useful methods for manipulating data in a
matrix table.

**Filter**

Hail has three methods to filter a matrix table based on a condition:

- `filter_rows`
- `filter_cols`
- `filter_entries`

Filter methods take a `boolean expression` as its argument. The simplest boolean
expression is ``False``, which will remove all rows, or ``True``, which will
keep all rows.

Just filtering out all rows, columns, or entries isn't particularly useful. Often,
we want to filter parts of a dataset based on a condition the elements satisfy.
A commonly used application in genetics is to only keep rows where the number of
alleles is two (biallelic). This can be expressed as follows:

    >>> mt_biallelic = mt.filter_rows(mt['alleles'].length() == 2)

So what is going on here? The reference to the row field `alleles` returns an
expression of type `Array[String] :class:`.ArrayStringExpression`. Array expressions
have multiple methods on them including `length` which returns the number of elements
in the array. This expression representing the length of the row field `alleles`
is compared to the number 2 with the `==` comparison operator to return a boolean expression.
Note that the expression `mt['alleles'].length() == 2` is not actually a value
in Python. Rather it represents a recipe for computation that is then used by
Hail to evaluate each row in the matrix table for whether the condition is met.

More complicated expressions can be written with a combination of Hail's functions.
An example of filtering columns where the fraction of non-missing elements for
the entry field `GT` is greater than 0.95 utilizes the function `is_defined` and
the aggregator function `fraction`.

    >>> mt_new = mt.filter_cols(agg.fraction(functions.is_defined(mt.GT)) >= 0.95)
    >>> mt.count_cols()
    100
    >>> mt_new.count_cols()
    91

In this case, the expression ``mt.GT`` is an aggregable because the function context
is an operation on columns (`filter_cols`). This means for each column in the
matrix table, we have N `GT` entries where N is the number of rows in the dataset.
Aggregables cannot be realized as an actual value, so we must use an aggregator
function to reduce the aggregable to an actual value.

In the example above, `functions.is_defined` is applied to each element of the aggregable ``mt.GT``
to transform it from an Aggregable[Call] to an Aggregable[Boolean] where ``True``
means the value `GT` was defined or ``False`` for missing. `agg.fraction` requires
an Aggregable[Boolean] for its input, which it then reduces to a single value by computing the
number of ``True`` values divided by `N`, the length of the aggregable. The result
of `fraction` is a single value per column, which can then be compared
to the value `0.95` with the `>=` comparison operator.

Hail also provides two methods to filter columns or rows based on an input list
of values. This is useful if you have a known subset of the dataset you want to
subset to.

- `filter_rows_list`
- `filter_cols_list`


**Annotate**

Hail provides four methods to add fields to a matrix table or update existing fields:

- `annotate_rows`
- `annotate_cols`
- `annotate_entries`
- `annotate_globals`

Annotate methods take key-word arguments where the key is the name of the new
field to add and the value is an expression specifying what should be added.

The simplest example is adding a new global field `foo` that just contains the constant
5.

    >>> mt_new = mt.annotate_globals(foo = 5)
    >>> mt.global_schema.pretty()
    Struct {
        foo: Int32
    }

Another example is adding a new row field `call_rate` which computes the fraction
of non-missing entries `GT` per row. This is similar to the filter example described
above, except the result of `agg.fraction(functions.is_defined(mt.GT))` is stored
as a new row field in the matrix table and the operation is performed over rows
rather than columns.

    >>> mt_new = mt.annotate_rows(call_rate = agg.fraction(functions.is_defined(mt.GT)))

Annotate methods are also useful for updating values. For example, to update the
GT entry field to be missing if `GQ` is less than 20, we can do the following:

    >>> mt_new = mt.annotate_entries(GT = functions.cond(mt.GQ < 20,
    ...                                                  functions.null(TCall()),
    ...                                                  mt.GT))

**Select**

Select is used to create a new schema for a dimension of the matrix table. For
example, following the matrix table schemas from importing a VCF file (shown above),
to create a hard calls dataset where each entry only contains the `GT` field
one can do the following:

    >>> mt_new = mt.select_entries('GT')
    >>> mt_new.entry_schema.pretty()
    Struct {
        GT: Call
    }

Hail has four select methods that correspond to modifying the schema of the row
fields, the column fields, the entry fields, and the global fields.

- `select_rows`
- `select_cols`
- `select_entries`
- `select_globals`

Each method can take either strings referring to top-level fields, an attribute
reference (useful for accessing nested fields), as well as key word arguments
``KEY=VALUE`` to compute new fields. The Python unpack operator ``**`` can be used
to specify that all fields of a Struct should become top level fields. However,
be aware that all field names must be unique across rows, columns, entries, and globals.
So in this example, `**mt['info']` would fail because `DP` already exists as an entry field.

The example below will keep
the row fields `locus` and `alleles` as well as add two new fields: `AC` is making
the subfield `AC` into a top level field and `n_filters` is a new computed field.

.. doctest::

    mt_new = mt.select_rows('locus',
                            'alleles',
                            AC = mt['info']['AC'],
                            n_filters = mt['filters'].length())

    mt_new.row_schema.pretty()

.. code-block:: text

    Struct {
        locus: Locus(GRCh37),
        alleles: Array[String],
        AC: Array[Int32],
        n_filters: Int32
    }

The order of the fields entered as arguments will be maintained in the new
matrix table.

**Drop**

Analogous to `select`, `drop` will remove any top level field. An example of
removing the `GQ` entry field is

    >>> mt_new = mt.drop('GQ')
    >>> mt_new.entry_schema.pretty()
    Struct {
        GT: Call,
        AD: Array[+Int32],
        DP: Int32,
        PL: Array[+Int32]
    }

Hail also has two methods to drop all rows or all columns from the matrix table:
`drop_rows` and `drop_cols`.

**Explode**

Explode is used to unpack a row or column field that is of type array or
set.

- `explode_rows`
- `explode_cols`

One use case of explode is to duplicate rows:

    >>> mt_new = mt.annotate_rows(replicate_num = [1, 2])
    >>> mt_new = mt_new.explode_rows(mt_new['replicate_num'])
    >>> mt.count_rows()
    7
    >>> mt_new.count_rows()
    14

    >>> mt_new.rows_table().select('locus', 'alleles', 'replicate_num').show()

.. code-block:: text

    +---------------+-----------------+---------------+
    | locus         | alleles         | replicate_num |
    +---------------+-----------------+---------------+
    | Locus(GRCh37) | Array[String]   |         Int32 |
    +---------------+-----------------+---------------+
    | 20:12990057   | ["T","A"]       |             1 |
    | 20:12990057   | ["T","A"]       |             2 |
    | 20:13090733   | ["A","AT"]      |             1 |
    | 20:13090733   | ["A","AT"]      |             2 |
    | 20:13695824   | ["CAA","C"]     |             1 |
    | 20:13695824   | ["CAA","C"]     |             2 |
    | 20:13839933   | ["T","C"]       |             1 |
    | 20:13839933   | ["T","C"]       |             2 |
    | 20:15948326   | ["GAAAAAA","G"] |             1 |
    | 20:15948326   | ["GAAAAAA","G"] |             2 |
    +---------------+-----------------+---------------+

Aggregations
============

Like :class:`Table`, Hail provides three methods to compute aggregate statistics:

- `aggregate_rows`
- `aggregate_cols`
- `aggregate_entries`



 A commonly used operation is to compute an aggregate statistic over the rows of
the dataset. Hail provides an `aggregate`
method along with many `aggregator functions` to return the result of a query.
For example, to compute the fraction of rows with ``Status == "CASE"`` and the
mean value for `qPhen`, we can run the following command:

.. doctest::

    result = t.aggregate(frac_case = agg.fraction(t.Status == "CASE"),
                         mean_qPhen = agg.mean(t.qPhen))
    result

.. code-block:: text

    Struct(frac_case=0.41, mean_qPhen=17594.625)

We also might want to compute the mean value of `qPhen` for each unique value of `Status`.
To do this, we need to first create a :class:`.GroupedTable` using the `group_by` method. This
will expose the method `aggregate` which can be used to compute new row fields
over the grouped-by rows.

.. doctest::

    t_agg = (t.group_by('Status')
              .aggregate(mean = agg.mean(t['qPhen'])))
    t_agg.show()


.. code-block:: text

    +--------+-------------+
    | Status |        mean |
    +--------+-------------+
    | String |     Float64 |
    +--------+-------------+
    | CASE   | 1.83183e+04 |
    | CTRL   | 1.70995e+04 |
    +--------+-------------+

Joins
=====

Interacting with MatrixTables Locally
=====================================

describe, sample, head
rows_table, cols_table, entries_table => exporting results
count_cols, count_rows

Export
======

  - rows, entries, cols tables
  - exporting
    - write, rows_table etc.
`read_matrix_table`


--------------------------
Other Hail Data Structures
--------------------------
- linear algebra
- block matrix


---------------------
Where's the Genetics?
---------------------
  - genetics specific
    - import vcf, gen, bgen
    - export vcf, gen, etc.
    - call stats, inbreeding, hwe aggregators
    - alternate alleles
- tdt
- genetics objects
- genetics types

---------------------
Python Considerations
---------------------
  - chaining methods together => not referring to correct dataset in future operations
  - varargs vs. keyword args
  - how to access attributes (square brackets vs. method accessor)
  - how to work with fields with special chars or periods in name **{'a.b': 5}


--------------------------
Performance Considerations
--------------------------
  - when to use broadcast
  - cache, persist
  - repartition
  - shuffling
  - group / join with null is bad!

-----
Other
-----
  - hadoop_open, etc.

