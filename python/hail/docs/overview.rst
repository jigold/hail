.. _sec-overview:

========
Overview
========

-------------
Why use Hail?
-------------
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
Keys are important for joining tables together (discussed below). Important table
attributes are `columns`, `schema`, `global_schema`, `key`, and `num_columns`.

Referencing Fields
==================

Each :class:`.Table` object has all of its row fields and global fields as
attributes in its namespace. This means that the row field `Sample` can be accessed
from table `t` with ``t.Sample`` or ``t['Sample']``. If `t` also had a global field `X`,
then it could be accessed by either ``t.X`` or ``t['X']``. Both row fields and global
fields are top level fields. Be aware that accessing a field with the `dot` notation will not work
if the field name has special characters or periods in it. The Python type of each
attribute is an :class:`.Expression`. A more detailed discussion of expressions
is <below>.

Import
======

Hail has functions to create tables from a variety of data sources.
The most common use case is to load data from a TSV or CSV file, which can be
done with the `import_table` function.

.. doctest::

    t = functions.import_table("data/kt_example1.tsv", impute=True)

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

Grouped Aggregations
====================

One operation we might want to do is group by the row field `Status` and then
compute the mean of `qPhen` for each unique value of `Status`. To do this,
we need to first create a :class:`.GroupedTable` using the `group_by` method. This
will expose the method `aggregate` which can be used to compute new row fields
over the aggregated rows.

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

Lastly, if both `t1` and `t2` have the same schema, but different rows, the rows
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

It is often useful to return a result as a local value in Python. Use the `aggregate`
method along with many aggregator functions to return the result of a query.
For example, to compute the fraction of rows with ``Status == "CASE"`` and the
mean value for `qPhen`, we can run the following command:

.. doctest::

    result = t.aggregate(frac_case = agg.fraction(t.Status == "CASE"),
                         mean_qPhen = agg.mean(t.qPhen))
    result
    result['frac_case']
    result['mean_qPhen']

.. code-block:: text

    Struct(frac_case=0.41, mean_qPhen=17594.625)
    0.41
    17594.625


Export
======

Hail provides multiple functions to export data to other formats. Tables
can be exported to TSV files with the `export` method or written to disk in Hail's
on-disk format with `write`. Tables can also be exported to Pandas tables with
`to_pandas` or to Spark Dataframes with `to_spark`. Lastly, tables can be converted
to a Hail MatrixTable with `to_matrix_table`, which is the subject of the next
section.

-----------
MatrixTable
-----------


Import
======

Common Operations
=================

    - select / drop
    - filter
    - annotate

Grouped Aggregations
====================

Joins
=====

Interacting with MatrixTables Locally
=====================================

Export
======

  - rows, entries, cols tables
  - exporting
    - write, rows_table etc.

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
  - StructExpression is splattable

---------
Functions
---------
  - min, max, count, etc.
  - aggregators
  - linear algebra
  - randomness (pcoin, etc) -- plus note on why this isn't stable
  - statistical tests
  - genetics specific
    - import vcf, gen, bgen
    - export vcf, gen, etc.
    - call stats, inbreeding, hwe aggregators
    - alternate alleles


---------------------
Where's the Genetics?
---------------------

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
  - expanding fields with splat / double splat
  - hadoop_open, etc.
