.. _sec-api:

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
| String  | Boolean | Int32 |
+---------+---------+-------+
| HG00096 | true    | 27704 |
| HG00097 | true    | 16636 |
| HG00099 | true    |  7256 |
| HG00100 | true    | 28574 |
| HG00101 | true    | 12088 |
| HG00102 | true    | 19740 |
| HG00103 | true    |  1861 |
| HG00105 | true    | 22278 |
| HG00106 | true    | 26484 |
| HG00107 | true    | 29726 |
+---------+---------+-------+

It's schema is

.. code-block::text

    TStruct(Sample=TString, Status=TBoolean, qPhen = TInt32)

In addition, Hail tables also have global variables. You can think of globals as
extra fields in the table whose values are identical for every row. For example,
the same table above with the global variable ``X = 5`` can be thought of as

+---------+---------+-------+-------+
| Sample  | Status  | qPhen |     X |
+---------+---------+-------+-------+
| String  | Boolean | Int32 | Int32 |
+---------+---------+-------+-------+
| HG00096 | true    | 27704 |     5 |
| HG00097 | true    | 16636 |     5 |
| HG00099 | true    |  7256 |     5 |
| HG00100 | true    | 28574 |     5 |
| HG00101 | true    | 12088 |     5 |
| HG00102 | true    | 19740 |     5 |
| HG00103 | true    |  1861 |     5 |
| HG00105 | true    | 22278 |     5 |
| HG00106 | true    | 26484 |     5 |
| HG00107 | true    | 29726 |     5 |
+---------+---------+-------+-------+

but the value ``5`` is only stored once for the entire dataset and NOT once per
row of the table. The output of `describe` lists what all of the top level row
fields and global fields are.

.. code-block::text

    Global fields:
        'X': Int32

    Row fields:
        'Sample': String
        'Status': String
        'qPhen': Int32


Row fields can be specified to be the keys of the table with the method `key_by`.
Keys are important for joining tables together (discussed below). Important table
attributes are `columns`, `schema`, `global_schema`, `key`, and `num_columns`.

Hail has functions to create tables from a variety of data sources.
The most common use case is to load data from a TSV or CSV file, which can be
done with the `import_table` function.

.. doctest::

    t = functions.import_table("data/kt_example1.tsv", )

A table can also be created from Python
objects with `parallelize`. For example, a table with only the first two rows
above could be created from Python objects.

.. doctest::

    rows = [{"Sample": "HG00096", "Status": True, "qPhen": 27704},
            {"Sample": "HG00097", "Status": True, "qPhen": 16636}]

    schema = TStruct(["Sample", "Status", "qPhen"], [TString(), TBoolean(), TInt32()])

    t = Table.parallelize(rows, schema)

Examples of genetics-specific import methods are
`import_interval_list`, `import_fam`, and `import_bed`. Many Hail methods also
return tables.

The main operations on a table are `select` and `drop` to add or remove row fields,
`filter` to either keep or remove rows based on a condition, and `annotate` to add
new row fields or update the values of existing row fields. For example, extending
the example table above, we can filter the table to only contain rows where
``qPhen < 15000``, add a new row field `SampleInt` which is the integer component of the row
field `Sample`, add a new global field `foo`, and select only the row fields `SampleInt` and
`qPhen` as well as define a new row field `bar` which is the product of `qPhen` and `SampleInt`.

.. doctest::

    t = t.filter(t['qPhen'] < 15000)
    t = t.annotate(SampleInt = t.Sample.replace("HG", "").to_int32())
    t = t.annotate_globals(foo = 131)
    t = t.select(t['SampleInt'], t['qPhen'], bar = t['qPhen'] * t['SampleInt'])

The final output is

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

.. code-block::text

    Global fields:
        'foo': Int32

    Row fields:
        'SampleInt': Int32
        'qPhen': Int32
        'bar': Int32

One operation we might want to do is group by the row field `Status` and then
compute the mean of `qPheno` for each unique value of `Status`. To do this,
we need to first create a :class:`.GroupedTable` using the `group_by` method. This
will expose the method `aggregate` which can be used to compute new row fields
over the aggregated rows.

.. code-block::python

    t = (t.group_by('Status')
          .aggregate(mean_qtPheno = agg.mean(t.qPheno)))



  - grouped vs. ungrouped
  - joins
  - helpful debugging methods
    - show, take, head, count, sample
  - extracting data as python objects
  - exporting to other formats
    - export, write, to_pandas, to_dataframe, to_spark

-----------
MatrixTable
-----------
  - imports
  - schema / rows table / entries table / matrix / cols table
  - keys
  - basic operations
    - select / drop
    - filter
    - annotate
    - explode
  - grouped vs ungrouped
    - group by
    - aggregate
  - joins
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
