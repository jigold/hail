.. _sec-api:

========
Overview
========

- Why use Hail?
  - what problems can Hail solve
  - what problems can't Hail solve

- Hail Types
  - what are they
  - why used

- Struct
  - what is it?
  - fields (name, type)
  - ordered

- Table
  - imports
  - schema (describe)
  - globals
  - keys
  - main operations
    - select / drop
    - filter
    - annotate
    - forall / exists
    - explode
  - grouped vs. ungrouped
  - joins
  - helpful debugging methods
    - show, take, head, count, sample
  - extracting data as python objects
  - exporting to other formats
    - export, write, to_pandas, to_dataframe, to_spark

- MatrixTable
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

- Expressions
  - capture / broadcast
  - basic operations depending on type
  - if else
  - bind
  - can add 5 + ds.AC or ds.AC + 5 => IntExpression
  - boolean
  - propogation of missingness
  - debugging methods
  - How are these different than Hail objects?

- Functions
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

- Python Considerations
  - chaining methods together => not referring to correct dataset in future operations
  - varargs vs. keyword args
  - how to access attributes (square brackets vs. method accessor)
  - how to work with fields with special chars or periods in name **{'a.b': 5}


- Performance Considerations
  - when to use broadcast
  - cache, persist
  - repartition
  - shuffling
  - group / join with null is bad!

- Other tips
  - expanding fields with splat / double splat
  - hadoop_open, etc.
