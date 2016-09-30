<div class="cmdhead"></div>

<div class="description"></div>

<div class="synopsis"></div>

<div class="options"></div>

<div class="cmdsubsection">

The generality of this module allows it to load delimited text files, json, or a mixture of the two.  

**Using the `--locus-expr` argument:**

This argument tells Hail how to get a variant out of your table.  To do this, you are given the [Hail Expression Language](intro.html#HailExpressionLanguage), and each column in the table is exposed.  Since a `Variant` is not a simple string, using this is a little bit more complicated than using `--sample-expr` in `annotatesamples table`.  There are three ways to make a variant out of a table:
  
  1. Use a column of type `Locus`.  These columns look like `CHR:POS`.  You can get a column of type `Locus` by explictly typing it using `--types` (`-t locusCol: Locus`), or by allowing Hail to do type imputation with `--impute`.  The `--locus-expr` argument in this second case looks like `-e <locusCol>`.  
  2. Use a string column that takes the format designated above in an expr locus constructor.  This looks like `-e Locus(<stringCol>)` 
  3. Use the locus constructor that takes chromosome (`String`), position (`Int`), ref (`String`), and alt (`String`).  This looks something like `-e 'Locus(Chromosome, Position)'` assuming that these columns have the correct types.  _Note:_ type imputation will often assign chromosomes to `Int` type, so use `-t 'Chromosome: String'` to fix this.  
    
**Using the `--code` / `--root` arguments:**

Using this module requires one of these two arguments, and they are used to tell Hail how to insert the table into the variant annotation schema.  The `--root` argument is the simpler of these two, and simply packages up all table annotations as a `Struct` and drops it at the given `--root` location.  If your table had columns "Locus", "Consequence", and "Gene", then `--root va.metadata` would create the struct `{Locus, Consequence, Gene}` at `sa.metadata`, which would give you access to the paths `va.metadata.Locus`, `va.metadata.Consequence`, `va.metadata.Gene`.  The `--code` argument expects an annotation expression just like [`annotatevariants expr`](AnnotateVariantsExpr.md), where you have access to `va` (the variant annotations in the VDS), and `table`, a struct with all the columns in the table.  `--root va.anno` is equivalent to `--code 'va.anno = table'`.

**Here are some examples of common uses for the `--code` argument:**

Table with only one annotation column -- don't generate a full struct:
```
-c 'va.annot = table._1'
```

Want to put annotations on the top level under `va`:
```
-c 'va = merge(va, table)'
```

Want to load only specific annotations from the table:
```
-c 'va.annotations = select(table, toKeep1, toKeep2, toKeep3)'
```

The above is equivalent to:
```
-c 'va.annotations.toKeep1 = table.toKeep1, 
    va.annotations.toKeep2 = table.toKeep2,
    va.annotations.toKeep3 = table.toKeep3'
```
</div>

<div class="cmdsubsection">
### Examples

<h4 class="example">Annotating variants from a tsv file indexed by one column containing the locus</h4>
```
$ zcat ~/consequences.tsv.gz
Locus       Consequence     DNAseSensitivity
1:2001020   Synonymous      0.86
1:2014122   Frameshift      0.65
1:2015242   Missense        0.77
1:2061928   Intergenic      0.12
1:2091230   Synonymous      0.66
```

This file contains one field to identify the locus and two data columns: one which encodes a string and one which encodes a double.  Using the `--impute` option, we can avoid having to specify the `Locus` and `Double` types.  The command line should appear as:

```
$ hail [read / import / previous commands] \
    annotatevariants loci \
        file:///user/me/consequences.tsv.gz \
        -r va.teffects \
        -e Locus
        --impute
```

This invocation will annotate variants with the following schema:

```
Variant annotations:
va: Struct {
    <probably lots of other stuff here>
    varianteffects: Struct {
        Locus: Locus
        Consequence: String
        DNAseSensitivity: Double
    }
}
```

<h4 class="example">Annotating variants from a tsv file indexed by two columns with chromosome and position</h4>
```
$ zcat ~/ExAC_Counts.tsv.gz
Chr  Pos         AC
16   29501233    1
16   29561200    15023
16   29582880    10

```

In this case, the locus is indicated by two chromosome / position columns.  The proper command line is below:

```
$ hail [read / import / previous commands] \
    annotatevariants loci \
        file:///user/me/ExAC_Counts.tsv.gz \
        -t "AC: Int, Pos: Int" \
        -c "va.exac_AC = table.AC" \
        -e "Locus(Chr,Pos)"
```

And the schema:

```
Variant annotations:
va: Struct {
    <probably lots of other stuff here>
    exac_AC: Int
}
```
</div>