.. _sec-exprlang:

========================
Hail Expression Language
========================


Many Hail commands provide the ability to perform a broad array of computations based on data structures exposed to the user.

--------------------------
Expressions and Operations
--------------------------

 - Conditionals: ``if (p) a else b`` -- The value of the conditional is the value of ``a`` or ``b`` depending on ``p``.  If ``p`` is missing, the value of the conditional is missing.

 - Let: ``let v1 = e1 and v2 = e2 and ... and vn = en in b`` -- Bind variables ``v1`` through ``vn`` to result of evaluating the ``ei``.  The value of the ``let`` is the value of ``b``.  ``v1`` is visible in ``e2`` through ``en``, etc.

 - Global comparisons: ``a == b``, ``a != b``

 - Boolean comparisons: ``a || b``, ``a && b``  Boolean comparisons short circuit.  If ``a`` is true, ``a || b`` is ``true`` without evaluating ``b``.  If ``a`` is missing, ``b`` is evaluated and the comparison returns ``true`` if ``b`` is true, otherwise missing.

 - Boolean conversion
    - toInt: ``b.toInt`` -- returns ``1`` if ``true``, ``0`` if ``false``

 - Missingness:
     - isMissing: ``isMissing(a)`` -- returns true if ``a`` is missing
     - isDefined: ``isDefined(a)`` -- returns true if ``a`` is defined
     - orElse: ``a.OrElse(x)`` -- return ``a`` if ``a`` is defined, otherwise ``x``.  ``x`` is only evaluated if ``a`` is NA.

 - Numerical comparisons: ``<``, ``<=``, ``>``, ``>=``

 - Numerical conversions:
     - toDouble: ``i.toDouble``
     - toInt: ``i.toInt``
     - toFloat: ``i.toFloat``
     - toLong: ``i.toLong``
     - str: ``str(i)`` -- returns ``i`` as a string

 - Numerical operations:
     - +, -, /, *, %: ``a + b - c / d * e % f``. ``/`` converts its arguments to Double, so ``7 / 2`` equals ``3.5``
     - //: floor division ``floor(x / y)`` as in Python, so ``7 // 2`` is ``3``, ``-7 // 2`` is ``-4``, and ``1.0 // 2.0`` is ``0.0``
     - abs: ``i.abs`` -- returns the absolute value of ``i``
     - signum: ``i.signum`` -- returns the sign of ``i`` (1, 0, or -1)
     - min: ``i.min(j)`` -- returns the minimum of ``i`` and ``j``
     - max: ``i.max(j)`` -- returns the maximum of ``i`` and ``j``
     - log(x[, b]) -- log of ``x`` base ``b``.  If ``b`` is not given, the natural log of ``x``.
     - log10(x) -- log of ``x`` base 10
     - exp(x) -- exponential of ``x``
     - pow(b, e) -- ``b`` to the power ``e``
     - sqrt(x) -- the square root of ``x``

 - String operations:
     - 'regular expression pattern' ~ targetstring: Matches given ``regular expression pattern`` to ``targetstring`` and returns boolean.
     - apply: ``str[index]`` -- returns the character at ``index``
     - length: ``str.length`` -- returns the length of the string
     - concatenate: ``str1 + str2`` -- returns the two strings joined start-to-end
     - split: ``str.split(delimiter)`` -- returns an array of strings, split on the given regular expression ``delimiter``. If you need to    split on special characters, escape them with double backslash (\\\\). See Regular expression syntax: https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html

 - String conversions:
    - toInt: ``str.toInt``
    - toDouble: ``str.toDouble``
    - toLong: ``str.toLong``
    - toFloat: ``str.toFloat``

 - Random Booleans and doubles:
     - pcoin(p) -- returns ``true`` with probability ``p``. ``p`` should be between 0.0 and 1.0
     - runif(min, max) -- returns a random draw from a uniform distribution on \[``min``, ``max``). ``min`` should be less than or equal to ``max``
     - rnorm(mean, sd) -- returns a random draw from a normal distribution with mean ``mean`` and standard deviation ``sd``. ``sd`` should be non-negative
     
 - Statistics
    - pnorm(x) -- Returns left-tail probability p for which p = Prob($Z$ < x) with $Z$ a standard normal random variable
    - qnorm(p) -- Returns left-quantile x for which p = Prob($Z$ < x) with $Z$ a standard normal random variable. ``p`` must satisfy ``0 < p < 1``. Inverse of ``pnorm``
    - pchisq1tail(x) -- Returns right-tail probability p for which p = Prob($Z^2$ > x) with $Z^2$ a chi-squared random variable with one degree of freedom. ``x`` must be positive
    - qchisq1tail(p) -- Returns right-quantile x for which p = Prob($Z^2$ > x) with $Z^2$ a chi-squared RV with one degree of freedom. ``p`` must satisfy ``0 < p <= 1``. Inverse of ``pchisq1tail``

 - Array Operations:
     - constructor: ``[element1, element2, ...]`` -- Create a new array from elements of the same type.
     - indexing: ``arr[index]`` -- get a value from the array, or NA if array or index is missing
     - slicing: ``arr[start:end]`` -- get a slice of the array (as an array).  The ``start`` is inclusive, the ``end`` is not.  For example, ``[0,1,2,3,4][0:2]`` returns ``[0,1]``.  ``[0,1,2,3,4,5][2:5]`` returns ``[2,3,4]``.
     - length / size: ``arr.length`` / ``arr.size`` -- returns the length of the array as an integer
     - isEmpty: ``arr.isEmpty`` -- returns true if the array has length 0
     - mkString: ``arr.mkString(sep)`` -- returns a string generated by joining elements sequentially, delimited by ``sep``
     - toSet: ``arr.toSet`` -- returns a set of the same type (good if you need to call ``.contains``, which is not available for arrays)
     - find: ``arr.find(v => expr)`` -- Returns the first non-missing element of ``arr`` for which ``expr`` is true.  If no element satisfies the predicate, ``find`` returns NA.
     - map: ``arr.map(v => expr)`` -- Returns a new array produced by applying ``expr`` to each element
     - flatMap: ``arr.flatMap(v => expr)`` -- valid only for ``expr`` of type Array. Returns a new array by mapping each element of ``arr`` and concatenating the resulting arrays
     - flatten: ``arr.flatten()`` -- Returns a new array by concatenating the elements of ``arr``, which must be an array of arrays
     - filter: ``arr.filter(v => expr)`` -- Returns a new array subsetted to the elements where ``expr`` evaluated to true
     - exists: ``arr.exists(v => expr)`` -- Returns a boolean which is true if **any** element satisfies ``expr``, false otherwise
     - forall: ``arr.forall(v => expr)`` -- Returns a boolean which is true if the array is empty, or ``expr`` evaluates to ``true`` for **every** element
     - sort: ``arr.sort([ascending])`` -- Returns a new array with the same elements in ascending order according to their value, which must be numeric or string. For descending order, use ``arr.sort(false)``. Missing elements are always placed at the end.
     - sortBy: ``arr.sortBy(v => expr[,ascending])`` -- Returns a new array with the same elements in ascending order according to the value of ``expr``, which must be numeric or string. For descending order, use ``arr.sortBy(v => expr, false)``. Elements with missing ``expr`` values are always placed at the end.

 - Numeric Array Operations:
     - min: ``arr.min`` -- valid only for numeric arrays, returns the minimum value
     - max: ``arr.max`` -- valid only for numeric arrays, returns the minimum value
     - arithmetic: ``+ - * /``
        - Array with scalar will apply the operation to each element of the array.  ``[1, 2, 3] * 2`` = ``[2, 4, 6]``.
        - Array with Array will apply the operation positionally.  ``[1, 2, 3] * [1, 0, -1]`` = ``[1, 0, -3]``.  _Fails if the dimension of the two arrays does not match._

 - Set Operations:
     - contains: ``set.contains(elem)`` -- returns true if the element is contained in the array, otherwise false
     - size: ``set.size`` -- returns the number of elements in the set as an integer
     - isEmpty: ``set.isEmpty`` -- returns true if the set contains 0 elements
     - equals: ``set1 == set2`` -- returns true if both sets contain the same elements
     - min: ``set.min`` -- valid only for numeric sets, returns the minimum value
     - max: ``set.max`` -- valid only for numeric sets, returns the minimum value
     - find: ``set.find(v => expr)`` -- Returns the first non-missing element of ``set`` for which ``expr`` is true.  If no element satisfies the predicate, ``find`` returns NA.
     - map: ``set.map(v => expr)`` -- Returns a new set produced by applying ``expr`` to each element
     - flatMap: ``set.flatMap(v => expr)`` -- valid only for ``expr`` of type Set. Returns a new set by mapping each element of ``set`` and taking the union of the resulting sets
     - flatten: ``set.flatten()`` -- Returns a new set by taking the union of elements of ``set``, which must be a set of sets
     - filter: ``set.filter(v => expr)`` -- Returns a new set subsetted to the elements where ``expr`` evaluated to true
     - exists: ``set.exists(v => expr)`` -- Returns a boolean which is true if **any** element satisfies ``expr``, false otherwise
     - forall: ``set.forall(v => expr)`` -- returns a boolean which is true if the set is empty, or ``expr`` evaluates to ``true`` for **every** element

 - Dict Operations:
     - select: ``dict[key]`` -- returns the value keyed by the string ``key``.  An example might be ``global.genedict["SCN2A"]``.
     - contains: ``dict.contains(key)`` -- returns true if ``dict`` has key ``key``, false otherwise.
     - mapValues: ``dict.mapValues(x => expr)`` -- returns a new dict with a transformation of the values
     - size: ``dict.size`` -- returns the number of key/value pairs
     - isEmpty: ``dict.isEmpty`` -- returns true if there is at least one key/value pairs

 - Struct Operations:
     - constructor: ``{key1: 1, key2: "Hello", key3: 0.99, ...}`` -- Create a new struct from specified field names and values in the format shown.
     - select: ``struct.field`` -- returns the value of the given field of a struct.  For example, ``va.info.AC`` selects the struct ``info`` from the struct ``va``, and then selects the array ``AC`` from the struct ``info``.
     - index: ``index(Array[Struct], fieldname)`` -- returns a dictionary keyed by the string field ``fieldname`` of the given ``struct``, referencing values that are structs with the remaining fields.

            For example, ``global.gene_info`` is the following ``Array[Struct]``:

                 [{PLI: 0.998, genename: "gene1", hits_in_exac: 1},
                 {PLI: 0.0015, genename: "gene2", hits_in_exac: 10},
                 {PLI: 0.9045, genename: "gene3", hits_in_exac: 2}]

            We can index it by gene:

                global.gene_dict = index(global.gene_info, genename)

            Now the following equality is true:

                global.gene_dict["gene1"] == {PLI: 0.998, hits_in_exac: 1}

      - merge: ``merge(struct1, struct2)`` -- create a new struct with all fields in struct1 and struct2
      - select and drop: ``select`` / ``drop`` -- these take the format ``select(struct, identifier1, identifier2, ...)``.  These methods return a subset of the struct.  One could, for example, remove the horrible ``CSQ`` from the info field of a vds with ``annotatevariants expr -c 'va.info = drop(va.info, CSQ)``.  One can select a subset of fields from a table using ``select(va.EIGEN, field1, field2, field3)``

  - Object constructors:

    - Variant: ``Variant(chr, pos, ref, alt)``, where chr, ref, are ``String``, and ``pos`` is Int, and ``alt`` is either a ``String`` or ``Array[String]``
    - Variant: ``Variant(str)``, where str is of the form ``CHR:POS:REF:ALT`` or ``CHR:POS:REF:ALT1,ALT2...ALTN``
    - Locus: ``Locus(chr, pos)``, where chr is a ``String`` and pos is an ``Int``
    - Interval: ``Interval(startLocus, endLocus)``, where startLocus and endLocus are loci
    
  - Apply methods:
    
    - range: ``range(end)`` or ``range(start, end)``.  This function will produce an ``Array[Int]``.  ``range(3)`` produces ``[0, 1, 2]``.  ``range(-2, 2)`` produces ``[-2, -1, 0, 1]``.

    - ``gtj(i)`` and ``gtk(i)``.  Convert from genotype index (triangular numbers) to ``j/k`` pairs.

    - ``gtIndex(j, k)``.  Convert from ``j/k`` pair to genotype index (triangular numbers).

**Note:**

 - All variables and values are case sensitive
 - Missingness propagates up.  If any element in an expression is missing, the expression will evaluate to missing.



## Filtering

Filtering requires an expression that evaluates to a boolean.

``````
filtersamples expr --keep -c '"PT-1234" ~ s.id'
``````


``````
filtersamples expr --keep -c 'sa.qc.callRate > 0.99'
``````

In the below expression, we will use a different cutoff for samples with European and non-European ancestry.  This can be done with an if/else statement.

``````
filtersamples expr --keep -c 'if (sa.ancestry == "EUR") sa.qc.nSingleton < 100 else sa.qc.nSingleton < 200'
``````

The below expression assumes a VDS was split from a VCF, and filters down to sites which were singletons on import.  ``va.aIndex - 1`` (NB: ``va.aIndex`` is the allele index, not the alternate allele index) indexes into the originally-multiallelic array ``va.info.AC`` with the original position of each variant.

``````
filtervariants expr --keep -c 'if (va.info.AC[va.aIndex - 1]) == 1'
``````

See documentation on [exporting to TSV](commands.html#ExportTSV) for more examples of what Hail's language can do.


## <a class="jumptarget" name="statsFunctions"></a> Statistical Functions

### <a class="jumptarget" name="fet"></a> Fisher's Exact Test

Hail's expression language exposes the ``fet`` function to calculate the p-value, odds ratio, and 95% confidence interval with Fisher's exact test for 2x2 tables. This implementation of FET is identical to the version implemented in [R](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/fisher.test.html) with default parameters (two-sided, alpha = 0.05, null hypothesis that the odds ratio equals 1).

The ``fet`` function takes four non-negative arguments of type Int.
``````
annotatevariants expr -c 'va.fet = fet(a, b, c, d)'
``````

The function adds four annotations of type Double to the annotation root specified on the left-hand side of the equation:
 - ``pValue``
 - ``oddsRatio``
 - ``ci95Lower``
 - ``ci95Upper``

Note that the aggregator function ``count()`` creates annotation of type Long, which must be converted to Int as in the workflow below. Caution: the maximum value of an Int is 2147483647. Converting a Long of larger value to Int will corrupt the value.

**Example Workflow to Perform a Single-Variant Association Test Using FET:**
``````
annotatesamples table -i /path/my/annotations.tsv -r "sa.pheno"
annotatevariants expr -c 'va.minorCase = gs.filter(g => sa.pheno.Pheno1 == "Case" && g.isHet).count() + 2 * gs.filter(g => sa.pheno.Pheno1 == "Case" && g.isHomVar).count()'
annotatevariants expr -c 'va.majorCase = gs.filter(g => sa.pheno.Pheno1 == "Case" && g.isHet).count() + 2 * gs.filter(g => sa.pheno.Pheno1 == "Case" && g.isHomRef).count()'
annotatevariants expr -c 'va.minorControl = gs.filter(g => sa.pheno.Pheno1 == "Control" && g.isHet).count() + 2 * gs.filter(g => sa.pheno.Pheno1 == "Control" && g.isHomVar).count()'
annotatevariants expr -c 'va.majorControl = gs.filter(g => sa.pheno.Pheno1 == "Control" && g.isHet).count() + 2 * gs.filter(g => sa.pheno.Pheno1 == "Control" && g.isHomRef).count()'
annotatevariants expr -c 'va.fet = fet(va.minorCase.toInt, va.majorCase.toInt, va.minorControl.toInt, va.majorControl.toInt)'
filtervariants expr --keep -c 'va.fet.pValue < 1e-4'
exportvariants -o /path/my/results.tsv -c 'v, va.minorCase, va.majorCase, va.minorControl, va.majorControl, va.fet.pValue, va.fet.oddsRatio, va.fet.ci95Lower, va.fet.ci95Upper'
``````


### <a class="jumptarget" name="infoscore_doc"></a> IMPUTE Info Score (Dosage Data)

The ``infoScore`` aggregator can be used to calculate the IMPUTE info score from a [genotype aggregable](#aggreg_infoscore). 

**Example:**

``````
annotatevariants expr -c 'va.infoScore = gs.infoScore()'
``````

We implemented the IMPUTE info measure as described in the [supplementary information from Marchini & Howie. Genotype imputation for genome-wide association studies. Nature Reviews Genetics (2010)](http://www.nature.com/nrg/journal/v11/n7/extref/nrg2796-s3.pdf).

To calculate the info score $I_{A}$ for one SNP:

$$
I_{A} = 
\begin{cases}
1 - \frac{\sum_{i=1}^{N}(f_{i} - e_{i}^2)}{2N\hat{\theta}(1 - \hat{\theta})} & \text{when } \hat{\theta} \in (0, 1) \\
1 & \text{when } \hat{\theta} = 0, \hat{\theta} = 1\\
\end{cases}
$$

 - $N$ is the number of samples with imputed genotype probabilities [$p_{ik} = P(G_{i} = k)$ where $k \in \{0, 1, 2\}$]
 - $e_{i} = p_{i1} + 2p_{i2}$ is the expected genotype per sample
 - $f_{i} = p_{i1} + 4p_{i2}$
 - $\hat{\theta} = \frac{\sum_{i=1}^{N}e_{i}}{2N}$ is the MLE for the population minor allele frequency

Hail will not generate identical results as [QCTOOL](http://www.well.ox.ac.uk/~gav/qctool/#overview) for the following reasons:
 
 - The floating point number Hail stores for each dosage is slightly different than the original data due to rounding and normalization of probabilities.
 - Hail automatically removes dosages that [do not meet certain requirements](commands.html#dosagefilters) on data import with [``importgen``](commands.html#importgen) and [``importbgen``](commands.html#importbgen).
 - Hail does not use the population frequency to impute dosages when a dosage has been set to missing.
 - **Hail calculates the same statistic for sex chromosomes as autosomes while QCTOOL incorporates sex information**

**Warning!!! The info score Hail reports will be extremely different from qctool when a SNP has a high missing rate.**

### <a class="jumptarget" name="ibc_doc"></a> Inbreeding Coefficient

The ``inbreeding`` aggregator can be used to calculate the Inbreeding Coefficient from a [genotype aggregable](#aggreg_ibc).
This is equivalent to the [``--het`` method in PLINK](https://www.cog-genomics.org/plink2/basic_stats#ibc).

The Inbreeding Coefficient (F) is computed as follows:

2. For each variant and sample with a non-missing genotype call, ``E``, the expected number of homozygotes (computed from user-defined expression for minor allele frequency), is computed as ``1.0 - (2.0*maf*(1.0-maf))``
3. For each variant and sample with a non-missing genotype call, ``O``, the observed number of homozygotes, is computed as ``0 = heterozygote; 1 = homozygote``
4. For each variant and sample with a non-missing genotype call, ``N`` is incremented by 1
5. For each sample, ``E``, ``O``, and ``N`` are combined across variants
6. ``F`` is calculated by ``(O - E) / (N - E)``
