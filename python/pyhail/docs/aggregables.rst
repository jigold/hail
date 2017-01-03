-----------
Aggregables
-----------

Hail's expression language exposes a number of 'aggregables', special objects which allow users to specify computations across entire rows or columns of a dataset.  Aggregables allow a user to replicate nearly all the of the statistics generated in [``sampleqc``](commands.html#sampleqc) or [``variantqc``](commands.html#variantqc), as well as compute an unrestricted set of new metrics.

**Additional namespace of ``gs``:**

Identifier | Description
:-: | ---
``v`` | Variant
``va`` | Variant annotations
``s`` | Sample
``sa`` | Sample annotations
``global`` | Global annotations

**Additional namespace of ``samples`` in ``annotateglobal``:**

Identifier | Description
:-: | ---
``sa`` | Sample annotations
``global`` | Global annotations

**Additional namespace of ``variants`` in ``annotateglobal``:**

Identifier | Description
:-: | ---
``va`` | Variant annotations
``global`` | Global annotations

### Map and Filter

``````
<aggregable>.map( <Any lambda expression> )
<aggregable>.filter( <Boolean lambda expression> )
``````

These two generic helper functions allow the proceeding calculations to be totally general and modular.

``map`` changes the type of an aggregable: ``gs.map(g => g.gq)`` takes the ``Aggregable[Genotype]`` "gs" and returns an ``Aggregable[Int]``.

``filter`` subsets an aggregable by excluding/including elements based on a lambda expression.  Note: does not change the type of an aggregable.  ``gs.filter(g => g.isHet)`` produces an aggregable where only heterozygous genotypes are considered.

### <a class="jumptarget" name="#aggregables_count"></a> Count

``````
<aggregable>.count()
``````

``count()`` counts the number of included elements in this aggregable.

The result of ``count`` is a ``Long`` (integer).

**Examples:**

One can replicate ``qc.nHet`` for either samples or variants by counting:
``````
annotatesamples expr -c 'sa.nHet = gs.filter(g => g.isHet).count()'
annotatevariants expr -c 'va.nHet = gs.filter(g => g.isHet).count()'
``````

One can also compute more complicated counts.  Here we compute the number of non-ref cases and controls per variant (Assuming that ``sa.pheno.isCase`` is a boolean sample annotation)
``````
annotatevariants expr -c 'va.caseCount = gs.filter(g => sa.pheno.isCase && g.isCalledNonRef).count(), va.controlCount = gs.filter(g => !(sa.pheno.isCase) && g.isCalledNonRef).count()'
``````

Here we count the number of singleton non-ref LOFs and the number of homozygous alternate LOFs per sample, assuming that one has previously annotated variant consequence into ``va.consequence``:
``````
annotatevariants expr -c 'va.isSingleton = gs.filter(g => g.isCalledNonRef).count() == 1' \
annotatesamples expr -c 'sa.singletonLOFs = gs.filter(g => va.isSingleton && g.isCalledNonRef && va.consequence == "LOF").count(),
                    sa.homVarLOFs = gs.filter(g => g.isHomVar && va.consequence == "LOF").count()
``````

This can also be used to calculate statistics from sample/variant annotations in ``annotateglobal``:
``````
annotatevariants expr -c 'va.isSingleton = gs.filter(g => g.isCalledNonRef).count() == 1'
annotatesamples expr -c 'sa.callrate = gs.fraction(g => g.isCalled)'
annotateglobal expr -c 'global.lowQualSamples = samples.filter(s => sa.callrate < 0.95).count(),
              global.totalNSingleton = variants.filter(v => va.isSingleton).count()'
``````

### Fraction

``````
<aggregable>.fraction( <Boolean lambda expression> )
``````

``fraction`` computes the ratio of the number of occurrences for which a boolean condition evaluates to ``true``, divided the number of included elements in the aggregable.

The result of ``fraction`` is a ``Double`` (floating-point)

**Examples:**

One can replicate call rate, or calculate missingness:
``````
filtervariants expr --keep -c 'gs.fraction(g => g.isCalled) > 0.90'
filtersamples expr --keep -c 'gs.fraction(g => g.isCalled) > 0.95'
``````

One can also extend this thinking to compute the differential missingness at SNPs and indels:
``````
annotatesamples expr -c 'sa.SNPmissingness = gs.filter(g => v.altAllele.isSNP).fraction(g => g.isNotCalled),
                    sa.indelmissingness = gs.filter(g => v.altAllele.isIndel).fraction(g => g.isNotCalled)
``````

### Sum

``sum()`` computes the sum of an ``Aggregable[Numeric]``, or the position-wise sum of an ``Aggregable[Array[Numeric]]``.  The result is a ``Double`` or ``Long`` in the first case, or ``Array[Double]`` or ``Array[Long]`` if the aggregable contains arrays.

**Examples:**

This aggregator function can be used to compute counts of each allele per variant.  The result will be an "R"-numbered array (one element per allele, including the reference):

``````
annotatevariants expr -c 'va.AC = gs.map(g => g.oneHotAlleles(v)).sum()
``````

Count the number of total LOF heterozygous calls in the dataset:

``````
annotatevariants expr -c 'va.hets = gs.filter(g => g.isHet).count()' \
annotateglobal expr -c 'global.total_LOFs =
    variants.filter(v => va.isLOF).map(v => va.hets).sum()'
``````


### Stats

``````
<numeric aggregable>.stats()
``````

``stats()`` computes six useful statistics about a numeric aggregable.

The result of ``stats`` is a struct:
``````
Struct {
   mean: Double,
   stdev: Double,
   min: Double,
   max: Double,
   nNotMissing: Long,
   sum: Double
}
``````

**Examples:**

One can replicate the calculations in ``<va / sa>.qc.gqMean`` and ``<va / sa>.qc.gqStDev`` with the command below.  After this command, ``va.gqstats.mean`` is equal to the result of running ``variantqc`` and querying ``va.qc.gqMean``, and this equivalence holds for the other values.
``````
annotatevariants expr -c 'va.gqstats = gs.map(g => g.gq).stats()'
annotatesamples expr -c 'sa.gqstats = g.map(g => g.gq).stats()'
``````

One can use ``stats`` to compute statistics on annotations as well:
``````
sampleqc
annotateglobal expr -c 'global.singletonStats = samples.map(s => sa.qc.nSingleton).stats()'
``````

Compute gq/dp statistics stratified by genotype call:
``````
annotatevariants expr -c '
    va.homrefGQ = gs.filter(g => g.isHomRef).map(g => g.gq).stats(),
    va.hetGQ = gs.filter(g => g.isHet).map(g => g.gq).stats(),
    va.homvarGQ = gs.filter(g => g.isHomVar).map(g => g.gq).stats(),
    va.homrefDP = gs.filter(g => g.isHomRef).map(g => g.dp).stats(),
    va.hetDP = gs.filter(g => g.isHet).map(g => g.dp).stats(),
    va.homvarDP = gs.filter(g => g.isHomVar).map(g => g.dp).stats()'
``````

Compute statistics on number of singletons stratified by case/control:
``````
 sampleqc
 annotateglobal expr -c 'global.caseSingletons = samples.filter(s => sa.fam.isCase).map(s => sa.qc.nSingleton).stats(),
     global.controlSingletons = samples.filter(s => !sa.fam.isCase).map(s => sa.qc.nSingleton).stats()'
``````

### Counter

``````
<aggregable>.counter()
``````

This aggregator counts the number of occurrences of each element of an aggregable.  It produces an array of structs with the following schema:

``````
Array [
  Struct {
    key: T, // element type of aggregator
    count: Long
  }
]
``````

The resulting array is sorted by count in descending order (the most common element is first).

**Example:** compute the number of indels in each chromosome:

``````
    annotateglobal expr -c
      'global.chr_indels = variants
        .filter(v => v.altAllele.isIndel)
        .map(v => v.contig)
        .counter()'
``````

### Hist

``````
<numeric aggregable>.hist( start, end, bins )
``````

This aggregator is used to compute frequency distributions of numeric parameters.  The start, end, and bins params are no-scope parameters, which means that while computations like ``100 / 4`` are acceptable, variable references like ``global.nBins`` are not.

The result of a ``hist`` invocation is a struct:

``````
Struct {
    binEdges: Array[Double],
    binFrequencies: Array[Long],
    nLess: Long,
    nGreater: Long
}
``````

Important properties:

 - Bin size is calculated from ``(end - start) / bins``
 - (bins + 1) breakpoints are generated from the range ``(start to end by binsize)``
 - ``binEdges`` stores an array of bin cutoffs.  Each bin is left-inclusive, right-exclusive except the last bin, which includes the maximum value.  This means that if there are N total bins, there will be N + 1 elements in binEdges.  For the invocation ``hist(0, 3, 3)``, ``binEdges`` would be ``[0, 1, 2, 3]`` where the bins are ``[0, 1)``, ``[1, 2)``, ``[2, 3]``.
 - ``binFrequencies`` stores the number of elements in the aggregable that fall in each bin.  It contains one element for each bin.
 - Elements greater than the max bin or less than the min bin will be tracked separately by ``nLess`` and ``nGreater``

**Examples:**

Compute GQ-distributions per variant:

``````
annotatevariants expr -c 'va.gqHist = gs.map(g => g.gq).hist(0, 100, 20)'
``````

Or, extend the above to compute a global gq histogram:

``````
annotatevariants expr -c 'va.gqHist = gs.map(g => g.gq).hist(0, 100, 20)'
annotateglobal expr -c 'global.gqHist = variants.map(v => va.gqHist.binFrequencies).sum()'
``````

### Collect

``````
<aggregable>.collect()
``````

``collect()`` is an aggregator that allows a set of elements of an aggregator to be collected into an ``Array``.  For example, one can collect the list of non-ref sample IDs per variant with the following:

``````
annotatevariants expr \
  -c 'va.hetSamples = gs.filter(g => g.isCalledNonRef)
                        .map(g => s.id)
                        .collect()'
``````

The above example is updating the value of the ``va.hetSamples`` annotation. The value is calculated by transforming the array of genotypes, ``gs``, in three steps.

  1. apply ``filter(g => g.isCalledNonRef)`` to keep only those genotypes which are non-reference
  2. apply ``map(g => s.id)`` to convert the each kept genotype to its corresponding sample id
  3. apply ``collect()`` to remove ``NA``s and produce an ``Array`` of sample ids (which are ``String``s)

### Call Stats

``````
<aggregable>.callStats( <Variant lambda expression> )
``````

``callStats`` is an aggregator which operates on an ``Aggregable[Genotype]`` that computes four commonly-used metrics over a set of genotypes in a variant.  The resulting annotation is a struct:

``````
Struct {
    AC: Array[Int],
    AF: Array[Double],
    AN: Int,
    GC: Array[Int]
}
``````

In the above schema, the types mean the following:

 * ``AC``: Allele count.  One element per allele **including reference**.  There are two elements for a biallelic variant, or 4 for a variant with three alternate alleles.
 * ``AF``: Allele frequency.  One element per allele **including reference**.  Sums to 1.
 * ``AN``: Allele number.  This is equal to the sum of AC, or 2 * the total number of called genotypes in the aggregable.
 * ``GC``: Genotype count.  One element per possible genotype, including reference genotypes -- 3 for biallelic, 6 for triallelic, 10 for 3 alt alleles, and so on.  The sum of this array is the number of called genotypes in the aggregable.

**Example:** compute population-specific call statistics.  After the below command, ``va.eur_stats.AC`` will be the AC computed from individuals marked as "EUR".

``````
annotatevariants expr -c "va.eur_stats = gs.filter(g => sa.pop == "EUR").callStats(g => v),
                          va.afr_stats = gs.filter(g => sa.pop == "AFR").callStats(g => v),
                          va.eas_stats = gs.filter(g => sa.pop == "EAS").callStats(g => v)"
``````

### <a class="jumptarget" name="aggreg_hwe"></a> HardyWeinberg

``````
<genotype aggregable>.hardyWeinberg()
``````

``hardyWeinberg()`` is an aggregator that computes a p-value computed from the [Hardy Weinberg Equilibrium (HWE) null model](LeveneHaldane.tex) on an aggregable of genotypes (gs).

The result of ``hardyWeinberg()`` is a struct:

``````
Struct {
    rExpectedHetFrequency: Double,
    pHWE: Double
}
``````

In the above schema, ``rExpectedHetFrequency`` is the expected rHeterozygosity based on HWE and ``pHWE`` is the p-value.

**Examples:**

Add a new variant annotation that calculates HWE p-value by phenotype

``````
annotatevariants expr -c 'va.hweCase = gs.filter(g => sa.pheno == "Case").hardyWeinberg(),
                          va.hweControl = gs.filter(g => sa.pheno == "Control").hardyWeinberg()'
``````


### <a class="jumptarget" name="aggreg_infoscore"></a> InfoScore

``````
<genotype aggregable>.infoScore()
``````

``infoScore()`` is an aggregator that computes an [IMPUTE info score](#infoscore_doc) on an aggregable of genotypes (gs).

The result of ``infoScore()`` is a struct:

``````
Struct {
    score: Double,
    nIncluded: Int
}
``````

In the above schema, ``score`` is the IMPUTE info score produced, and ``nIncluded`` is the number of samples with non-missing dosages.

**Note:**
If the genotype data was not imported using the [``importbgen``](commands.html#importbgen) or [``importgen``](commands.html#importgen) commands, then the results for all variants will be ``score = NA`` and ``nIncluded = 0``.

**Note:**
It only makes sense to compute info score for an ``Aggregable[Genotype]`` per variant.  While a per-sample info score will run complete, the result is meaningless.

**Examples:**

Calculate the info score per variant and export the resulting annotations to a TSV file:

``````
hail importgen -s /my/path/example.sample /my/path/example.gen
    annotatevariants expr -c 'va.infoScore = gs.infoScore()'
    exportvariants -c 'v, va.infoScore.score, va.infoScore.nIncluded'
                   -o infoScores.tsv
``````

Calculate group-specific info scores per variant:

``````
hail importgen -s /my/path/example.sample /my/path/example.gen
    annotatesamples table -i phenotypes.tsv -r "sa.pheno"
    annotatevariants expr -c 'va.infoScoreCase = gs.filter(g => sa.pheno.Pheno1 == "Case").infoScore()'
    annotatevariants expr -c 'va.infoScoreControl = gs.filter(g => sa.pheno.Pheno1 == "Control").infoScore()'
``````

### <a class="jumptarget" name="aggreg_ibc"></a> Inbreeding

``````
<genotype aggregable>.inbreeding( <Double lambda expression> )
``````

``inbreeding`` is an aggregator that computes [inbreeding metrics](#ibc_doc) on an ``Aggregable[Genotype]``.  It takes a lambda expression from ``Genotype`` to ``Double`` expression for the alt allele frequency as a required parameter.

The result of ``inbreeding`` is a struct:

``````
Struct {
    fStat: Double,
    nTotal: Int,
    nCalled: Int,
    expectedHoms: Double,
    observedHoms: Int
}
``````

In the above schema, ``fStat`` is the inbreeding coefficient produced, ``nTotal`` is the number of genotypes analyzed, ``nCalled`` is the number of genotypes with non-missing calls, ``expectedHoms`` is the expected number of homozygote calls, and ``observedHoms`` is the total number of homozygote calls observed.

**Note:** in the case of multiallelics, the allele frequency passed to this function should be the sum of all alternate allele frequencies.

**Examples:**

Calculate the inbreeding metrics per sample and export the resulting annotations to a TSV file:

``````
hail read ...
    variantqc
    annotatesamples expr -c 'sa.inbreeding = gs.inbreeding(g => va.qc.AF)'
    exportsamples -c 'Sample = s, sa.inbreeding.*' -o ib_stats.tsv
``````

Calculate the inbreeding metrics per variant and export these metrics to a TSV file:

``````
hail read ...
    variantqc
    annotatevariants expr -c 'va.inbreeding = gs.inbreeding(g => va.qc.AF)'
    exportvariants -c 'Variant = v, va.inbreeding.*' -o ib_stats_variants.tsv

``````

To obtain the same answer as [PLINK](https://www.cog-genomics.org/plink2), use the following series of commands:

``````
read ...
variantqc
filtervariants expr --keep -c 'va.qc.AC > 1 && va.qc.AF >= 1e-8 &&
    va.qc.nCalled * 2 - va.qc.AC > 1 && va.qc.AF <= 1 - 1e-8 &&
    v.isAutosomal'
annotatesamples expr -c 'sa.inbreeding = gs.inbreeding(g => va.qc.AF)'
``````
