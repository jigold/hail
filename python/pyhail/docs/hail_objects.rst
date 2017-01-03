.. _sec-objects:

============
Hail Objects
============

.. _variant:

-------
Variant
-------

**Variable Name:** ``v``

- **v.contig** (*String*) -- String representation of contig, exactly as imported.  *NB: Hail stores contigs as strings.  Use double-quotes when checking contig equality*
- **v.start** (*Int*) -- SNP position or start of an indel
- **v.ref** (*String*) -- Reference allele sequence
- **v.isBiallelic** (*Boolean*) -- True if `v` has one alternate allele
- **v.nAlleles** (*Int*) -- Number of alleles
- **v.nAltAlleles** (*Int*) -- Number of alternate alleles, equal to ``nAlleles - 1``
- **v.nGenotypes** (*Int*) -- Number of genotypes
- **v.altAlleles** (*Array[AltAllele]*) -- The :ref:`alternate alleles <altallele>`
- **v.inXPar** (*Boolean*) -- True if chromosome is X and start is in pseudo-autosomal region of X
- **v.inYPar** (*Boolean*) -- True if chromosome is Y and start is in pseudo-autosomal region of Y. *NB: most callers assign variants in PAR to X*
- **v.inXNonPar** (*Boolean*) -- True if chromosome is X and start is not in pseudo-autosomal region of X
- **v.inYNonPar** (*Boolean*) -- True if chromosome is Y and start is not in pseudo-autosomal region of Y
- **v.altAllele** (*AltAllele*) -- The :ref:`alternate allele <altallele>`.  **Assumes biallelic.**
- **v.alt** (*String*) -- Alternate allele sequence.  **Assumes biallelic.**
- **v.locus** (*Locus*) -- Chromosomal locus (chr, pos) of this variant
- **v.isAutosomal** (*Boolean*) -- True if chromosome is not X, not Y, and not MT

.. _altallele:

---------
AltAllele
---------

**Variable Name:** ``v.altAlleles[idx]`` or ``v.altAllele`` (biallelic)

- **<altAllele>.ref** (*String*) -- Reference allele sequence
- **<altAllele>.alt** (*String*)  -- alternate allele sequence
- **<altAllele>.isSNP** (*Boolean*) -- true if both ``v.ref`` and ``v.alt`` are single bases
- **<altAllele>.isMNP** (*Boolean*) -- true if ``v.ref`` and ``v.alt`` are the same (>1) length
- **<altAllele>.isIndel** (*Boolean*) -- true if ``v.ref`` and ``v.alt`` are not the same length
- **<altAllele>.isInsertion** (*Boolean*) -- true if ``v.ref`` is shorter than ``v.alt``
- **<altAllele>.isDeletion** (*Boolean*) -- true if ``v.ref`` is longer than ``v.alt``
- **<altAllele>.isComplex** (*Boolean*) -- true if ``v`` is not an indel, but ``v.ref`` and ``v.alt`` length do not match
- **<altAllele>.isTransition** (*Boolean*) -- true if the polymorphism is a purine-purine or pyrimidine-pyrimidine switch
- **<altAllele>.isTransversion** (*Boolean*) -- true if the polymorphism is a purine-pyrimidine flip

.. _locus:

-----
Locus
-----

**Variable Name:** ``v.locus`` or ``Locus(chr, pos)``

- **<locus>.contig** (*String*) -- String representation of contig
- **<locus>.position** (*Int*) -- Chromosomal position

.. _interval:

--------
Interval
--------

**Variable Name:** ``Interval(locus1, locus2)``

- **<interval>.start** (*Locus*) -- :ref:`locus` at the start of the interval (inclusive)
- **<interval>.end** (*Locus*) -- :ref:`locus` at the end of the interval (exclusive)

.. _sample:

------
Sample
------

**Variable Name:** ``s``

- **s.id** (*String*) -- The ID of this sample, as read at import-time

.. _genotype:

--------
Genotype
--------

**Variable Name:** ``g``

- **g.gt** (*Int*) -- the call, ``gt = k\*(k+1)/2+j`` for call ``j/k``
- **g.ad** (*Array[Int]*) -- allelic depth for each allele
- **g.dp** (*Int*) -- the total number of informative reads
- **g.od** (*Int*) -- **od = dp - ad.sum``
- **g.gq** (*Int*) -- the difference between the two smallest PL entries
- **g.pl** (*Array[Int]*) -- phred-scaled normalized genotype likelihood values
- **g.dosage** (*Array[Double]*) -- the linear-scaled probabilities
- **g.isHomRef** (*Boolean*) -- true if this call is ``0/0``
- **g.isHet** (*Boolean*) -- true if this call is heterozygous
- **g.isHetRef** (*Boolean*) -- true if this call is ``0/k`` with ``k>0``
- **g.isHetNonRef** (*Boolean*) -- true if this call is ``j/k`` with ``j>0``
- **g.isHomVar** (*Boolean*) -- true if this call is ``j/j`` with ``j>0``
- **g.isCalledNonRef** (*Boolean*) -- true if either ``g.isHet`` or ``g.isHomVar`` is true
- **g.isCalled** (*Boolean*) -- true if the genotype is not ``./.``
- **g.isNotCalled** (*Boolean*) -- true if the genotype is ``./.``
- **g.nNonRefAlleles** (*Int*) -- the number of called alternate alleles
- **g.pAB** (*Double*)  -- p-value for pulling the given allelic depth from a binomial distribution with mean 0.5.  Missing if the call is not heterozygous.
- **g.fractionReadsRef** (*Double*) -- the ratio of ref reads to the sum of all *informative* reads
- **g.fakeRef** (*Boolean*) -- true if this genotype was downcoded in :py:meth:`~pyhail.VariantDataset.split_multi`.  This can happen if a ``1/2`` call is split to ``0/1``, ``0/1``
- **g.isDosage** (*Boolean*) -- true if the data was imported from :py:meth:`~pyhail.HailContext.import_gen` or :py:meth:`~pyhail.HailContext.import_bgen`
- **g.oneHotAlleles(Variant)** (*Array[Int]*) -- Produces an array of called counts for each allele in the variant (including reference).  For example, calling this function with a biallelic variant on hom-ref, het, and hom-var genotypes will produce ``[2, 0]``, ``[1, 1]``, and ``[0, 2]`` respectively.
- **g.oneHotGenotype(Variant)** (*Array[Int]*) -- Produces an array with one element for each possible genotype in the variant, where the called genotype is 1 and all else 0.  For example, calling this function with a biallelic variant on hom-ref, het, and hom-var genotypes will produce ``[1, 0, 0]``, ``[0, 1, 0]``, and ``[0, 0, 1]`` respectively.
- **g.gtj** (*Int*) -- the index of allele ``j`` for call ``j/k`` (0 = ref, 1 = first alt allele, etc.)
- **g.gtk** (*Int*) -- the index of allele ``k`` for call ``j/k`` (0 = ref, 1 = first alt allele, etc.)


The conversion between ``g.pl`` (Phred-scaled likelihoods) and ``g.dosage`` (linear-scaled probabilities) assumes a uniform prior.