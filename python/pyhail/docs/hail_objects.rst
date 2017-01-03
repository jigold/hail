.. _sec-api:

============
Hail Objects
============

-------
Variant
-------

- ``v.contig`` (*String*) -- String representation of contig, exactly as imported.  *NB: Hail stores contigs as strings.  Use double-quotes when checking contig equality*
- ``v.start`` (*Int*) -- SNP position or start of an indel
- ``v.ref`` (*String*) -- Reference allele sequence
- ``v.isBiallelic`` (*Boolean*) -- True if `v` has one alternate allele
- ``v.nAlleles`` (*Int*) -- Number of alleles
- ``v.nAltAlleles`` (*Int*) -- Number of alternate alleles, equal to ``nAlleles - 1``
- ``v.nGenotypes`` (*Int*) -- Number of genotypes
- ``v.altAlleles`` (*Array[AltAllele]*) -- The alternate alleles
- ``v.inXPar`` (*Boolean*) -- True if chromosome is X and start is in pseudo-autosomal region of X
- ``v.inYPar`` (*Boolean*) -- True if chromosome is Y and start is in pseudo-autosomal region of Y. *NB: most callers assign variants in PAR to X*
- ``v.inXNonPar`` (*Boolean*) -- True if chromosome is X and start is not in pseudo-autosomal region of X
- ``v.inYNonPar`` (*Boolean*) -- True if chromosome is Y and start is not in pseudo-autosomal region of Y
- ``v.altAllele`` (*AltAllele*) -- The alternate allele (schema below).  **Assumes biallelic.**
- ``v.alt`` (*String*) -- Alternate allele sequence.  **Assumes biallelic.**
- ``v.locus`` (*Locus*) -- Chromosomal locus (chr, pos) of this variant
- ``v.isAutosomal`` (*Boolean*) -- True if chromosome is not X, not Y, and not MT

---------
AltAllele
---------

-----
Locus
-----

--------
Interval
--------

------
Sample
------

--------
Genotype
--------
