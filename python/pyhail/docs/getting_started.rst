.. _sec-getting_started:

===============
Getting Started
===============

**Add spark installation**

All you'll need is the `Java 8 JDK <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_ and the Hail source code. To clone the `Hail repository <https://github.com/broadinstitute/hail>`_ using `Git <https://git-scm.com/>`_, run

    ::

    $ git clone https://github.com/broadinstitute/hail.git
    $ cd hail

The following commands are relative to the ``hail`` directory.

You can also download the source code directly from `Github <https://github.com/broadinstitute/hail/archive/master.zip>`_.

-------------------------
Building and running Hail
-------------------------

Hail may be built to run locally or on a Spark cluster. Running locally is useful for getting started, analyzing or experimenting with small datasets, and Hail development.


Running locally
===============

The single command::

    $ ./gradlew shadowJar

creates a Hail JAR file at ``build/libs/hail-all-spark.jar``. The initial build takes time as `Gradle <https://gradle.org/>`_ installs all Hail dependencies.

The executable is ``build/install/hail/bin/hail`` (to run using ``hail`` add ``build/install/hail/bin`` to your path).

**Make an alias for hail**

``PYTHONPATH=/Users/jigold/spark-2.0.2-bin-hadoop2.7/python:/Users/jigold/spark-2.0.2-bin-hadoop2.7/python/lib/py4j-0.10.3-src.zip:/Users/jigold/hail/python SPARK_HOME=/Users/jigold/spark-2.0.2-bin-hadoop2.7 SPARK_CLASSPATH=/Users/jigold/hail/build/libs/hail-all-spark.jar python``



Here are a few simple things to try in order. To list all commands, run::

    >>> import pyhail
    >>> hc = pyhail.HailContext()

To `~pyhail.HailContext.import_vcf` the included *sample.vcf* into Hail's *.vds* format, run::

    >>> hc.import_vcf('src/test/resources/sample.vcf').write('~/sample.vds')

To `split <https://hail.is/commands.html#splitmulti>`_ multi-allelic variants, compute a panel of `sample <https://hail.is/commands.html#sampleqc>`_ and `variant <https://hail.is/commands.html#variantqc>`_ quality control statistics, write these statistics to files, and save an annotated version of the vds, run::

    >>> vds = (hc.read('~/sample.vds`)
    >>>     .split_multi()
    >>>     .sample_qc()
    >>>     .variant_qc()
    >>>     .export_variants('~/variantqc.tsv', 'Variant = v, va.qc.*')
    >>>     .write('~/sample.qc.vds'))

To count the number of samples, variants, and genotypes, run::

    >>> vds.count(genotypes=True)

Now let's get a feel for Hail's powerful `object methods <https://hail.is/reference.html#HailObjectProperties>`_, `annotation system <https://hail.is/reference.html#Annotations>`_, and `expression language <https://hail.is/reference.html#HailExpressionLanguage>`_. To `print <https://hail.is/commands.html#printschema>`_ the current annotation schema and use these annotations to `filter <https://hail.is/reference.html#Filtering>`_ variants, samples, and genotypes, run::

    >>> (vds.print_schema('~/schema.txt')
    >>>     .filter_variants_expr('v.altAllele.isSNP && va.qc.gqMean >= 20')
    >>>     .filter_samples_expr('sa.qc.callRate >= 0.97 && sa.qc.dpMean >= 15')
    >>>     .filter_genotypes('let ab = g.ad[1] / g.ad.sum in '
    >>>                       '((g.isHomRef && ab <= 0.1) || '
    >>>                       ' (g.isHet && ab >= 0.25 && ab <= 0.75) || '
    >>>                       ' (g.isHomVar && ab >= 0.9))')
    >>>     .write('~/sample.filtered.vds')

Try running :py:meth:`~pyhail.VariantDataset.count` on ``sample.filtered.vds`` to see how the numbers have changed. For further background and examples, continue to the [overview](https://hail.is/overview.html), check out the [general reference](https://hail.is/reference.html) and [command reference](https://hail.is/commands.html), and try the [tutorial](https://hail.is/tutorial.html).

Note that during each run Hail writes a ``hail.log`` file in the current directory; this is useful to developers for debugging.

Running on a Spark cluster and in the cloud
===========================================

In order to run Hail on a Spark cluster, we must first create a Hail JAR. A Hail JAR is specialized to a version of Spark. The Hail Team currently builds against and supports Spark versions `1.5` and `1.6`. The following builds a Hail JAR for use on a cluster with Spark version `1.6.2`::

    patch -p0 < spark1.patch
    ./gradlew -Dspark.version=1.6.2 shadowJar


Note that this modifies the local repository so that it compiles for Spark ``1.x``. If you later want to build for Spark ``2.x``, you must remove this patch, for example, by ``git reset --hard master``.

The resulting JAR ``build/libs/hail-all-spark.jar`` can be submitted using ``spark-submit``. See the `Spark documentation <http://spark.apache.org/docs/1.6.2/cluster-overview.html>`_ for details.

`Google <https://cloud.google.com/dataproc/>`_ and `Amazon <https://aws.amazon.com/emr/details/spark/>`_ offer optimized Spark performance and exceptional scalability to tens of thousands of cores without the overhead of installing and managing an on-prem cluster.
To get started running Hail on the Google Cloud Platform, see this `forum post <http://discuss.hail.is/t/using-hail-on-the-google-cloud-platform/80>`_.

---------------
BLAS and LAPACK
---------------

Hail uses BLAS and LAPACK optimized linear algebra libraries. On Linux, these must be explicitly installed. On Ubuntu 14.04, run::

    $ apt-get install libatlas-base-dev

If natives are not found, ``hail.log`` will contain the warnings::

    Failed to load implementation from: com.github.fommil.netlib.NativeSystemLAPACK
    Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS

See `netlib-java <http://github.com/fommil/netlib-java>`_ for more information.

-----------------
Running the tests
-----------------

Several Hail tests have additional dependencies:

 - `PLINK 1.9 <http://www.cog-genomics.org/plink2](http://www.cog-genomics.org/plink2>`_

 - `QCTOOL 1.4 <http://www.well.ox.ac.uk/~gav/qctool](http://www.well.ox.ac.uk/~gav/qctool>`_

 - `R 3.3.1 <http://www.r-project.org/](http://www.r-project.org/>`_ with packages ``jsonlite`` and ``logistf``, which depends on ``mice`` and ``Rcpp``.

Other recent versions of QCTOOL and R should suffice, but PLINK 1.0 will not.

To execute all Hail tests, run::

    $ ./gradlew test

