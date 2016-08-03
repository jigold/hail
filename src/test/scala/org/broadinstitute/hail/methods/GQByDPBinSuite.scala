package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.io.vcf.LoadVCF
import org.testng.annotations.Test

class GQByDPBinSuite extends SparkSuite {
  @Test def test() {
    val vds = LoadVCF(sc, "src/test/resources/gqbydp_test.vcf")
    val gqbydp = GQByDPBins(vds)
    assert(gqbydp == Map(("1", 5) -> 0.5, ("2", 2) -> 0.0))
  }
}
