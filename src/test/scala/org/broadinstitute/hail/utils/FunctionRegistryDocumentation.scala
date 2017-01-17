package org.broadinstitute.hail.utils

import org.testng.annotations.Test
import org.broadinstitute.hail.expr.FunctionRegistry
import org.broadinstitute.hail.SparkSuite

class FunctionRegistryDocumentation extends SparkSuite {
  @Test def test() = {
    FunctionRegistry.generateDocumentation(sc, "test_documentation.rst")
  }
}
