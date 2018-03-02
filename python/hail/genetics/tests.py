import unittest

import hail as hl
from hail.genetics import *

def setUpModule():
    hl.init(master='local[2]', min_block_size=0)

def tearDownModule():
    hl.stop()

class Tests(unittest.TestCase):
    def test_classes(self):
        l = Locus.parse('1:100')

        self.assertEqual(l, Locus('1', 100))
        self.assertEqual(l, Locus(1, 100))
        self.assertEqual(l.reference_genome, hl.default_reference())

        c_hom_ref = Call([0, 0])
        self.assertEqual(c_hom_ref.alleles, [0, 0])
        self.assertEqual(c_hom_ref.ploidy, 2)
        self.assertFalse(c_hom_ref.phased)
        self.assertFalse(c_hom_ref.is_haploid())
        self.assertTrue(c_hom_ref.is_diploid())
        self.assertEqual(c_hom_ref.num_alt_alleles(), 0)
        self.assertTrue(c_hom_ref.one_hot_alleles(2) == [2, 0])
        self.assertTrue(c_hom_ref.is_hom_ref())
        self.assertFalse(c_hom_ref.is_het())
        self.assertFalse(c_hom_ref.is_hom_var())
        self.assertFalse(c_hom_ref.is_non_ref())
        self.assertFalse(c_hom_ref.is_het_non_ref())
        self.assertFalse(c_hom_ref.is_het_ref())
        self.assertTrue(c_hom_ref.unphased_diploid_gt_index() == 0)

        c_het_phased = Call([1, 0], phased=True)
        self.assertEqual(c_het_phased.alleles, [1, 0])
        self.assertEqual(c_het_phased.ploidy, 2)
        self.assertTrue(c_het_phased.phased)
        self.assertFalse(c_het_phased.is_haploid())
        self.assertTrue(c_het_phased.is_diploid())
        self.assertEqual(c_het_phased.num_alt_alleles(), 1)
        self.assertTrue(c_het_phased.one_hot_alleles(2) == [1, 1])
        self.assertFalse(c_het_phased.is_hom_ref())
        self.assertTrue(c_het_phased.is_het())
        self.assertFalse(c_het_phased.is_hom_var())
        self.assertTrue(c_het_phased.is_non_ref())
        self.assertFalse(c_het_phased.is_het_non_ref())
        self.assertTrue(c_het_phased.is_het_ref())

        c_hom_var = Call([1, 1])
        self.assertEqual(c_hom_var.alleles, [1, 1])
        self.assertEqual(c_hom_var.ploidy, 2)
        self.assertFalse(c_hom_var.phased)
        self.assertFalse(c_hom_var.is_haploid())
        self.assertTrue(c_hom_var.is_diploid())
        self.assertEqual(c_hom_var.num_alt_alleles(), 2)
        self.assertTrue(c_hom_var.one_hot_alleles(2) == [0, 2])
        self.assertFalse(c_hom_var.is_hom_ref())
        self.assertFalse(c_hom_var.is_het())
        self.assertTrue(c_hom_var.is_hom_var())
        self.assertTrue(c_hom_var.is_non_ref())
        self.assertFalse(c_hom_var.is_het_non_ref())
        self.assertFalse(c_hom_var.is_het_ref())
        self.assertTrue(c_hom_var.unphased_diploid_gt_index() == 2)

        c_haploid = Call([2], phased=True)
        self.assertEqual(c_haploid.alleles, [2])
        self.assertEqual(c_haploid.ploidy, 1)
        self.assertTrue(c_haploid.phased)
        self.assertTrue(c_haploid.is_haploid())
        self.assertFalse(c_haploid.is_diploid())
        self.assertEqual(c_haploid.num_alt_alleles(), 1)
        self.assertTrue(c_haploid.one_hot_alleles(3) == [0, 0, 1])
        self.assertFalse(c_haploid.is_hom_ref())
        self.assertFalse(c_haploid.is_het())
        self.assertTrue(c_haploid.is_hom_var())
        self.assertTrue(c_haploid.is_non_ref())
        self.assertFalse(c_haploid.is_het_non_ref())
        self.assertFalse(c_haploid.is_het_ref())

        c_zeroploid = Call([])
        self.assertEqual(c_zeroploid.alleles, [])
        self.assertEqual(c_zeroploid.ploidy, 0)
        self.assertFalse(c_zeroploid.phased)
        self.assertFalse(c_zeroploid.is_haploid())
        self.assertFalse(c_zeroploid.is_diploid())
        self.assertEqual(c_zeroploid.num_alt_alleles(), 0)
        self.assertTrue(c_zeroploid.one_hot_alleles(3) == [0, 0, 0])
        self.assertFalse(c_zeroploid.is_hom_ref())
        self.assertFalse(c_zeroploid.is_het())
        self.assertFalse(c_zeroploid.is_hom_var())
        self.assertFalse(c_zeroploid.is_non_ref())
        self.assertFalse(c_zeroploid.is_het_non_ref())
        self.assertFalse(c_zeroploid.is_het_ref())

        self.assertRaisesRegex(NotImplementedError,
                               "Calls with greater than 2 alleles are not supported.",
                               Call,
                               [1, 1, 1, 1])

        rg = ReferenceGenome.GRCh37()
        self.assertEqual(rg.name, "GRCh37")
        self.assertEqual(rg.contigs[0], "1")
        self.assertListEqual(rg.x_contigs, ["X"])
        self.assertListEqual(rg.y_contigs, ["Y"])
        self.assertListEqual(rg.mt_contigs, ["MT"])
        self.assertEqual(rg.par[0], hl.Interval.parse_locus_interval("X:60001-2699521"))
        self.assertEqual(rg.contig_length("1"), 249250621)

        name = "test"
        contigs = ["1", "X", "Y", "MT"]
        lengths = {"1": 10000, "X": 2000, "Y": 4000, "MT": 1000}
        x_contigs = ["X"]
        y_contigs = ["Y"]
        mt_contigs = ["MT"]
        par = [("X", 5, 1000)]

        gr2 = ReferenceGenome(name, contigs, lengths, x_contigs, y_contigs, mt_contigs, par)
        self.assertEqual(gr2.name, name)
        self.assertListEqual(gr2.contigs, contigs)
        self.assertListEqual(gr2.x_contigs, x_contigs)
        self.assertListEqual(gr2.y_contigs, y_contigs)
        self.assertListEqual(gr2.mt_contigs, mt_contigs)
        self.assertEqual(gr2.par, [hl.Interval.parse_locus_interval("X:5-1000", gr2)])
        self.assertEqual(gr2.contig_length("1"), 10000)
        self.assertDictEqual(gr2.lengths, lengths)
        gr2.write("/tmp/my_gr.json")

        gr3 = ReferenceGenome.read("src/test/resources/fake_ref_genome.json")
        self.assertEqual(gr3.name, "my_reference_genome")


    def test_pedigree(self):

        ped = Pedigree.read('src/test/resources/sample.fam')
        ped.write('/tmp/sample_out.fam')
        ped2 = Pedigree.read('/tmp/sample_out.fam')
        self.assertEqual(ped, ped2)
        print(ped.trios[:5])
        print(ped.complete_trios())

        t1 = Trio('kid1', pat_id='dad1', is_female=True)
        t2 = Trio('kid1', pat_id='dad1', is_female=True)

        self.assertEqual(t1, t2)

        self.assertEqual(t1.fam_id, None)
        self.assertEqual(t1.s, 'kid1')
        self.assertEqual(t1.pat_id, 'dad1')
        self.assertEqual(t1.mat_id, None)
        self.assertEqual(t1.is_female, True)
        self.assertEqual(t1.is_complete(), False)
        self.assertEqual(t1.is_female, True)
        self.assertEqual(t1.is_male, False)
