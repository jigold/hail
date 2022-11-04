import json
import os
import re
from shlex import quote as shq
import subprocess as sp
import sys
import time


CONSEQUENCE_REGEX = re.compile(r'CSQ=[^;^\t]+')
CONSEQUENCE_HEADER_REGEX = re.compile(r'ID=CSQ[^>]+Description="([^"]+)')


def grouped_iterator(n, it):
    group = []
    while True:
        if len(group) == n:
            yield group
            group = []

        try:
            elt = next(it)
        except StopIteration:
            break

        group.append(elt)

    if group:
        yield group


def context():
    return '''##fileformat=VCFv4.1
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT
'''


class Variant:
    @staticmethod
    def from_vcf_line(l):
        fields = l.split('\t')
        contig = fields[0]
        pos = fields[1]
        ref = fields[3]
        alts = fields[4].split(',')
        return Variant(contig, pos, ref, alts)

    @staticmethod
    def from_string(s):
        fields = s.split(':')
        contig = fields[0]
        pos = fields[1]
        ref = fields[2]
        alts = fields[3].split(',')
        return Variant(contig, pos, ref, alts)

    def __init__(self, contig, pos, ref, alts):
        self.contig = contig
        self.position = pos
        self.ref = ref
        self.alts = alts

    def to_vcf_line(self):
        v = self.strip_star_allele()
        result = [v.contig, v.position, v.ref, ','.join(v.alts), '.', '.', 'GT']
        return '\t'.join(str(field) for field in result) + '\n'

    def to_locus_alleles(self):
        return ((self.contig, self.position), self.ref + self.alts)

    def strip_star_allele(self):
        return Variant(self.contig, self.position, self.ref, [a for a in self.alts if a != '*'])

    def __str__(self):
        result = [self.contig, self.position, self.ref, ','.join(self.alts)]
        return ':'.join(str(field) for field in result)


def consume_header(f) -> str:
    header = ''
    line = None
    pos = 0
    while line is None or line.startswith('#'):
        pos = f.tell()
        line = f.readline()
        if line.startswith('#'):
            header += line
    f.seek(pos)
    return header


def get_csq_header(vep_cmd):
    with sp.Popen(['bash', '-c', shq(vep_cmd)], env=os.environ, stdin=sp.PIPE, stdout=sp.PIPE, encoding='utf-8') as proc:
        header = context()
        v = Variant(1, 13372, 'G', ['C'])
        data = f'{header}\n{v.to_vcf_line()}'

        stdout, stderr = proc.communicate(data)
        if stderr:
            print(stderr)

        for line in stdout.split('\n'):
            line = line.rstrip()
            for match in CONSEQUENCE_HEADER_REGEX.finditer(line):
                return match.group(1)
        print('WARNING: could not get VEP CSQ header')
        return None


def run_vep(vep_cmd, input_file, block_size, consequence, tolerate_parse_error, part_id, env):
    results = []

    with open(input_file, 'r') as inp:
        header = consume_header(inp)
        for block_id, block in enumerate(grouped_iterator(block_size, inp)):
            n_processed = len(block)
            start_time = time.time()

            proc_id = '{{"part_id":{0},block_id:{1}}}'.format(part_id, block_id)
            variants = [Variant.from_vcf_line(l.rstrip()) for l in block]
            non_star_to_orig_variants = {str(v.strip_star_allele()): str(v) for v in variants}

            with sp.Popen(['bash', '-c', shq(vep_cmd)], env=env, stdin=sp.PIPE, stdout=sp.PIPE,
                          stderr=sp.PIPE, encoding='utf-8') as proc:
                data = f'{header}{"".join(block)}'

                stdout, stderr = proc.communicate(data)
                if stderr:
                    print(stderr)

                for line in stdout.split('\n'):
                    line = line.rstrip()
                    if line != '' and not line.startswith('#'):
                        if consequence:
                            vep_v = Variant.from_vcf_line(line)
                            orig_v_str = non_star_to_orig_variants.get(str(vep_v))
                            orig_v = Variant.from_string(orig_v_str)

                            if orig_v is not None:
                                x = CONSEQUENCE_REGEX.findall(line)
                                if x:
                                    first: str = x[0]
                                    result = (orig_v, first[4:].split(','), proc_id)
                                else:
                                    print('WARNING: No CSQ INFO field for VEP output variant {0}. VEP output is {1}'.format(vep_v, line))
                                    result = (orig_v, None, proc_id)
                            else:
                                raise ValueError('VEP output variant {0} not found in original variants. VEP output is {1}'.format(vep_v, line))
                        else:
                            try:
                                jv = json.loads(line)
                            except json.decoder.JSONDecodeError as e:
                                msg = 'VEP failed to produce parseable JSON!\n'
                                      f'json: {line}\n'
                                      f'error: {e.msg}'
                                if tolerate_parse_error:
                                    print(msg)
                                    continue
                                raise Exception(msg) from e
                            else:
                                variant_string = jv.get('input')
                                if variant_string is None:
                                    raise ValueError('VEP generated null variant string\n'
                                                     f'json: {line}\n'
                                                     f'parsed: {jv}')
                                v = Variant.from_vcf_line(variant_string)
                                orig_v_str = non_star_to_orig_variants.get(str(v))
                                if orig_v_str is not None:
                                    orig_v = Variant.from_string(orig_v_str)
                                    result = (orig_v, line, proc_id)
                                else:
                                    raise ValueError(f'VEP output variant {vep_v} not found in original variants. VEP output is {line}')

                        results.append(result)

                if proc.returncode != 0:
                    raise ValueError(f'VEP command {vep_cmd} failed with non-zero exit status {proc.returncode}\n'
                                     'VEP error output:\n'
                                     f'{stderr}')

            elapsed_time = time.time() - start_time
            print(f'processed {n_processed} variants in {elapsed_time}')

    return results


if __name__ == '__main__':
    action = sys.argv[1]

    consequence = bool(os.environ['VEP_CONSEQUENCE'])
    tolerate_parse_error = bool(os.environ['VEP_TOLERATE_PARSE_ERROR'])
    block_size = int(os.environ['VEP_BLOCK_SIZE'])
    input_file = os.environ['VEP_INPUT_FILE']
    output_file = os.environ['VEP_OUTPUT_FILE']
    data_dir = os.environ['VEP_DATA_MOUNT']
    part_id = os.environ['VEP_PART_ID']

    reference_genome = os.environ['REFERENCE_GENOME']
    if reference_genome == 'grch37':
        # Had to add loftee_path:/vep_bin/loftee in order to get the loftee plugin to be found
        # Had to add dir = /root/.vep for the cache to be found because the home dir can no longer be /vep
        vep_cmd = f'''
/vep --input_file {input_file} \
    --format vcf \
    {"--vcf" if consequence else "--json"} \
    --everything \
    --allele_number \
    --no_stats \
    --cache \
    --offline \
    --minimal \
    --assembly GRCh37 \
    --dir={data_dir} \
    --dir_plugins={data_dir}/Plugins/ \
    --plugin LoF,loftee_path:/vep_bin/loftee,human_ancestor_fa:{data_dir}/loftee_data/human_ancestor.fa.gz,filter_position:0.05,min_intron_size:15,conservation_file:{data_dir}/loftee_data/phylocsf_gerp.sql,gerp_file:{data_dir}/loftee_data/GERP_scores.final.sorted.txt.gz
    {output_file}
'''
    else:
        assert reference_genome == 'grch38'
        vep_cmd = f'''
/vep --input_file {input_file} \
    --format vcf \
    {"--vcf" if consequence else "--json"} \
    --everything \
    --allele_number \
    --no_stats \
    --cache \
    --offline \
    --minimal \
    --verbose \
    --assembly GRCh38 \
    --dir={data_dir} \
    --fasta /opt/vep/.vep/homo_sapiens/95_GRCh38/Homo_sapiens.GRCh38.dna.toplevel.fa.gz \
    --plugin LoF,loftee_path:/opt/vep/Plugins/,gerp_bigwig:{data_dir}/gerp_conservation_scores.homo_sapiens.GRCh38.bw,human_ancestor_fa:{data_dir}/human_ancestor.fa.gz,conservation_file:{data_dir}/loftee.sql \
    --dir_plugins {data_dir}/Plugins/ \
    -o {output_file}
'''

    if action == 'csq_header':
        print('running csq header function')
        # csq_header = get_csq_header(vep_cmd)
        # with open(output_file, 'w') as out:
        #     out.write(f'{csq_header}\n')
    else:
        assert action == 'vep'
        # print('running vep function')
        # run_vep(vep_cmd, input_file, block_size, consequence, tolerate_parse_error, part_id, os.environ)
