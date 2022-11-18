import os
import sys

from vep import main

action = sys.argv[1]

consequence = bool(int(os.environ['VEP_CONSEQUENCE']))
tolerate_parse_error = bool(int(os.environ['VEP_TOLERATE_PARSE_ERROR']))
block_size = int(os.environ['VEP_BLOCK_SIZE'])
input_file = os.environ['VEP_INPUT_FILE']
output_file = os.environ['VEP_OUTPUT_FILE']
data_dir = os.environ['VEP_DATA_MOUNT']
part_id = os.environ['VEP_PART_ID']

vep_cmd = f'''/vep \
--input_file {input_file} \
--format vcf {"--vcf" if consequence else "--json"} \
--everything \
--allele_number \
--no_stats \
--cache \
--offline \
--minimal \
--assembly GRCh37 \
--dir={data_dir} \
--plugin LoF,loftee_path:/vep_bin/loftee,human_ancestor_fa:{data_dir}/loftee_data/human_ancestor.fa.gz,filter_position:0.05,min_intron_size:15,conservation_file:{data_dir}/loftee_data/phylocsf_gerp.sql,gerp_file:{data_dir}/loftee_data/GERP_scores.final.sorted.txt.gz \
-o STDOUT
'''

main(action, consequence, tolerate_parse_error, block_size, input_file, output_file, part_id, vep_cmd)
