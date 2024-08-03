version 1.0

workflow scatter_vcf {
  input {
    File vcf
    Int num_splits
  }

  call split_vcf {
    input:
      vcf = vcf,
      num_splits = num_splits,
  }

  scatter (vcf_part in split_vcf.vcf_parts) {
    call VariantEffectPredictorTask {
      input:
        vcffile = vcf_part,
        output_filename = basename(vcf_part, ".vcf") + "_vep.tsv"
    }
  }

  call concat_csv {
    input:
      csv_files = VariantEffectPredictorTask.output_file,
      original_vcf_name = basename(vcf, ".vcf")
  }

  output {
    File final_csv = concat_csv.output_csv
  }
}

task split_vcf {
  input {
    File vcf
    Int num_splits
  }

  String output_prefix = "split_vcf_output"

  command <<<
    apt-get update && apt-get install -y bcftools
    pip install pysam
    python3 <<CODE
import os
import subprocess
import multiprocessing
from multiprocessing import Pool
import pysam

def count_variants(vcf_file):
    view_proc = subprocess.Popen(['bcftools', 'view', '-H', vcf_file], stdout=subprocess.PIPE)
    wc_proc = subprocess.Popen(['wc', '-l'], stdin=view_proc.stdout, stdout=subprocess.PIPE)
    wc_proc.wait()
    assert wc_proc.returncode == 0
    return int(wc_proc.stdout.read().decode().strip())

def split_vcf(args):
    vcf_file, output_dir, output_prefix, split_idx, split_size, vcf_extension = args
    split_filename = f'{output_dir}/{output_prefix}-{split_idx:04d}.{vcf_extension}'
    with pysam.VariantFile(vcf_file) as vcf_in:
        with pysam.VariantFile(split_filename, 'w', header=vcf_in.header) as vcf_out:
            for i, rec in enumerate(vcf_in):
                if i >= split_idx * split_size and i < (split_idx + 1) * split_size:
                    vcf_out.write(rec)
                if i >= (split_idx + 1) * split_size:
                    break

def distribute_items(total_items, num_buckets):
    min_items_per_bucket = total_items // num_buckets
    remaining_items = total_items % num_buckets

    for i in range(num_buckets):
        if i < remaining_items:
            yield min_items_per_bucket + 1
        else:
            yield min_items_per_bucket

vcf_file = "~{vcf}"
num_splits = ~{num_splits}
output_dir = "split_vcf"
output_prefix = "~{output_prefix}"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

vcf_extension = 'vcf.gz' if vcf_file.endswith('.gz') else 'vcf'
output_prefix = output_prefix if output_prefix else os.path.basename(vcf_file).replace('.vcf', '').replace('.gz', '')

num_variants = count_variants(vcf_file)
split_sizes = distribute_items(num_variants, num_splits)

pool_args = [(vcf_file, output_dir, output_prefix, idx, split_size, vcf_extension) for (idx, split_size) in enumerate(split_sizes)]

with Pool(multiprocessing.cpu_count()) as pool:
    pool.map(split_vcf, pool_args)
CODE
  >>>

  output {
    Array[File] vcf_parts = glob("split_vcf/${output_prefix}*")
  }

  runtime {
    docker: "python:3.8-slim"
  }

}

task VariantEffectPredictorTask {
  input {
    File vcffile
    File? vcfindexfile
    String output_filename
    Array[String] cli_arguments = ["--everything"]
    String vep_docker_image = "us-docker.pkg.dev/broad-dsde-methods/exomiser/vep:r112_human_hg38"
  }

  command <<<
    # ;; syntax=shell
    set -ex
    shopt -s globstar

    output_format_cli="--tab"

    vep \
      --input_file "~{vcffile}" \
      "${output_format_cli}" \
      --output_file "~{output_filename}" \
      --fork "$(nproc)" \
      --offline \
      ~{sep=" " cli_arguments}

    # ;; syntax=
  >>>

  output {
    File output_file = "~{output_filename}"
  }

  runtime {
    cpu: "16"
    memory: "32 GB"
    docker: "~{vep_docker_image}"
    disks: "local-disk 256 HDD"
  }
}

task concat_csv {
  input {
    Array[File] csv_files
    String original_vcf_name
  }

  File csv_file_list = write_lines(csv_files)
  String concatenated_csv = "${original_vcf_name}_final.csv"

  command <<<
    python3 <<CODE
import sys

def concat_csv(file_list, output_file):
    header_written = False
    with open(output_file, 'w') as outfile:
        for file in file_list:
            with open(file, 'r') as infile:
                header = infile.readline()
                if not header_written:
                    outfile.write(header)
                    header_written = True
                for line in infile:
                    outfile.write(line)

file_list = "~{csv_file_list}"
output_file = "~{concatenated_csv}"

with open(file_list, 'r') as f:
    files = f.read().strip().split()
    print(files)

concat_csv(files, output_file)
CODE
  >>>

  output {
    File output_csv = "${concatenated_csv}"
  }

  runtime {
    docker: "python:3.8-slim"
  }
}
