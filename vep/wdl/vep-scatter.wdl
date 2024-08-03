version 1.0

workflow VariantEffectPredictorWorkflowScatter {
  input {
    File vcf
    Int num_splits
  }

  # Remove .vcf, .vcf.gz, or .vcf.bz2 from the filename
  String base_name = basename(vcf)
  String name_without_gz = sub(base_name, "\\.vcf\\.gz$", "")
  String name_without_bz2 = sub(name_without_gz, "\\.vcf\\.bz2$", "")
  String original_vcf_name = sub(name_without_bz2, "\\.vcf$", "")

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
      original_vcf_name = original_vcf_name
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
  Int required_disk_size_gb = 3 * ceil(size(vcf, "GB"))

  command <<<
    apt-get update && apt-get install -y bcftools
    python3 <<CODE
import os
import subprocess
import multiprocessing
from multiprocessing import Pool

def count_variants(vcf_file):
    view_proc = subprocess.Popen(['bcftools', 'view', '-H', vcf_file], stdout=subprocess.PIPE)
    wc_proc = subprocess.Popen(['wc', '-l'], stdin=view_proc.stdout, stdout=subprocess.PIPE)
    wc_proc.wait()
    assert wc_proc.returncode == 0
    return int(wc_proc.stdout.read().decode().strip())

def get_vcf_header(vcf_file):
    view_proc = subprocess.Popen(['bcftools', 'view', '-h', vcf_file], stdout=subprocess.PIPE)
    header, _ = view_proc.communicate()
    assert view_proc.returncode == 0
    return header.decode()

def split_vcf(args):
    vcf_file, output_dir, output_prefix, split_idx, split_size, vcf_extension, header = args
    split_filename = f'{output_dir}/{output_prefix}-{split_idx:04d}.{vcf_extension}'

    start = split_idx * split_size + 1
    end = (split_idx + 1) * split_size

    with open(split_filename, 'wb') as out_f:
        out_f.write(header.encode())
        view_proc = subprocess.Popen(['bcftools', 'view', '-H', vcf_file], stdout=subprocess.PIPE)
        awk_proc = subprocess.Popen(['awk', '-v', f'start={start}', '-v', f'end={end}', 'NR>=start&&NR<=end'], stdin=view_proc.stdout, stdout=out_f)
        awk_proc.wait()

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
output_prefix = output_prefix if output_prefix else os.path.basename(vcf_file).replace('.vcf', '').replace('.gz', '').replace('.bz2', '')

num_variants = count_variants(vcf_file)
header = get_vcf_header(vcf_file)
split_sizes = distribute_items(num_variants, num_splits)

pool_args = [(vcf_file, output_dir, output_prefix, idx, split_size, vcf_extension, header) for (idx, split_size) in enumerate(split_sizes)]

with Pool(multiprocessing.cpu_count()) as pool:
    pool.map(split_vcf, pool_args)
CODE
  >>>

  output {
    Array[File] vcf_parts = glob("split_vcf/${output_prefix}*")
  }

  runtime {
    cpu: "16"
    memory: "32 GB"
    docker: "python:3.8-slim"
    disks: "local-disk ${required_disk_size_gb} HDD"
  }

}

task VariantEffectPredictorTask {
  input {
    File vcffile
    File? vcfindexfile
    String output_filename
    Array[String] cli_arguments = ["--everything"]
    #String vep_docker_image = "us-docker.pkg.dev/broad-dsde-methods/exomiser/vep:r112_human_hg38"
    String vep_docker_image = "vep"
  }

  Int required_disk_size_gb = 3 * ceil(size(vcffile, "GB"))

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
    disks: "local-disk ${required_disk_size_gb} HDD"
  }
}

task concat_csv {
  input {
    Array[File] csv_files
    String original_vcf_name
  }

  File csv_file_list = write_lines(csv_files)
  String concatenated_csv = "${original_vcf_name}_final.csv"

  Int required_disk_size_gb = 3 * ceil(size(csv_files, "GB"))

  command <<<
    python3 <<CODE
import sys

def concat_csv(file_list, output_file):
    header_written = False
    with open(output_file, 'w') as outfile:
        for file in file_list:
            with open(file, 'r') as infile:
                for line in infile:
                    if not line.startswith('## '):
                      break
                header = line
                if not header_written:
                    while header[0] == '#':
                        header = header[1:]
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
    memory: "8 GB"
    docker: "python:3.8-slim"
    disks: "local-disk ${required_disk_size_gb} HDD"
  }
}
