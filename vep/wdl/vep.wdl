version 1.0

workflow VariantEffectPredictorWorkflow {
  input {
    File vcffile
    File? vcfindexfile
    String output_format = "json"
    Array[String] cli_arguments = ["--everything"]
    String vep_docker_image = "us-docker.pkg.dev/broad-dsde-methods/exomiser/vep:r112_human_hg38"
  }

  if (output_format == "vcf") {
    String ext_1 = "vcf"
  }

  if (output_format == "json") {
    String ext_2 = "json"
  }

  if (output_format == "tsv") {
    String ext_3 = "tsv"
  }


  if ( !(defined(ext_1) || defined(ext_2) || defined(ext_3)) ) 
  {
    String error_message = "Error: output_format must be one of the following: vcf, json, tsv"
    call FailWithError {
      input:
        message = error_message
    }
    File error_file = FailWithError.error_file
  }

  String output_format_ext = select_first([ext_1, ext_2, ext_3, "__error__"])
  String output_filename = sub(basename(vcffile), "\\.vcf$", "_vep.~{output_format_ext}")

  call VariantEffectPredictorTask {
    input:
      vcffile=vcffile,
      vcfindexfile=vcfindexfile,
      output_format=output_format,
      output_filename=output_filename,
      cli_arguments=cli_arguments,
      vep_docker_image=vep_docker_image
  }
  
  output {
    File output_file = select_first([VariantEffectPredictorTask.output_file, error_file])
  }
}

task VariantEffectPredictorTask {
  input {
    File vcffile
    File? vcfindexfile
    String output_format = "json"
    String output_filename
    Array[String] cli_arguments = ["--everything"]
    String vep_docker_image = "us-docker.pkg.dev/broad-dsde-methods/exomiser/vep:r112_human_hg38"
  }

  command <<<
    # ;; syntax=shell
    set -ex
    shopt -s globstar

    case "~{output_format}" in
      vcf)
        output_format_cli="--vcf"
        ;;
      json)
        output_format_cli="--json"
        ;;
      tsv)
        output_format_cli="--tab"
        ;;
      *)
        echo "Error: output_format must be one of the following: vcf, json, tsv"
        exit 1
        ;;
    esac

    # XXX hardwired for organism
    #genome_fafn="/opt/vep/.vep/homo_sapiens/112_GRCh38/Homo_sapiens.GRCh38.dna.toplevel.fa.gz"

    # silence miniwdl
    # ~{vcfindexfile}
    #--fasta "${genome_fafn}" \
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

task FailWithError {
  input {
    String message
  }
  
  command <<<
      echo "~{message}" >&2
      exit 1
  >>>

  output {
    File error_file = stderr()
  }
}

