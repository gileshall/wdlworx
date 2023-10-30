version 1.0

workflow MultitoolWorkflow {
  input {
    Array[File] file_manifest
    String python_script_url
    String docker_image = "python:3.18-bullseye"
  }

  call MultitoolTask {
    input:
      file_manifest=file_manifest,
      python_script_url=python_script_url,
      docker_image=docker_image
  }

  output {
    Array[File] results = MultitoolTask.results
  }
}

task MultitoolTask {
  input {
    Array[File] file_manifest
    String python_script_url
    String docker_image = "python:3.18-bullseye"
  }

  command <<<
    set -ex
    mkdir output
    for manifest_filename in ~{sep=" " file_manifest}
    do
        echo "$manifest_filename"
        echo "$(ls -la $manifest_filename)"
    done

    wget "~{python_script_url}"
    python $(basename "~{python_script_url}")
  >>>

  output {
    Array[File] results = glob("output/*")
  }

  runtime {
    cpu: "2"
    memory: "4 GB"
    docker: "~{docker_image}"
    disks: "local-disk 16 HDD"
  }
}
