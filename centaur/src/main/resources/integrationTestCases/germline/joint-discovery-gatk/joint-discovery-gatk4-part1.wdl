workflow JointGenotyping {
  File unpadded_intervals_file

  String callset_name
  File sample_name_map

  File ref_fasta
  File ref_fasta_index
  File ref_dict

  File dbsnp_vcf
  File dbsnp_vcf_index

  String gatk_docker
  String gatk_path
  String python_docker

  Int small_disk
  Int medium_disk
  Int huge_disk

  Array[String] snp_recalibration_tranche_values
  Array[String] snp_recalibration_annotation_values
  Array[String] indel_recalibration_tranche_values
  Array[String] indel_recalibration_annotation_values

  File eval_interval_list
  File hapmap_resource_vcf
  File hapmap_resource_vcf_index
  File omni_resource_vcf
  File omni_resource_vcf_index
  File one_thousand_genomes_resource_vcf
  File one_thousand_genomes_resource_vcf_index
  File mills_resource_vcf
  File mills_resource_vcf_index
  File axiomPoly_resource_vcf
  File axiomPoly_resource_vcf_index
  File dbsnp_resource_vcf = dbsnp_vcf
  File dbsnp_resource_vcf_index = dbsnp_vcf_index

  # ExcessHet is a phred-scaled p-value. We want a cutoff of anything more extreme
  # than a z-score of -4.5 which is a p-value of 3.4e-06, which phred-scaled is 54.69
  Float excess_het_threshold = 54.69
  Float snp_filter_level
  Float indel_filter_level
  Int SNP_VQSR_downsampleFactor

  Int num_of_original_intervals = length(read_lines(unpadded_intervals_file))
  Int num_gvcfs = length(read_lines(sample_name_map))

  # Make a 2.5:1 interval number to samples in callset ratio interval list
  Int possible_merge_count = floor(num_of_original_intervals / num_gvcfs / 2.5)
  Int merge_count = if possible_merge_count > 1 then possible_merge_count else 1

  call DynamicallyCombineIntervals {
    input:
      intervals = unpadded_intervals_file,
      merge_count = merge_count,
      docker_image = python_docker
  }

  Array[String] unpadded_intervals = read_lines(DynamicallyCombineIntervals.output_intervals)

  scatter (idx in range(length(unpadded_intervals))) {
    # the batch_size value was carefully chosen here as it
    # is the optimal value for the amount of memory allocated
    # within the task; please do not change it without consulting
    # the Hellbender (GATK engine) team!
    call ImportGVCFs {
      input:
        sample_name_map = sample_name_map,
        interval = unpadded_intervals[idx],
        workspace_dir_name = "genomicsdb",
        disk_size = medium_disk,
        docker_image = gatk_docker,
        gatk_path = gatk_path,
        batch_size = 50
    }

    call GenotypeGVCFs {
      input:
        workspace_tar = ImportGVCFs.output_genomicsdb,
        interval = unpadded_intervals[idx],
        output_vcf_filename = "output.vcf.gz",
        ref_fasta = ref_fasta,
        ref_fasta_index = ref_fasta_index,
        ref_dict = ref_dict,
        dbsnp_vcf = dbsnp_vcf,
        disk_size = medium_disk,
        docker_image = gatk_docker,
        gatk_path = gatk_path
    }

    call HardFilterAndMakeSitesOnlyVcf {
      input:
        vcf = GenotypeGVCFs.output_vcf,
        vcf_index = GenotypeGVCFs.output_vcf_index,
        excess_het_threshold = excess_het_threshold,
        variant_filtered_vcf_filename = callset_name + "." + idx + ".variant_filtered.vcf.gz",
        sites_only_vcf_filename = callset_name + "." + idx + ".sites_only.variant_filtered.vcf.gz",
        disk_size = medium_disk,
        docker_image = gatk_docker,
        gatk_path = gatk_path
   }
  }

  call GatherVcfs as SitesOnlyGatherVcf {
    input:
      input_vcfs_fofn = write_lines(HardFilterAndMakeSitesOnlyVcf.sites_only_vcf),
      output_vcf_name = callset_name + ".sites_only.vcf.gz",
      disk_size = medium_disk,
      docker_image = gatk_docker,
      gatk_path = gatk_path
  }

 output {
    SitesOnlyGatherVcf.output_vcf
    SitesOnlyGatherVcf.output_vcf_index
    #HardFilter
    HardFilterAndMakeSitesOnlyVcf.variant_filtered_vcf
    HardFilterAndMakeSitesOnlyVcf.variant_filtered_vcf_index
    HardFilterAndMakeSitesOnlyVcf.sites_only_vcf
    HardFilterAndMakeSitesOnlyVcf.sites_only_vcf_index
    # output the interval list generated/used by this run workflow
    DynamicallyCombineIntervals.output_intervals
  }
}
task GetNumberOfSamples {
  File sample_name_map
  String mem_size
  Int preemptibles

  command <<<
    wc -l ${sample_name_map} | awk '{print $1}'
  >>>
  runtime {
    docker: docker_image
    memory: mem_size
    preemptible: preemptibles
  }
  output {
    Int sample_count = read_int(stdout())
  }
}

task ImportGVCFs {
  File sample_name_map
  String interval

  String workspace_dir_name

  String java_opt
  String gatk_path

  String docker_image
  Int disk_size
  String mem_size
  Int preemptibles
  Int batch_size

  command <<<
    set -e

    rm -rf ${workspace_dir_name}

    # The memory setting here is very important and must be several GB lower
    # than the total memory allocated to the VM because this tool uses
    # a significant amount of non-heap memory for native libraries.
    # Also, testing has shown that the multithreaded reader initialization
    # does not scale well beyond 5 threads, so don't increase beyond that.
    ${gatk_path} --javaOptions "${java_opt}" \
    GenomicsDBImport \
    --genomicsDBWorkspace ${workspace_dir_name} \
    --batchSize ${batch_size} \
    -L ${interval} \
    --sampleNameMap ${sample_name_map} \
    --readerThreads 5 \
    -ip 500

    tar -cf ${workspace_dir_name}.tar ${workspace_dir_name}

  >>>
  runtime {
    docker: docker_image
    memory: mem_size
    cpu: "2"
    disks: "local-disk " + disk_size + " HDD"
    preemptible: preemptibles
  }
  output {
    File output_genomicsdb = "${workspace_dir_name}.tar"
  }
}

task GenotypeGVCFs {
  File workspace_tar
  String interval

  String output_vcf_filename

  String gatk_path
  String java_opt

  File ref_fasta
  File ref_fasta_index
  File ref_dict

  String dbsnp_vcf

  String docker_image
  Int disk_size
  String mem_size
  Int preemptibles

  command <<<
    set -e

    tar -xf ${workspace_tar}
    WORKSPACE=$( basename ${workspace_tar} .tar)

    ${gatk_path} --javaOptions "${java_opt}" \
     GenotypeGVCFs \
     -R ${ref_fasta} \
     -O ${output_vcf_filename} \
     -D ${dbsnp_vcf} \
     -G StandardAnnotation \
     --onlyOutputCallsStartingInIntervals \
     -newQual \
     -V gendb://$WORKSPACE \
     -L ${interval}
  >>>
  runtime {
    docker: docker_image
    memory: mem_size
    cpu: "2"
    disks: "local-disk " + disk_size + " HDD"
    preemptible: preemptibles
  }
  output {
    File output_vcf = "${output_vcf_filename}"
    File output_vcf_index = "${output_vcf_filename}.tbi"
  }
}

task HardFilterAndMakeSitesOnlyVcf {
  File vcf
  File vcf_index
  Float excess_het_threshold

  String variant_filtered_vcf_filename
  String sites_only_vcf_filename

  String gatk_path
  String java_opt

  String docker_image
  Int disk_size
  String mem_size
  Int preemptibles

  command {
    set -e

    ${gatk_path} --javaOptions "${java_opt}" \
      VariantFiltration \
      --filterExpression "ExcessHet > ${excess_het_threshold}" \
      --filterName ExcessHet \
      -O ${variant_filtered_vcf_filename} \
      -V ${vcf}

    ${gatk_path} --javaOptions "${java_opt}" \
      MakeSitesOnlyVcf \
      --INPUT ${variant_filtered_vcf_filename} \
      --OUTPUT ${sites_only_vcf_filename}

  }
  runtime {
    docker: docker_image
    memory: mem_size
    cpu: "1"
    disks: "local-disk " + disk_size + " HDD"
    preemptible: preemptibles
  }
  output {
    File variant_filtered_vcf = "${variant_filtered_vcf_filename}"
    File variant_filtered_vcf_index = "${variant_filtered_vcf_filename}.tbi"
    File sites_only_vcf = "${sites_only_vcf_filename}"
    File sites_only_vcf_index = "${sites_only_vcf_filename}.tbi"
  }
}

task GatherVcfs {
  File input_vcfs_fofn
  String output_vcf_name

  String gatk_path
  String java_opt

  String docker_image
  Int disk_size
  String mem_size
  Int preemptibles

  command <<<
    set -e

    # Now using NIO to localize the vcfs but the input file must have a ".list" extension
    mv ${input_vcfs_fofn} inputs.list

    # ignoreSafetyChecks make a big performance difference so we include it in our invocation
    ${gatk_path} --javaOptions "${java_opt}" \
    GatherVcfsCloud \
    --ignoreSafetyChecks \
    --gatherType BLOCK \
    --input inputs.list \
    --output ${output_vcf_name}

    /gatk/gatk-launch --javaOptions "-Xmx6g -Xms6g" \
    IndexFeatureFile \
    --feature_file ${output_vcf_name}
  >>>
  runtime {
    docker: docker_image
    memory: mem_size
    cpu: "1"
    disks: "local-disk " + disk_size + " HDD"
    preemptible: preemptibles
  }
  output {
    File output_vcf = "${output_vcf_name}"
    File output_vcf_index = "${output_vcf_name}.tbi"
  }
}
task DynamicallyCombineIntervals {
  File intervals
  Int merge_count
  String docker_image
  String mem_size
  Int preemptibles

  command {
    python << CODE
    def parse_interval(interval):
        colon_split = interval.split(":")
        chromosome = colon_split[0]
        dash_split = colon_split[1].split("-")
        start = int(dash_split[0])
        end = int(dash_split[1])
        return chromosome, start, end

    def add_interval(chr, start, end):
        lines_to_write.append(chr + ":" + str(start) + "-" + str(end))
        return chr, start, end

    count = 0
    chain_count = ${merge_count}
    l_chr, l_start, l_end = "", 0, 0
    lines_to_write = []
    with open("${intervals}") as f:
        with open("out.intervals", "w") as f1:
            for line in f.readlines():
                # initialization
                if count == 0:
                    w_chr, w_start, w_end = parse_interval(line)
                    count = 1
                    continue
                # reached number to combine, so spit out and start over
                if count == chain_count:
                    l_char, l_start, l_end = add_interval(w_chr, w_start, w_end)
                    w_chr, w_start, w_end = parse_interval(line)
                    count = 1
                    continue

                c_chr, c_start, c_end = parse_interval(line)
                # if adjacent keep the chain going
                if c_chr == w_chr and c_start == w_end + 1:
                    w_end = c_end
                    count += 1
                    continue
                # not adjacent, end here and start a new chain
                else:
                    l_char, l_start, l_end = add_interval(w_chr, w_start, w_end)
                    w_chr, w_start, w_end = parse_interval(line)
                    count = 1
            if l_char != w_chr or l_start != w_start or l_end != w_end:
                add_interval(w_chr, w_start, w_end)
            f1.writelines("\n".join(lines_to_write))
    CODE
  }

  runtime {
    memory: mem_size
    preemptible: preemptibles
    docker: docker_image
  }

  output {
    File output_intervals = "out.intervals"
  }
}