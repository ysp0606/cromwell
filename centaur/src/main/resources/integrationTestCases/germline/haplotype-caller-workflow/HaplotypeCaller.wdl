task ScatterIntervalList {
  File interval_list
  Int scatter_count
  Int break_bands_at_multiples_of

  command <<<
    mkdir out
    java -Xmx1g -jar /usr/picard/picard.jar \
    IntervalListTools \
    SCATTER_COUNT=${scatter_count} \
    SUBDIVISION_MODE=BALANCING_WITHOUT_INTERVAL_SUBDIVISION_WITH_OVERFLOW \
    UNIQUE=true \
    SORT=true \
    BREAK_BANDS_AT_MULTIPLES_OF=${break_bands_at_multiples_of} \
    INPUT=${interval_list} \
    OUTPUT=out
    python <<CODE
    import glob, os
    # Works around a JES limitation where multiples files with the same name overwrite each other when globbed
    for i, interval in enumerate(glob.glob("out/*/*.interval_list")):
        (directory, filename) = os.path.split(interval)
        newName = os.path.join(directory, str(i) + filename)
        os.rename(interval, newName)
    CODE
  >>>
  output {
    Array[File] out = glob("out/*/*.interval_list")
  }

  runtime {
    docker: "kcibul/picard"
    memory: "2 GB"
  }

}

task HaplotypeCaller {
  File input_bam
  File input_bam_index
  Float contamination
  File interval_list
  String gvcf_basename
  File ref_dict
  File ref_fasta
  File ref_fasta_index

  # tried to find lowest memory variable where it would still work, might change once tested on JES
  command {
    java -XX:GCTimeLimit=50 -XX:GCHeapFreeLimit=10 -Xmx6000m \
      -jar /usr/gitc/GenomeAnalysisTK-3.4-g3c929b0.jar \
      -T HaplotypeCaller \
      -R ${ref_fasta} \
      -o ${gvcf_basename}.vcf.gz \
      -I ${input_bam} \
      -L ${interval_list} \
      -ERC GVCF \
      --max_alternate_alleles 3 \
      -variant_index_parameter 128000 \
      -variant_index_type LINEAR \
      -contamination ${contamination} \
      --read_filter OverclippedRead
  }
  runtime {
    docker: "broadinstitute/genomes-in-the-cloud:1.892"
    memory: "8 GB"
    cpu: "1"
  }
  output {
    File gvcf = "${gvcf_basename}.vcf.gz"
    File gvcf_index = "${gvcf_basename}.vcf.gz.tbi"
  }
}

task GatherVCFs {
  Array[File] vcfs
  Array[File] vcfs_indexes
  String output_vcf_basename

  # using MergeVcfs instead of GatherVcfs so we can create indicies
  # WARNING	2015-10-28 15:01:48	GatherVcfs	Index creation not currently supported when gathering block compressed VCFs.
  command {
    java -Xmx2g -jar /usr/picard/picard.jar \
    MergeVcfs \
    INPUT=${sep=' INPUT=' vcfs} \
    OUTPUT="${output_vcf_basename}.vcf.gz"
  }
  output {
    File output_vcf = "${output_vcf_basename}.vcf.gz"
    File output_vcf_index = "${output_vcf_basename}.vcf.gz.tbi"
  }
  runtime {
    docker: "kcibul/picard"
    memory: "3 GB"
  }
}

workflow HaplotypeCallerWorkflow {
  String sample_name
  Float contamination
  Int scatter_count
  Int break_bands_at_multiples_of
  File input_bam
  File input_bam_index
  File calling_interval_list
  File ref_fasta
  File ref_fasta_index
  File ref_dict

  call ScatterIntervalList {
      input:
        interval_list = calling_interval_list,
        scatter_count = scatter_count,
        break_bands_at_multiples_of = break_bands_at_multiples_of
    }

  # Generate gVCF variant calls
  scatter (subInterval in ScatterIntervalList.out) {
     call HaplotypeCaller {
       input:
         input_bam = input_bam,
         input_bam_index = input_bam_index,
         contamination = contamination,
         interval_list = subInterval,
         gvcf_basename = sample_name,
         ref_dict = ref_dict,
         ref_fasta = ref_fasta,
         ref_fasta_index = ref_fasta_index
     }
  }

  call GatherVCFs {
    input:
      gvcfs = HaplotypeCaller.gvcf,
      gvcfs_indexes = HaplotypeCaller.gvcf_index,
      output_gvcf_basename = sample_name + ".final"
  }
}