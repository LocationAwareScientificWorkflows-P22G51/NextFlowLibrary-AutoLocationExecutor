nextflow.enable.dsl=2

params.data_dir = "/external/diskC/22P63/shotgun/*gz"
input_ch = Channel.fromPath("${params.data_dir}")
node_suggestion = [:] 

process fastqc {
   echo true
   input:
      path input_ch
   output:
      file ("*/*{zip,html}")
   script:
      base = input_ch.simpleName
   """
      mkdir $base
      /home/tlilford/FastQC/fastqc $input_ch --outdir $base
      echo SLURM_JOB_ID: $SLURM_JOB_ID
      echo SLURM_JOB_NODELIST: $SLURM_JOB_NODELIST
      echo SLURM_SUBMIT_DIR: $SLURM_SUBMIT_DIR
      hostname
   """
}

workflow {
    fastqc(input_ch)
}
