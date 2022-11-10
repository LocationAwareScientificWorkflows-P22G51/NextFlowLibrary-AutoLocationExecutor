nextflow.enable.dsl=2

def printCurrentClusterStatus(){
  try {
    cmd = "squeue"
    queue_status = cmd.execute().text
    cmd = "sinfo"
    node_status = cmd.execute().text
    println "${queue_status}" + "\n"
    println "${node_status}" + "\n"
  }catch(Exception ex){
    println "Error: cluster squeue and/or sinfo unavailble"
  }
}
//"/external/diskC/22P63/shotgun/SRR13061610.fastq.gz" + 
params.data_dir = "/external/diskC/22P63/shotgun/SRR13061611.fastq.gz"
input_ch = Channel.fromPath("${params.data_dir}")
params.data_dir1 = "/external/diskC/22P63/shotgun/SRR13061610.fastq.gz"
input_ch1 = Channel.fromPath("${params.data_dir1}")
node_suggestion = [:] 

process fastqc {
   echo true
   input:
      path input_ch
      path input_ch1
   output:
      file ("*/*{zip,html}")
   script:
      base = input_ch.simpleName
      base1 = input_ch1.simpleName
   """
      mkdir $base
      /home/rjonker/FastQC/fastqc $input_ch --outdir $base
      /home/rjonker/FastQC/fastqc $input_ch1 --outdir $base1
      echo SLURM_JOB_ID: $SLURM_JOB_ID
      echo SLURM_JOB_NODELIST: $SLURM_JOB_NODELIST
      echo SLURM_SUBMIT_DIR: $SLURM_SUBMIT_DIR
      hostname
   """
}

printCurrentClusterStatus()

workflow {
    fastqc(input_ch, input_ch1)
}
