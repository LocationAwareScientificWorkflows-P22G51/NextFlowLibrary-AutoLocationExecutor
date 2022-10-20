#!/usr/bin/env nextflow

nextflow.enable.dsl=2



// Function that is called on the subscibe observing event whenever the input channel transfers data

//
//
//
//
//
// Workflow code starts here
// Only addition within your workflow code is that within the initial process clusterOptions needs to be set as below


process fastqc {
   echo true
   maxForks params.forks
   clusterOptions {nodeOption(cluster_option)}
   input:
      val cluster_option
      path f
   output:
      file ("*/*{zip,html}")
   script:
      base = f.simpleName
   """
      mkdir $base
      /home/tlilford/FastQC/fastqc $f --outdir $base
       echo SLURM_JOB_ID: $SLURM_JOB_ID
       echo SLURM_JOB_NODELIST: $SLURM_JOB_NODELIST
       echo SLURM_SUBMIT_DIR: $SLURM_SUBMIT_DIR
       echo SLURM_JOB_NUM_NODES: $SLURM_JOB_NUM_NODES
       echo SLURM_CLUSTER_NAME: $SLURM_CLUSTER_NAME
       echo File_path: $cluster_option
       hostname
   """
}

//Channel.fromPath(params.input) 

workflow {
    data = Channel.fromPath(params.input) 
    fastqc((params.input, data)
}