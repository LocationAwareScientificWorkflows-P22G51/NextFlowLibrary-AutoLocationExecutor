nextflow.enable.dsl=2


process fastqc {
   maxForks params.forks
   input:
      path f
   output:
      file ("/{zip,html}")
   script:
      base = f.simpleName
   """
      mkdir $base
      ./FastQC/fastqc $f --outdir $base
   """
}


Channel.fromPath(params.input) 



workflow {
    data = Channel.fromPath(params.input) 
    fastqc(data)
}