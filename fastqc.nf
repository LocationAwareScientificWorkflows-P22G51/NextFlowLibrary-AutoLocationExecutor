nextflow.enable.dsl=2


process fastqc {
   input:
      path f
   output:
      file ("*/*{zip,html}")
   script:
      base = f.simpleName
   """
      mkdir $base
      /home/rjonker/FastQC/fastqc $f --outdir $base
   """
}


Channel.fromPath(params.input) 



workflow {
    data = Channel.fromPath(params.input) 
    fastqc(data)
}
