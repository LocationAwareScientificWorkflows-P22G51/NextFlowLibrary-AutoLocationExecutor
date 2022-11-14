#!/usr/bin/env nextflow
nextflow.enable.dsl=2

// ===== PARAMETERS
params.outdir        = "/home/tlilford/nf-align"

// ===== assign CHANNELS
outdir               = file(params.outdir, type: 'dir')

// CRESATE OUTPUT DIRECTORY
outdir.mkdir()

///////////////////////////////////////////////////////
// LOCATION AWARE SCIENTIFC WORKFLOW FUNCTIONS - SLURM
///////////////////////////////////////////////////////

//For the purpose of testing, in order to best understand and interpret execution results the SLURM queue should be printed when this job begins executing 
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

// Function that determines on which nodes the input files are stored and the size of the file
def getNodesInfo(fname) {
  try {
    cmd = "getfattr -n glusterfs.pathinfo -e text ${fname}"
    msg = cmd.execute().text
    def matcher = msg =~ /(<POSIX.*)/
    def bricks = matcher[0][0].tokenize(" ")

    // Data storage identification
    nodes = []
    for (b : bricks ) {
      if (b =~ /.*arbiter.*/) continue
      matcher  = b =~ /.*:(.*):.*/ 
      node = matcher[0][1]
      matcher = node  =~ /(.*?)\..*/
      if (matcher)
        node = matcher[0][1]
      nodes << node
    }
    println "Data from that file is stored on the following nodes: " + nodes + "\n"

    // Finding file size
    fsize = fname.size()
    println "The file ${fname} has ${fsize} bytes" 

    return [nodes, fsize]
  }catch(Exception ex){
    println "Error: cant locate node where data resides"
  }
}

// Function that determines which nodes are available for execution
def getClusterStatus() {
  try {
    node_states = 'sinfo -p batch -O NodeHost,StateCompact'.execute().text.split("\n")
    state_map = [:]
    possible  = []
    possible_states = ['idle','mix','alloc','allocated','allocated+','completing']
    for (n : node_states) {
      line = n.split()
      the_node = line[0]
      the_state = line[1]
      state_map[the_node] = the_state
      if  (the_state in possible_states) possible << the_node
    }

    println "The following nodes are currently available for execution on the cluster: " + possible + "\n"
    return [possible, state_map]
  }catch(Exception ex){
    println "Error: cant determine possible node for execution"
  }
}

def getIdealNode(nodes,state_map, file_size,possible_nodes){
  free_states = ['idle','mix']
  idles = []
  mixes = []
  busy = []
  busy_checks = [:]
  for (n : nodes) {//Gluster stores files in 2 instances on 2 seperate nodes and as such 1 node may be more ideal to use
    if (state_map[n] == 'idle') idles.add(n)
    if (state_map[n] == 'mix') mixes.add(n)
    if (!(state_map[n] in free_states)) busy.add(n)
  }
  if (idles.size() > 0) {
    println "Best node/s for execution is: " + idles + ". They are idle."
    return idles
  } 
  else if (mixes.size() > 0) {
    println "Best node/s for execution is: " + mixes + ". They are mix."
    return mixes
  } 
  else {//Dertermine if its worth it to process on a node thats currently busy or rather use an available node.
    try {
      for (n : busy) {
        is_busy = false
        if (file_size > 50000000000){//if the file is less than 5Gb most likely more efficient to transfer data to another node for computation
          cpu_count = "sinfo -n, --node=$n -o, --format=%c".execute().text.split('/n').toString().split()
          println "There are ${cpu_count[1]} cpu's on node $n" 
          node_queue_info = "squeue -w, --nodelist=$n -o, --format=%C,%h,%L,%m,%p,%S".execute().text.split('/n')//retreive all jobs for allocated node
          for (jobs : node_queue_info) {
            line = jobs.split()
            counter = 0
            println "There are ${line.size()-1} Jobs allocated to the node" 
            if (line.size()-1 < 3){//if there are 3 jobs queued use another node
              for(job_details : line){//Order of job details are CPU_used,Over_sbucribe,Time_left,Min_memory,Priority,Start_time
                if (counter > 0){//first line skipped as is variable headers
                  line = job_details.split() 
                  str = line.toString()  
                  str = str.replace("[", "")
                  str = str.replace("]", "")
                  single_val = str.split(',')
                  println "${single_val}"
                  single_val[3].replaceAll("G", "000")
                  if ((single_val[0].toInteger() > cpu_count[1].toInteger()/2) || (single_val[3].replaceAll("[^\\d.]", "").toInteger() > 10000)) {  
                    //in the case more than half cpu's in use and min RAM is over 10000MB
                    println "Job is large"
                    is_busy = true
                  } else {
                    println "Job is small"  
                  }
                }
                counter = counter + 1
              }
            } else {
              is_busy = true
            } 
          }
        } else {//use another node
         return (possible_nodes - busy)
        }
      if (is_busy == false){
        println "WAITING to use node with data" 
        return n
      } 
      }
    } catch(Exception ex) {
      println "ERROR: node is too busy, SLURM scheduler is to choose nodes from those possible"
      return (possible_nodes - busy)
    }
    println "Node is too busy, utilising another node"
    return (possible_nodes - busy)
  }
}

// Function that calls getNodesInfo & getStatus to check if there are any nodes available that have the input files data stored on it.
// There is a conditional to decide whether its best to execute on the storage nodes or not.
// This function returns the nodes to be excluded during execution set within the clusterOptions in the initial process.

def nodeOption(fname,other="") {
  location = "hostname".execute().text
  println  "LOCATION IS FOUND using $location"
  try {
    node_location = getNodesInfo(fname)[0]
    file_size = getNodesInfo(fname)[1]
    possible_nodes = getClusterStatus()[0]
    state_map = getClusterStatus()[1]
    ideal_node = getIdealNode(nodes,state_map, file_size, possible_nodes)
    if ((possible_nodes.intersect(nodes)).size()<1)
    {
      println "The job is executed regardless of location as the amount of available nodes that have the data stored on them is less than "
      return "${other}"
    }
    else {
      possible = possible_nodes - ideal_node;
      options="--exclude="+possible.join(',')+" ${other}"
      println "Job execution can occur on the available storage nodes. \nThe following nodes should be excluded during execution: " + options + "\n"
      return options
    }
  }catch(Exception ex){
    println "Error: cant determine cluster options"
    return other
  }
}

printCurrentClusterStatus()

///////////////////////////////////////////////////////
// WORKFLOW PROCESSES
///////////////////////////////////////////////////////

process downloadImages {
    tag { "download_images" }
    
    input:
    each image

    """
    apptainer pull --force --dir /home/tlilford/nf-align-cont/ docker://phelelani/nf-rnaseqcount:${image}
    """
}

process downloadDdata {
    tag { "dawnload_data" }
    publishDir "${outdir}/data", mode: 'move', overwrite: true

    output:
    tuple val('data'), path("*"), emit: data
    
    """
    lftp -e 'mirror -c --use-pget-n=10 http://h3data.cbio.uct.ac.za/assessments/RNASeq/practice/dataset/ .; exit'

    lftp -e 'pget -n10 ftp://ftp.ensembl.org/pub/release-68/fasta/mus_musculus/dna/Mus_musculus.GRCm38.68.dna.toplevel.fa.gz; exit'
    lftp -e 'pget -n10 ftp://ftp.ensembl.org/pub/release-68/gtf/mus_musculus/Mus_musculus.GRCm38.68.gtf.gz; exit'

    mv Mus_musculus.GRCm38.68.dna.toplevel.fa.gz genome.fa.gz
    mv Mus_musculus.GRCm38.68.gtf.gz genes.gtf.gz

    gunzip genome.fa.gz
    gunzip genes.gtf.gz
    """
}

process indexRef {
    tag { 'index_ref' }
    publishDir "${outdir}/data", mode: 'move', overwrite: true
    
    output:
    tuple val("starIndex"), path("*"), emit: star_index

    """
    STAR --runThreadN ${task.cpus} \
        --runMode genomeGenerate \
        --genomeDir . \
        --genomeFastaFiles ${genome} \
        --sjdbGTFfile ${genes} \
        --sjdbOverhang 99 
    """
}

process doQC {
    tag { sample }
    publishDir "${outdir}/results_qc", mode: 'copy', overwrite: true
    
    echo true
    clusterOptions {nodeOption(cluster_option)}

    input:
    val cluster_option
    tuple val(sample), path(reads)
    
    output:
    tuple val(sample), path("${sample}*.html"), path("${sample}*.zip"), emit: qc_out
    
    """
    /home/tlilford/FastQC/fastqc ${reads.findAll().join(' ') } --threads ${task.cpus} --noextract
    echo File: $cluster_option
    hostname
    """
}

process doAlignment {
    tag { sample }
    memory '50 GB'
    cpus 12
    publishDir "${outdir}/results_alignment", mode: 'copy', overwrite: true

    input:
    tuple val(sample), path(reads)
    
    output:
    tuple val(sample), path("${sample}*.{out,tab}"), path("${sample}_Aligned.out.bam"), emit: qc_out

    """
    STAR --runMode alignReads \
        --genomeDir ${outdir}/data/ \
        --readFilesCommand gunzip -c \
        --readFilesIn ${reads.findAll().join(' ')} \
        --runThreadN ${task.cpus} \
        --outSAMtype BAM Unsorted \
        --outSAMunmapped Within \
        --outSAMattributes Standard \
        --outFileNamePrefix ${sample}_
    """
}

///////////////////////////////////////////////////////
// WORKFLOW ENTRY POINT
///////////////////////////////////////////////////////

workflow PREP_DATA {
    take:
        images
    main:
        downloadImages(images)
        downloadDdata()
}

workflow INDEX_REF {
    main:
        indexRef()
}

workflow RUN_ALIGNMENT {
    take:
        reads
    main:
        doQC(Channel.fromFilePairs(params.outdir + "/data/*{R1,R2}.fastq.gz", size:2).map{it.toAbsolutePath()}, reads)
        doAlignment(reads)
}

// WORKFLOW DATA
images = ["star", "bowtie2", "fastqc"]
// images = ["star"]
genome = file(params.outdir + '/data/genome.fa', type: 'file')
genes  = file(params.outdir + '/data/genes.gtf', type: 'file')
reads  = Channel.fromFilePairs(params.outdir + "/data/*{R1,R2}.fastq.gz", size:2)

// PICK AND CHOOSE
workflow {
    mode = params.mode
    switch (mode) {
        case['prep.data']:
            PREP_DATA(images)
            break
            // =====
        case['index.ref']:
            INDEX_REF()
            break
            // =====
        case['do.alignment']:
            RUN_ALIGNMENT(reads)
            break
            // =====
        case['do.all']:
            PREP_DATA(images)
            INDEX_REF()
            RUN_ALIGNMENT(reads)
            break
            // =====
        default:
            exit 1, """
OOOPS!! SEEMS LIE WE HAVE A WORFLOW ERROR!

No workflow \'mode\' give! Please use one of the following options for workflows:
    --mode prep.data    // To download containers, reference genome, reference annotation and reads
    --mode index.ref    // To index the reference genome
    --mode do.alignment // To align the reads to the reference
    --mode all          // To run all workflows
"""
            break
    }
}
