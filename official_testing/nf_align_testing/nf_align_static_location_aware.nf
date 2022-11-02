#!/usr/bin/env nextflow
nextflow.enable.dsl=2

///////////////////////////////////////////////////////
// PARAMETERS AND SETUP
///////////////////////////////////////////////////////

// ===== PARAMETERS
params.outdir        = "/home/tlilford/nf-align"

// ===== assign CHANNELS
outdir               = file(params.outdir, type: 'dir')

// CRESATE OUTPUT DIRECTORY
outdir.mkdir()

///////////////////////////////////////////////////////
// LOCATION AWARE SCIENTIFC WORKFLOW FUNCTIONS - SLURM
///////////////////////////////////////////////////////

// Set the path directory to your data files as shown in the example below
// input_ch is the Channel that will input the data into the workflow processes.

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

key_fnames = file(params.outdir + "/data/*{R1,R2}.fastq.gz", size:2)
node_suggestion = [:] 

def getNodesOfBricks(fname) {
  cmd = "getfattr -n glusterfs.pathinfo -e text ${fname}";
  msg=cmd.execute().text;
  def matcher = msg =~ /(<POSIX.*)/;
  def bricks = matcher[0][0].tokenize(" ")
  nodes = []
  for (b : bricks ) {
    if (b =~ /.*arbiter.*/) continue
    matcher  = b =~ /.*:(.*):.*/; 
    node = matcher[0][1]
    matcher = node  =~ /(.*?)\..*/;
    if (matcher)
      node=matcher[0][1]
    nodes << node
  }
  return nodes
}

possible_states = ['idle','alloc','mix' ]
free_states = ['idle','mix']

def getStatus(nodes) {
  node_states ='sinfo -p batch -O NodeHost,StateCompact'.execute().text.split("\n")
  state_map = [:]
  possible  = []
  num_free  = 0
  for (n : node_states) {
    line=n.split()
    the_node=line[0]
    the_state=line[1]
    state_map[the_node]=the_state
    if  (the_state in possible_states) possible << the_node
    if  ( !(the_node in nodes)) continue;
    if  (the_state in free_states) num_free++;
  }
  return [num_free,possible]
}

def nodeOption(fname,aggression=1,other="") {
  nodes = getNodesOfBricks(fname)
  state = getStatus(nodes)
  possible=state[1]
  if ((possible.intersect(nodes)).size()<aggression)
    return "${other}"
  else {
    possible=possible - nodes;
    options="--exclude="+possible.join(',')+" ${other}"
    return options
  }
}

//node_suggestion[key_fnames.getName()]=nodeOption(key_fnames)
printCurrentClusterStatus()
key_fnames.each { node_suggestion[it.getName()]=nodeOption(it) }

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
    clusterOptions { node_suggestion[reads.getName()] }

    input:
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
        doQC(reads)
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
