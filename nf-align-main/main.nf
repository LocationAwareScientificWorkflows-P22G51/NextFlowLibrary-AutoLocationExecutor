#!/usr/bin/env nextflow
nextflow.enable.dsl=2

// ===== PARAMETERS
params.outdir        = "/home/phelelani/projects/slurm_test"

// ===== assign CHANNELS
outdir               = file(params.outdir, type: 'dir')

// CRESATE OUTPUT DIRECTORY
outdir.mkdir()

// ===== START PIPELINE

process downloadImages {
    tag { "download_images" }
    
    input:
    each image

    """
    singularity pull --force --dir \$HOME/.singularity/cache/ docker://phelelani/nf-rnaseqcount:${image} 
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
    
    input:
    tuple val(sample), path(reads)
    
    output:
    tuple val(sample), path("${sample}*.html"), path("${sample}*.zip"), emit: qc_out
    
    """
    fastqc ${reads.findAll().join(' ') } --threads ${task.cpus} --noextract
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
