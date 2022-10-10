#!/usr/bin/env nextflow

params.str = 'hello world!'

process splitLetters {

        echo true
        
        clusterOptions "-S /bin/bash -q all.q@compute-0-n08" 
    
        output:
        path 'chunk_*'
//echo sstat -j $SLURM_JOB_ID
//echo sstat -j $SLURM_NODELIST
        """
        hostname
        printf '${params.str}' | split -b 6 - chunk_
        """
}

process convertToUpper {
        input:
        file x
        output:
        stdout
        """
        cat $x | tr '[a-z]' '[A-Z]'
        """
}

workflow{
        splitLetters | flatten | convertToUpper | view {it.trim()}
}


