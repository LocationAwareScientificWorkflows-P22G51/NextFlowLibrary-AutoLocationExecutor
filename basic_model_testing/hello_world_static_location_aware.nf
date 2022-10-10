#!/usr/bin/env nextflow

params.str = 'hello world!'

process splitLetters {

        echo true
        
        clusterOptions {"--exclude=n07,n12,n13,n24,n30,n03,n04,n05,n11,n16,n17,n18,n19,n20,n23,n25,n26,n27,n31,n33,n34,n35,n36,n37,n41" }
    
        output:
        path 'chunk_*'
//echo sstat -j $SLURM_JOB_ID
//echo sstat -j $SLURM_NODELIST
        """
        
        
        echo $hostname
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


