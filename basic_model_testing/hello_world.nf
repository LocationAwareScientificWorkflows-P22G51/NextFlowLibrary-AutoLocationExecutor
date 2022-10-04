#!/usr/bin/env nextflow

params.str = 'hello world!'

process splitLetters {
        output:
        path 'chunk_*'

        """
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
