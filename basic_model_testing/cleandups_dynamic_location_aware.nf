#!/usr/bin/env nextflow

// common code that must be included starts here
// This should be the files you want to use as determining the node allocations. Can contain other files (a small performance
// penalty but only minor but should contain all that you want

params.data_dir = "/external/diskC/22P63/data1"
key_fnames = file("/external/diskC/22P63/data1/11.bim")
node_suggestion = Channel.fromList()
input_ch = Channel.fromPath("${params.data_dir}/*.bim")




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
  println "\n The following data file and its storage nodes will be analysed: " + fname + "\n"
  println "Data from that file is stored on the following nodes: " + nodes + "\n"
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
  println "The following nodes are currently available for execution: " + possible + "\n"
  return [num_free,possible]
}




def nodeOption(fname,aggression=1,other="") {
  nodes = getNodesOfBricks(fname)
  state = getStatus(nodes)
  possible=state[1]
  if ((possible.intersect(nodes)).size()<aggression)
  {
    println "The job is executed regardless of location as the amount of available nodes that have the data stored on them is less than " + aggression + "\n"
    return "${other}"
  }
  else {
    possible=possible - nodes;
    options="--exclude="+possible.join(',')+" ${other}"
    println "Job execution can occur on the available storage nodes. The following nodes should be excluded during execution: " + options + "\n"
    return options
  }
}

//key_fnames.each { node_suggestion[it.getName()]=nodeOption(it) }

// sample code that you should use as a template
// use the node_suggestion hash map to find where the process should run
// NB: node_suggestion takes a string as an input type so we need to run .getName() on the input file
// Recall that the file itself is not staged at the point clusterOptions is called

process getIDs {
    input:
       //stdin node_suggestion
       file input_ch
    output:
       path "${input_ch.baseName}.ids", emit:  id_ch
       path "$input_ch", emit: orig_ch
    script:
       "cut -f 2 $input_ch | sort > ${input_ch.baseName}.ids"     
}

process getDups {
    input:
       path input
    output:
       path "${input.baseName}.dups" , emit: dups_ch
    script:
       out = "${input.baseName}.dups"
       """
       uniq -d $input > $out
       touch ignore
       """
}


process removeDups {
    input:
       path badids 
       path "orig.bim" 
    output:
       path "${badids.baseName}.bim", emit: cleaned_ch
    publishDir "output", pattern: "${badids.baseName}.bim",\
                  overwrite:true, mode:'copy'

    script:
       "grep -v -f $badids orig.bim > ${badids.baseName}.bim "
}

process splitIDs  {
    input:
       path bim
    each split
    output:
       path ("*-$split-*") 

    script:
    "split -l $split $bim ${bim.baseName}-$split- "
}


 process sample {
     input:
      each key_fnames
     output:
      val nodeSuggestion, emit: nodeSuggestion
     script:
     nodeSuggestion  = nodeOption(key_fnames)
      """
      echo 'Finding ${key_fnames.getName()}' 
      """
}


//input_ch.subscribe {node_suggestion << nodeOption(it)}
//input_ch.subscribe {println it.getName()}

//node_suggestion.subscribe {println it}




workflow {
   split = [400,500,600]
   sample(key_fnames)
   getIDs(input_ch)
   getDups(getIDs.out.id_ch)
   removeDups(getDups.out.dups_ch, getIDs.out.orig_ch)
   splitIDs(removeDups.out.cleaned_ch, split)
}

