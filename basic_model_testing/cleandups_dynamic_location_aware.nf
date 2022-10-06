#!/usr/bin/env nextflow

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

def updateNodes(it) {
    //node_suggestion.bind(nodeOption(it))
    println "Got: $it"
}

params.data_dir = "/external/diskC/22P63/data1/11.bim"
node_suggestion = [:] 
input_ch = Channel
        .fromPath("${params.data_dir}")        
        .randomSample(1000)
        .subscribe onNext: { updateNodes(it) }, onComplete: { println 'Done' }

process getIDs {
    echo true
    clusterOptions { node_suggestion }
    input:
       val node_suggestion
       file input_ch
    output:
       path "${input_ch.baseName}.ids", emit:  id_ch
       path "$input_ch", emit: orig_ch
    script:
       """
      echo sstat -j $SLURM_JOB_ID
      echo sstat -j $SLURM_NODELIST
      cut -f 2 $input_ch | sort > ${input_ch.baseName}.ids
      """    
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

workflow {
   split = [400,500,600]
   getIDs(node_suggestion, input_ch)
   getDups(getIDs.out.id_ch)
   removeDups(getDups.out.dups_ch, getIDs.out.orig_ch)
   splitIDs(removeDups.out.cleaned_ch, split)
}
