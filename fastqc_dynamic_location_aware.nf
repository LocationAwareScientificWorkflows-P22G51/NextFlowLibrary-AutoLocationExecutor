#!/usr/bin/env nextflow

// Set the path directory to your data files as shown in the example below
// input_ch is the Channel that will input the data into the workflow processes.

params.data_dir = "/external/diskC/22P63/shotgun/*gz"
input_ch = Channel.fromPath("${params.data_dir}")
node_suggestion = [:] 
    
// Function that determines on which nodes the input files are stored and determines the weighting coefficient based on the file size
// The weighting coefficient is used later to determine if its viable to execute on the storage nodes or not

def getNodesInfo(fname) {

  // file configuration
  cmd = "getfattr -n glusterfs.pathinfo -e text ${fname}";
  msg=cmd.execute().text;
  def matcher = msg =~ /(<POSIX.*)/;
  def bricks = matcher[0][0].tokenize(" ")

  // weighting setting
  fsize = fname.size()
  weighting = 1
  cluster_speed = 100000   // 100kbs transfer speed
  time_limit = 1           // time limit of 1 second for data transfer
  if (fsize > cluster_speed * time_limit )   // example where range of 100 kb is the limiter
     weighting += 1
  println "The file ${fname} has ${fsize} bytes, thus the node weighting is set ${weighting}" 

  // data storage identification
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
  println "Data from that file is stored on the following nodes: " + nodes + "\n"
  
  return [nodes,weighting]
}

// Function that determines which nodes are currently available for processing and their respective states

def getStatus(nodes) {
  node_states ='sinfo -p batch -O NodeHost,StateCompact'.execute().text.split("\n")
  state_map = [:]
  possible  = []
  num_free  = 0
  possible_states = ['idle','alloc','mix' ]
  free_states = ['idle','mix']
  for (n : node_states) {
    line=n.split()
    the_node=line[0]
    the_state=line[1]
    state_map[the_node]=the_state
    if  (the_state in possible_states) possible << the_node
    if  ( !(the_node in nodes)) continue;
    if  (the_state in free_states) num_free++;
  }
  println "The following nodes are currently available for execution on the cluster: " + possible + "\n"
  return [num_free,possible,state_map]
}

// Function to identify which of the nodes that have the data stored on them are the best suited to execute on

def getBestNode(nodes,state_map) {
   idles = []
   mixes = []
   allocs = []
   for (n : nodes) {
      if (state_map[n] == 'idle') idles.add(n)
      if (state_map[n] == 'mix') mixes.add(n)
      if (state_map[n] == 'alloc') allocs.add(n)
   }
   if (idles.size() > 0) {
      println "Best node/s for execution is: " + idles + ". They are idle."
      return idles
   } 
   else if (mixes.size() > 0) {
      println "Best node/s for execution is: " + mixes ". They are mix."
      return mixes
   } 
   else {
      println "Best node/s for execution is: " + allocs ". They are allocs."
      return allocs
   } 
}

// Function that calls getNodesInfo & getStatus to check if there are any nodes available that have the input files data stored on it.
// There is a conditional to decide whether its best to execute on the storage nodes or not.
// This function returns the nodes to be excluded during execution set within the clusterOptions in the initial process.

def nodeOption(fname,other="") {
  info = getNodesInfo(fname)
  state = getStatus(nodes)
  nodes = info[0]
  weighting = info[1]
  possible=state[1]
  state_map=state[2]
  best_node = getBestNode(nodes,state_map)
  if ((possible.intersect(nodes)).size()<weighting)
  {
    println "The job is executed regardless of location as the amount of available nodes that have the data stored on them is less than " + weighting + "\n"
    return "${other}"
  }
  else {
    possible=possible - nodes;
    options="--exclude="+possible.join(',')+" ${other}"
    println "Job execution can occur on the available storage nodes. \nThe following nodes should be excluded during execution: " + options + "\n"
    return options
  }
}

// Function that is called on the subscibe observing event whenever the input channel transfers data

def updateNodes(it) {
   println "\nUpdating node suggestion for: $it"
   node_suggestion[it.getName()]=nodeOption(it)   
}

//
//
//
//
//
// Workflow code starts here
// Only addition within your workflow code is that within the initial process clusterOptions needs to be set as below
// Take note of the workflow execution, use as is for the initial process

process fastqc {
   clusterOptions {nodeOption(cluster_option)}
   input:
      val cluster_option
      path input_ch
   output:
      file ("/{zip,html}")
   script:
      base = f.simpleName
   """
      mkdir $base
      fastqc $input_ch --outdir $base
      hostname
   """
}

workflow {
    fastqc(Channel.fromPath("${params.data_dir}").map{it.toAbsolutePath()}, input_ch)
}