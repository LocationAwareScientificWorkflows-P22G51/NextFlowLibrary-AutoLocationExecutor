#!/usr/bin/env nextflow

///////////////////////////////////////////////////////
// PARAMETERS AND SETUP
///////////////////////////////////////////////////////

params.data_dir = "/external/diskC/22P63/shotgun/SRR13061610.fastq.gz"// Set the path directory to your data files
input_ch = Channel.fromPath("${params.data_dir}")// Input_ch is the Channel that will input the data into the workflow processes.

///////////////////////////////////////////////////////
// LOCATION AWARE SCIENTIFC WORKFLOW FUNCTIONS - SLURM
///////////////////////////////////////////////////////

//For the purpose of testing, in order to best understand and interpret execution results the SLURM queue should be printed when this job begins executing 
def printCurrentClusterStatus(){
  cmd = "squeue -o %all"
  queue_status = cmd.execute().text
  cmd = "sinfo"
  node_status = cmd.execute().text
  println "${queue_status}" + "\n"
  println "${node_status}" + "\n"
}

// Function that determines on which nodes the input files are stored and the size of the file
def getNodesInfo(fname) {
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
}

// Function that determines which nodes are available for execution
def getClusterStatus() {
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
}

def getIdealNode(nodes,state_map){
  free_states = ['idle','mix']

  if !(state_map[nodes] in free_states){
    cmd = "squeue -w, --nodelist=${the_node}"
    node_queue_info = cmd.execute().text.split("\n");
    println "${node_queue_info}"
  }

  return the_node
}

// Function that calls getNodesInfo & getStatus to check if there are any nodes available that have the input files data stored on it.
// There is a conditional to decide whether its best to execute on the storage nodes or not.
// This function returns the nodes to be excluded during execution set within the clusterOptions in the initial process.

def nodeOption(fname,other="") {
  node_location = getNodesInfo(fname)[0]
  file_size = getNodesInfo(fname)[1]
  possible_nodes = getClusterStatus()[0]
  state_map = getClusterStatus()[1]

  ideal_node = getIdealNode(nodes,state_map)

  if ((possible_nodes.intersect(nodes)).size()<100)
  {
    println "The job is executed regardless of location as the amount of available nodes that have the data stored on them is less than " + weighting + "\n"
    return "${other}"
  }
  else {
    possible = possible_nodes - ideal_node;
    options="--exclude="+possible.join(',')+" ${other}"
    println "Job execution can occur on the available storage nodes. \nThe following nodes should be excluded during execution: " + options + "\n"
    return options
  }
}

///////////////////////////////////////////////////////
// WORKFLOW PROCESSES
///////////////////////////////////////////////////////


// Workflow code starts here
// Only addition within your workflow code is that within the initial process clusterOptions needs to be set as below
// Take note of the workflow execution, use as is for the initial process

process fastqc {
   echo true
   clusterOptions {nodeOption(cluster_option)}
   input:
      val cluster_option
      path input_ch
   output:
      file ("*/*{zip,html}")
   script:
      base = input_ch.simpleName
   """
      mkdir $base
      /home/tlilford/FastQC/fastqc $input_ch --outdir $base
      hostname
   """
}

///////////////////////////////////////////////////////
// WORKFLOW ENTRY POINT
///////////////////////////////////////////////////////

workflow {
   Channel.fromPath("${params.data_dir}").map{it.toAbsolutePath()}.view()
    fastqc(Channel.fromPath("${params.data_dir}").map{it.toAbsolutePath()}, input_ch)
}