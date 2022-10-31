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
  cmd = "squeue"
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

def getIdealNode(nodes,state_map, file_size, possible_nodes){
  free_states = ['idle','mix']
  idles = []
  mixes = []
  busy = []
  busy_checks = [false,false,false]
  for (n : nodes) {//Gluster stores files in 2 instances on 2 seperate nodes and as such 1 node may be more ideal to use
    if (state_map[n] == 'idle') idles.add(n)
    if (state_map[n] == 'mix') mixes.add(n)
    if (!(state_map[n] in free_states)) busy.add(n)
  }
if (file_size > 100){//if the file is over 10Gb otherwise most likely more efficient to transfer data to another node for computation
    cpu_count = "sinfo -n, --node=n04 -o, --format=%c".execute().text.split('/n').toString().split()
    println "There are ${cpu_count[1]} cpu's on node " 
    node_queue_info = "squeue -w, --nodelist=n04 -o, --format=%C,%h,%L,%m,%p,%S".execute().text.split('/n')//retreive all jobs for allocated node
    for (jobs : node_queue_info) {
      line = jobs.split()
      counter = 0
      println "There are ${line.size()-1} Jobs allocated to the node" 
      if (line.size()-1 < 5){
        for(job_details : line){//Order of job details are CPU_used,Over_sbucribe,Time_left,Min_memory,Priority,Start_time
          if (counter > 0){//first line skipped as is variable headers
            line = job_details.split() 
            str = line.toString()  
            str = str.replace("[", "")
            str = str.replace("]", "")
            single_val = str.split(',')
            println "${single_val}"
            if ((single_val[0].toInteger() > cpu_count[1].toInteger()/2) || (single_val[3].replaceAll("[^\\d.]", "").toInteger() > 10)) { 
              busy_checks[counter] = false
            } else {
              busy_checks[counter] = true
            }
          }
          counter = counter + 1
        }
      } 
    }
    counter = 0
    
  } else {//use another node
    return possible_nodes
  }

  if ((busy_checks[1] == false) && (busy_checks[2] == false)){
    return possible_nodes
  } else {
    return idle
  }



  if (idles.size() > 0) {
    println "Best node/s for execution is: " + idles + ". They are idle."
    return idles
  } 
  else if (mixes.size() > 0) {
    println "Best node/s for execution is: " + mixes + ". They are mix."
    return mixes
  } 
  else if (busy.size() > 0) {//Dertermine if its worth it to process on a node thats currently busy or rather use an available node.
    for (n : busy) {
      return busy 
    }
  }

}

// Function that calls getNodesInfo & getStatus to check if there are any nodes available that have the input files data stored on it.
// There is a conditional to decide whether its best to execute on the storage nodes or not.
// This function returns the nodes to be excluded during execution set within the clusterOptions in the initial process.

def nodeOption(fname,other="") {
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
}

printCurrentClusterStatus()

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
      echo File: $cluster_option
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