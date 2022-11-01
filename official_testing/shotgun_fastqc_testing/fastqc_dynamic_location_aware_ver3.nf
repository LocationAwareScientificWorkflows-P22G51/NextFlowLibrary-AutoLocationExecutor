#!/usr/bin/env nextflow

///////////////////////////////////////////////////////
// PARAMETERS AND SETUP
///////////////////////////////////////////////////////

params.data_dir = "/external/diskC/22P63/shotgun/*.gz"// Set the path directory to your data files
input_ch = Channel.fromPath("${params.data_dir}")// Input_ch is the Channel that will input the data into the workflow processes.

///////////////////////////////////////////////////////
// LOCATION AWARE SCIENTIFC WORKFLOW FUNCTIONS - SLURM
///////////////////////////////////////////////////////

//For the purpose of testing, in order to best understand and interpret execution results the SLURM queue should be printed when this job begins executing 
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

// Function that determines on which nodes the input files are stored and the size of the file
def getNodesInfo(fname) {
  try {
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
   // println "Data from that file is stored on the following nodes: " + nodes + "\n"

    // Finding file size
    fsize = fname.size()
    //println "The file ${fname} has ${fsize} bytes" 

    return [nodes, fsize]
  }catch(Exception ex){
    println "Error: cant locate node where data resides"
  }
}

// Function that determines which nodes are available for execution
def getClusterStatus() {
  try {
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

    //println "The following nodes are currently available for execution on the cluster: " + possible + "\n"
    return [possible, state_map]
  }catch(Exception ex){
    println "Error: cant determine possible node for execution"
  }
}

def getIdealNode(nodes,state_map, file_size,possible_nodes){
  free_states = ['idle','mix']
  idles = []
  mixes = []
  busy = []
  busy_checks = [:]
  for (n : nodes) {//Gluster stores files in 2 instances on 2 seperate nodes and as such 1 node may be more ideal to use
    if (state_map[n] == 'idle') idles.add(n)
    if (state_map[n] == 'mix') mixes.add(n)
    if (!(state_map[n] in free_states)) busy.add(n)
  }
  if (idles.size() > 0) {
    //println "Best node/s for execution is: " + idles + ". They are idle."
    return idles
  } 
  else if (mixes.size() > 0) {
    //println "Best node/s for execution is: " + mixes + ". They are mix."
    try {
      for (n : mixes) {
        
        is_busy = false
        if (file_size > 300000000){//if the file is less than 0.3Gb most likely more efficient to transfer data to another node for computation
          cpu_count = "sinfo -n, --node=$n -o, --format=%c".execute().text.split('/n').toString().split()
          //println "There are ${cpu_count[1]} cpu's on node $n" 
          node_queue_info = "squeue -w, --nodelist=$n -o, --format=%C,%h,%L,%m,%p,%S".execute().text.split('/n')//retreive all jobs for allocated node
          for (jobs : node_queue_info) {
            line = jobs.split()
            counter = 0
            //println "There are ${line.size()-1} Jobs allocated to the node" 
            if (line.size()-1 < 3){//if there are 3 jobs queued use another node
              for(job_details : line){//Order of job details are CPU_used,Over_sbucribe,Time_left,Min_memory,Priority,Start_time
                if (counter > 0){//first line skipped as is variable headers
                  line = job_details.split() 
                  str = line.toString()  
                  str = str.replace("[", "")
                  str = str.replace("]", "")
                  single_val = str.split(',')
                  //println "${single_val}"
                  single_val[3].replaceAll("G", "000")
                  if ((single_val[0].toInteger() > cpu_count[1].toInteger()/2) || (single_val[3].replaceAll("[^\\d.]", "").toInteger() > 10000)) {  
                    //in the case more than half cpu's in use and min RAM is over 10000MB
                    //println "Job is large"
                    println "________________________JOBLARGE______________________________"
                    is_busy = true
                  } else {
                    //println "Job is small"  
                  }
                }
                counter = counter + 1
              }
            } else {
               println "________________________QUEUEBIG______________________________"
              is_busy = true
            } 
          }
        } else {//use another node
        println "________________________mixFileSize______________________________"
         return ("")
        }
      if (is_busy == false){
        //println "WAITING to use node with data" 
        println "________________________mix______________________________"
        return n
      } else {
        println "________________________MixNotWorth______________________________"
        return ("")
      } 
      }
    } catch(Exception ex) {
      println "ERROR: node is too busy, SLURM scheduler is to choose nodes from those possible"
      return ("")
    }
  } 
  else {//Dertermine if its worth it to process on a node thats currently busy or rather use an available node.
    try {
      for (n : busy) {     
        is_busy = false
        if (file_size > 300000000){//if the file is less than 0.3Gb most likely more efficient to transfer data to another node for computation
          cpu_count = "sinfo -n, --node=$n -o, --format=%c".execute().text.split('/n').toString().split()
          //println "There are ${cpu_count[1]} cpu's on node $n" 
          node_queue_info = "squeue -w, --nodelist=$n -o, --format=%C,%h,%L,%m,%p,%M".execute().text.split('/n')//retreive all jobs for allocated node
          for (jobs : node_queue_info) {
            line = jobs.split()
            counter = 0
            //println "There are ${line.size()-1} Jobs allocated to the node" 
            if (line.size()-1 < 3){//if there are 3 jobs queued use another node
              for(job_details : line){//Order of job details are CPU_used,Over_sbucribe,Time_left,Min_memory,Priority,TimeUsed
                if (counter > 0){//first line skipped as is variable headers
                  line = job_details.split() 
                  str = line.toString()  
                  str = str.replace("[", "")
                  str = str.replace("]", "")
                  single_val = str.split(',')
                  //println "${single_val}"
                  single_val[3].replaceAll("G", "000")
                  if ((single_val[0].toInteger() > cpu_count[1].toInteger()/2) || (single_val[3].replaceAll("[^\\d.]", "").toInteger() > 10000) || (single_val[5].length() > 4) ) {  
                    //in the case more than half cpu's in use and min RAM is over 10000MB
                    //println "Job is large"
                    println "________________________JOBLARGE______________________________"
                    is_busy = true
                  } else {
                    //println "Job is small"  
                  }
                }
                counter = counter + 1
              }
            } else {
              println "________________________QUEUEBIG______________________________"
              is_busy = true
            } 
          }
        } else {//use another node
        println "________________________allocFIleSIze______________________________"
         return ("")
        }
      if (is_busy == false){
        //println "WAITING to use node with data" 
         println "________________________alloc______________________________"
        return n
      }else{
        println "________________________AllocNotWorth______________________________"
        return ("")
      } 
      }
    } catch(Exception ex) {
      println "ERROR: node is too busy, SLURM scheduler is to choose nodes from those possible"
      return ("")
    }    
  }
   println "________________________UNSURE______________________________"
  return ("")
}

// Function that calls getNodesInfo & getStatus to check if there are any nodes available that have the input files data stored on it.
// There is a conditional to decide whether its best to execute on the storage nodes or not.
// This function returns the nodes to be excluded during execution set within the clusterOptions in the initial process.

def nodeOption(fname,other="") {
  //location = "hostname".execute().text
  //println  "LOCATION IS FOUND using $location"
  //try {
    node_location = getNodesInfo(fname)[0]
    file_size = getNodesInfo(fname)[1]
    possible_nodes = getClusterStatus()[0]
    state_map = getClusterStatus()[1]
    ideal_node = getIdealNode(nodes,state_map, file_size, possible_nodes)
    if (((possible_nodes.intersect(nodes)).size()<1 )|| (ideal_node == "" ))
    {
      //println "The job is executed regardless of location as the amount of available nodes that have the data stored on them is less than "
      return "${other}"
    }
    else {
      possible = possible_nodes - ideal_node;
      options="--exclude="+possible.join(',')+" ${other}"
      //println "Job execution can occur on the available storage nodes. \nThe following nodes should be excluded during execution: " + options + "\n"
      return options
    }
  // }catch(Exception ex){
  //   println "Error: cant determine cluster options"
  //   return other
  // }
}

printCurrentClusterStatus()

///////////////////////////////////////////////////////
// WORKFLOW PROCESSES
///////////////////////////////////////////////////////

// Workflow code starts here
// Only addition within your workflow code is that of clusterOptions which needs to be set as below
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
    fastqc(Channel.fromPath("${params.data_dir}").map{it.toAbsolutePath()}, input_ch)
}