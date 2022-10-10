// common code that must be included starts here



// This should be the files you want to use as determining the node allocations. Can contain other files (a small performance
// penalty but only minor but should contain all that you want
//key_fnames = file("/external/diskC/22P63/data1/*.bim")
key_fnames = file("*.bim")

node_suggestion = [:]

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
  println "\nThe following data file and its storage nodes will be analysed: " + fname + "\n"
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

key_fnames.each { node_suggestion[it.getName()]=nodeOption(it) }
//println node_suggestion

// sample code that you should use as a template


params.str = 'Hello world!'

// use the node_suggestion hash map to find where the process should run
// NB: node_suggestion takes a string as an input type so we need to run .getName() on the input file
// Recall that the file itself is not staged at the point clusterOptions is called
//node_suggestion[filelocaion_ch[0].getName()]
 process sample {
     echo true
     clusterOptions { "--exclude=n07,n12,n13,n24,n30,n03,n04,n05,n11,n16,n17,n18,n19,n20,n23,n25,n26,n27,n31,n33,n34,n35,n36" }
     input:
      path filelocaion_ch
     output:
      path 'chunk_*'

  """
  hostname
  printf '${params.str}' | split -b 6 - chunk_
  """
}

process convertToUpper {
   //clusterOptions { node_suggestion[bams.getName()] }
  input:
   //path bams
    file x
  output:
    stdout

  """
  cat $x | tr '[a-z]' '[A-Z]'
  """
}


workflow {
   Channel.fromPath("*.bim") | sample | flatten | convertToUpper | view { it.trim() }
}

