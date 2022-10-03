// common code that must be included starts here



// This should be the files you want to use as determining the node allocations. Can contain other files (a small performance
// penalty but only minor but should contain all that you want
key_fnames = file("/external/diskC/22P63/data1/*.bim")


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
  return [num_free,possible]
}




def nodeOption(fname,aggression=1,other="") {
  nodes = getNodesOfBricks(fname)
  state = getStatus(nodes)
  possible=state[1]
  if ((possible.intersect(nodes)).size()<aggression)
    return "${other}"
  else {
    possible=possible - nodes;
    options="--exclude="+possible.join(',')+" ${other}"
    return options
  }
}

key_fnames.each { node_suggestion[it.getName()]=nodeOption(it) }
println node_suggestion

// sample code that you should use as a template


params.str = 'Hello world!'

// use the node_suggestion hash map to find where the process should run
// NB: node_suggestion takes a string as an input type so we need to run .getName() on the input file
// Recall that the file itself is not staged at the point clusterOptions is called
 process sample {
     clusterOptions { node_suggestion[filelocaion_ch[0].getName()] }
     input:
      path filelocaion_ch
     output:
      path 'chunk_*'

  """
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
   Channel.fromPath("/external/diskC/22P63/data1/*.bim") | sample | flatten | convertToUpper | view { it.trim() }
}



