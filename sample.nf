// Cealn duplicatews code
input_ch = Channel.fromPath("/external/diskC/22P63/11.bim")

process getIDs {
    input:
       file input from input_ch
       path result
    output:
       file "ids" into id_ch
       file "11.bim" into orig_ch
    script:
       " cut -f 2 $input | sort > ids "
}

process getDups {
    input:
       file input from id_ch
    output:
       file "dups" into dups_ch
    script:
       """
       uniq -d $input > dups 
       touch ignore
       """
}


process removeDups {
    input:
       file badids  from dups_ch
       file orig    from orig_ch
    output:
       file "clean.bim" into output
    script:
       "grep -v -f $badids $orig > clean.bim "
}


output.subscribe { print "Done!" }




// common code that must be included starts here



// This should be the files you want to use as determining the node allocations. Can contain other files (a small performance
// penalty but only minor but should contain all that you want
key_fnames = file("/external/diskC/22P63/*.bim")


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



// common code ends

// sample code that you should use as a template

bams = Channel.fromFilePairs("$src/*{.bim,.bim.bai}", size:2)
	      .map { [it[0],it[1][0], it[1][1]] }
        .randomSample(1000)
        



// use the node_suggestion hash map to find where the process should run
// NB: node_suggestion takes a string as an input type so we need to run .getName() on the input file
// Recall that the file itself is not staged at the point clusterOptions is called
 process sample {
     clusterOptions { node_suggestion[bam.getName()] }
     input:
        tuple sample, file(bam), file(bai) from bams
  output:
  path 'finishSulrm.txt' 

  """
  echo "SLLLUURRMM" > finishSulrm.txt
  """
}

output.subscribe { print "Done!" }
