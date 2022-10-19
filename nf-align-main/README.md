# nf-align

### 1.1 Download/clone the workflow
```
nextflow pull phelelani/nf-align
```
#### Workflow running options:
```
--mode       STRING    To specify which step of the workflow you are running.
                       Available options:
				"prep.data"    : For downloading `Singularity` containers and test data to used in this workflow.
				"index.ref"    : For indexing your reference genome using STAR.
				"do.alignment" : For aligning your reads to the reference genome using STAR.
--outdir     FOLDER    Path to where your output will go.
```

### 1.2. Download  `Singularity` containers and test data (required to execute the pipeline):
```bash
nextflow run nf-align -profile slurm --mode prep.data
```

### 1.3. Generating genome indexes.
```
nextflow run nf-align -profile slurm --mode index.ref
```

### 1.3. Read QC and Alignment:

```bash
nextflow run nf-align -profile slurm --mode do.alignment
```

---
