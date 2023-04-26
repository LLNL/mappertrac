# Singularity Recipes

### The Singularity recipes used here requires a local copy of your Freesurfer license.txt to be present in the build context.
If you remove the copy instruction from the files section of the recipe, please mount your license.txt file to the container (-B license.txt:/opt/freesurfer/license.txt) when running. You can acquire a FreeSurfer license here: https://surfer.nmr.mgh.harvard.edu/fswiki/License

### Datalad
To easily download test data from OpenNeuro and other supported data repositories, we include a Datalad image recipe. Datalad commands can fetch the desired data using the container with the desired local folder for data mounted in the run command. For more information, please see this quickstart guide: https://handbook.datalad.org/en/latest/usecases/openneuro.html#quickstart

## Examples:

To build Datalad image:

```./build.sh yes```

Without Datalad image build:

```./build.sh no```

Testing with Datalad set from Traveling Human Phantom Project:
```
../datalad/datalad_get_samples.sh ../datalad
mappertrac -s1 --multi_container ../container/ ../datalad/ds000206/sub-THP0001/ && \
mappertrac -s2 --multi_container ../container/ ../datalad/ds000206/sub-THP0001/ && \
mappertrac -s3 --multi_container ../container/ ../datalad/ds000206/sub-THP0001/
```
