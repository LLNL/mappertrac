from datalad.api import install
import argparse, os, tempfile, sys, glob
from subprocess import call

#
# Download MRI images from OpenNeuro repository
# by providing path to install data and accession ID of the 
# MRI image.
#
# Usage: python download_openneuro.py --install-directory INSTALL_DIR -a ACC_NUM
#

# parse arguments
def parse_args():
    parser = argparse.ArgumentParser(description='Download MRI dataset from OpenNeuro.')
    parser.add_argument('--install-directory', dest='install_dir',
                        help='Path where data will be installed')
    parser.add_argument('-a', '--accession', dest='acc_num',
                        help='MRI Accession ID from OpenNeuro')

    args = parser.parse_args()
    if not args.install_dir:
        print('Install directory not specified')
        parser.print_help(sys.stderr)
        sys.exit()
    if not args.acc_num:
        print('OpenNeuro Accession ID not specified')
        parser.print_help(sys.stderr)
        sys.exit()    
    return(args)

# Main function controls the inputs and steps to submit
def main():
    # get input arguments
    args = parse_args()

    #acc_num = "ds000174"
    #data_dir = tempfile.mkdtemp()
    path = "%s/%s" % (args.install_dir, args.acc_num)
    if not os.path.exists(path):
        os.makedirs(path)

    print("Installation directory of data: %s" % path)
    dataset = install(path, "///openneuro/%s" % args.acc_num)
    nii_files = glob.glob("%s/sub*/ses*/func/*.nii.gz" % path)
    for nii in nii_files:
        dataset.get(nii)
        #dataset.get("sub-116/")
        print("Downloaded: %s" % nii)
    #print("SUCCESS!!")

main()
