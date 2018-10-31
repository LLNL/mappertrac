#!/usr/bin/env python3
from subscripts.config import executor_labels
from subscripts.utilities import write_start
from parsl.app.app import python_app

@python_app(executors=executor_labels)
def s4_1_edi_consensus(sdir, a, b, stdout, container, inputs=[]):
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,is_float
    from os.path import join,exists
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    a_to_b = "{}to{}".format(a, b)
    a_to_b_file = join(pbtk_dir,"{}_s2fato{}_s2fa.nii.gz".format(a,b))
    b_to_a_file = join(pbtk_dir,"{}_s2fato{}_s2fa.nii.gz".format(b,a))
    if not exists(a_to_b_file) or not exists(b_to_a_file):
        write(stdout, "Error: both {} and {} must exist".format(a_to_b_file, b_to_a_file))
        return
    smart_mkdir(join(pbtk_dir,"twoway_consensus_edges"))
    consensus = join(pbtk_dir,"twoway_consensus_edges",a_to_b)
    amax = run("fslstats {} -R | cut -f 2 -d \" \" ".format(a_to_b_file), stdout, container, print_output=False, working_dir=pbtk_dir).strip()
    if not is_float(amax):
        write(stdout, "Error: fslstats on {} returns invalid value {}".format(a_to_b_file, amax))
        return
    amax = int(float(amax))
    bmax = run("fslstats {} -R | cut -f 2 -d \" \" ".format(b_to_a_file), stdout, container, print_output=False, working_dir=pbtk_dir).strip()
    if not is_float(bmax):
        write(stdout, "Error: fslstats on {} returns invalid value {}".format(b_to_a_file, bmax))
        return
    bmax = int(float(bmax))
    if amax > 0 and bmax > 0:
        tmp1 = join(pbtk_dir, "{}to{}_tmp1.nii.gz".format(a, b))
        tmp2 = join(pbtk_dir, "{}to{}_tmp2.nii.gz".format(b, a))
        run("fslmaths {} -thrP 5 -bin {}".format(a_to_b_file, tmp1), stdout, container)
        run("fslmaths {} -thrP 5 -bin {}".format(b_to_a_file, tmp2), stdout, container)
        run("fslmaths {} -add {} -thr 1 -bin {}".format(tmp1, tmp2, consensus), stdout, container)
        smart_remove(tmp1)
        smart_remove(tmp2)
    else:
        with open(join(pbtk_dir, "zerosl.txt"), 'a') as log:
            log.write("For edge {}:\n".format(a_to_b))
            log.write("{} is thresholded to {}\n".format(a, amax))
            log.write("{} is thresholded to {}\n".format(b, bmax))

@python_app(executors=executor_labels)
def s4_2_edi_combine(sdir, processed_edges, stdout, container, checksum, inputs=[]):
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,write_finish
    from os.path import join,exists
    from shutil import copyfile
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    edi_maps = join(sdir,"EDI","EDImaps")
    total = join(edi_maps,"FAtractsumsTwoway.nii.gz")
    smart_mkdir(edi_maps)

    for a_to_b in processed_edges:
        consensus = join(pbtk_dir, "twoway_consensus_edges", a_to_b + ".nii.gz")
        if not exists(consensus):
            write(stdout,"{} has been thresholded. See {} for details".format(a_to_b, join(pbtk_dir, "zerosl.txt")))
            continue
        if not exists(total):
            copyfile(consensus, total)
        else:
            run("fslmaths {0} -add {1} {1}".format(consensus, total), stdout, container)
    write_finish(stdout, "s4_edi")

def create_job(sdir, edge_list, stdout, container, checksum):
    s4_1_futures = []
    processed_edges = []
    write_start(stdout, "s4_edi")
    with open(edge_list) as f:
        for edge in f.readlines():
            if edge.isspace():
                continue
            a, b = edge.replace("_s2fa", "").strip().split(',', 1)
            a_to_b = "{}to{}".format(a, b)
            b_to_a = "{}to{}".format(b, a)
            if a_to_b not in processed_edges and b_to_a not in processed_edges:
                s4_1_futures.append(s4_1_edi_consensus(sdir, a, b, stdout, container))
                processed_edges.append(a_to_b)
    return s4_2_edi_combine(sdir, processed_edges, stdout, container, checksum, inputs=s4_1_futures)
