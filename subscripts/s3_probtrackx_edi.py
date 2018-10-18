#!/usr/bin/env python3
from subscripts.config import executor_labels
from subscripts.utilities import write_start
from parsl.app.app import python_app

@python_app(executors=executor_labels)
def s3_1_probtrackx(sdir, a, b, stdout):
    from subscripts.utilities import run,smart_remove,smart_mkdir,write
    from os.path import join,exists
    EDI_allvols = join(sdir,"EDI","allvols")
    a_file = join(EDI_allvols, a + "_s2fa.nii.gz")
    b_file = join(EDI_allvols, b + "_s2fa.nii.gz")
    a_to_b = "{}to{}".format(a, b)
    a_to_b_formatted = "{}_s2fato{}_s2fa.nii.gz".format(a,b)
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    a_to_b_file = join(pbtk_dir,a_to_b_formatted)
    waypoints = join(sdir,"tmp","tmp_waypoint_{}.txt".format(a_to_b))
    exclusion = join(sdir,"tmp","tmp_exclusion_{}.nii.gz".format(a_to_b))
    termination = join(sdir,"tmp","tmp_termination_{}.nii.gz".format(a_to_b))
    bs = join(sdir,"bs.nii.gz")
    allvoxelscortsubcort = join(sdir,"allvoxelscortsubcort.nii.gz")
    terminationmask = join(sdir,"terminationmask.nii.gz")
    bedpostxResults = join(sdir,"bedpostx_b1000.bedpostX")
    merged = join(bedpostxResults,"merged")
    nodif_brain_mask = join(bedpostxResults,"nodif_brain_mask.nii.gz")
    if not exists(a_file) or not exists(b_file):
        write(stdout, "Error: Both Freesurfer regions must exist: {} and {}".format(a_file, b_file))
        return
    smart_remove(a_to_b_file)
    smart_mkdir(pbtk_dir)
    write(stdout, "Running subproc: {}".format(a_to_b))
    run("fslmaths {} -sub {} {}".format(allvoxelscortsubcort, a_file, exclusion), stdout)
    run("fslmaths {} -sub {} {}".format(exclusion, b_file, exclusion), stdout)
    run("fslmaths {} -add {} {}".format(exclusion, bs, exclusion), stdout)
    run("fslmaths {} -add {} {}".format(terminationmask, b_file, termination), stdout)
    with open((waypoints),"w") as f:
        f.write(b_file + "\n")
    arguments = (" -x {} ".format(a_file) +
        " --pd -l -c 0.2 -S 2000 --steplength=0.5 -P 1000" +
        " --waypoints={} --avoid={} --stop={}".format(waypoints, exclusion, termination) +
        " --forcedir --opd" +
        " -s {}".format(merged) +
        " -m {}".format(nodif_brain_mask) +
        " --dir={}".format(pbtk_dir) +
        " --out={}".format(a_to_b_formatted) +
        " --omatrix1")
    run("probtrackx2" + arguments, stdout)

@python_app(executors=executor_labels)
def s3_2_edi_consensus(sdir, a, b, stdout, inputs=[]):
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,is_float
    from os.path import join,exists
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    a_to_b = "{}to{}".format(a, b)
    a_to_b_file = join(pbtk_dir,"{}_s2fato{}_s2fa.nii.gz".format(a,b))
    b_to_a_file = join(pbtk_dir,"{}_s2fato{}_s2fa.nii.gz".format(b,a))
    smart_mkdir(join(pbtk_dir,"twoway_consensus_edges"))
    consensus = join(pbtk_dir,"twoway_consensus_edges",a_to_b)
    amax = run("fslstats {} -R | cut -f 2 -d \" \" ".format(a_to_b_file), print_output=False, working_dir=pbtk_dir).strip()
    if not is_float(amax):
        write(stdout, "Error: fslstats on {} returns invalid value {}".format(a_to_b_file, amax))
        return
    amax = int(float(amax))
    bmax = run("fslstats {} -R | cut -f 2 -d \" \" ".format(b_to_a_file), print_output=False, working_dir=pbtk_dir).strip()
    if not is_float(bmax):
        write(stdout, "Error: fslstats on {} returns invalid value {}".format(b_to_a_file, bmax))
        return
    bmax = int(float(bmax))
    if amax > 0 and bmax > 0:
        tmp1 = join(pbtk_dir, "{}to{}_tmp1.nii.gz".format(a, b))
        tmp2 = join(pbtk_dir, "{}to{}_tmp2.nii.gz".format(b, a))
        run("fslmaths {} -thrP 5 -bin {}".format(a_to_b_file, tmp1), stdout, working_dir=pbtk_dir)
        run("fslmaths {} -thrP 5 -bin {}".format(b_to_a_file, tmp2), stdout, working_dir=pbtk_dir)
        run("fslmaths {} -add {} -thr 1 -bin {}".format(tmp1, tmp2, consensus), stdout, working_dir=pbtk_dir)
        smart_remove(tmp1)
        smart_remove(tmp2)
    else:
        with open(join(pbtk_dir, "zerosl.txt"), 'a') as log:
            log.write("For edge {}:\n".format(a_to_b))
            log.write("{} is thresholded to {}\n".format(a, amax))
            log.write("{} is thresholded to {}\n".format(b, bmax))

@python_app(executors=executor_labels)
def s3_3_edi_combine(sdir, consensus_edges, stdout, inputs=[]):
    from subscripts.utilities import run,smart_remove,smart_mkdir,write,write_finish,write_checkpoint
    from os.path import join,exists
    from shutil import copyfile
    pbtk_dir = join(sdir,"EDI","PBTKresults")
    edi_maps = join(sdir,"EDI","EDImaps")
    total = join(edi_maps,"FAtractsumsTwoway.nii.gz")
    smart_mkdir(edi_maps)

    for a_to_b in consensus_edges:
        consensus = join(pbtk_dir, "twoway_consensus_edges", a_to_b + ".nii.gz")
        if not exists(consensus):
            write(stdout,"{} has been thresholded. See {} for details".format(a_to_b, join(pbtk_dir, "zerosl.txt")))
            continue
        if not exists(total):
            copyfile(consensus, total)
        else:
            run("fslmaths {0} -add {1} {1}".format(consensus, total), stdout)
    write_finish(stdout, "s3_probtrackx_edi")
    write_checkpoint(sdir, "s3", checksum)

def create_job(sdir, stdout, checksum):
    s3_2_futures = []
    oneway_edges = []
    consensus_edges = []
    with open(args.edge_list) as f:
        for edge in f.readlines():
            if edge.isspace():
                continue
            edge = edge.replace("_s2fa", "")
            a, b = edge.strip().split(',', 1)
            a_to_b = "{}to{}".format(a, b)
            b_to_a = "{}to{}".format(b, a)
            if a_to_b not in oneway_edges and b_to_a not in oneway_edges:
                write_start(stdout, "s3_probtrackx_edi")
                s3_1_future_ab = s3_1_probtrackx(sdir, a, b, stdout)
                s3_1_future_ba = s3_1_probtrackx(sdir, b, a, stdout)
                s3_2_future = s3_2_edi_consensus(sdir, a, b, stdout, inputs=[s3_1_future_ab, s3_1_future_ba])
                s3_2_futures.append(s3_2_future)
                oneway_edges.append(a_to_b)
                oneway_edges.append(b_to_a)
                consensus_edges.append(a_to_b)
    return s3_3_edi_combine(sdir, consensus_edges, checksum, stdout, inputs=s3_2_futures)
