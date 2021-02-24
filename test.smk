"""

snakemake.snakemake(
    snakefile="/home/kraemers/projects/mouse_hema_meth/lsf-smk-profile/test.smk",
    jobscript="/home/kraemers/projects/mouse_hema_meth/lsf-smk-profile/lsf-jobscript.sh",
    cluster="/home/kraemers/projects/mouse_hema_meth/lsf-smk-profile/lsf-submit.py",
    cluster_status="/home/kraemers/projects/mouse_hema_meth/lsf-smk-profile/lsf-status.py",
    restart_times=1,
    nodes=1000,
    latency_wait=60,
)

"""
import sys
import time

rule all:
    input:
        [
            '/home/kraemers/temp/smk-test/A.txt',
            '/home/kraemers/temp/smk-test/B.txt',
            '/home/kraemers/temp/smk-test/C.txt',
        ]


rule create_text:
    output:
          '/home/kraemers/temp/smk-test/{name}.txt',
    resources:
        mem_mb = 100,
        avg_mem = 100,
        walltime_min = 1,
        attempt = lambda wildcards, attempt: attempt,
    shell:
        """
        sleep 30
        echo rescued > {output}
        """
    # shell:
    #     """\
    #     if (( {resources.attempt} >= 2 )) ; then
    #         exit 1
    #     else
    #         sleep 1
    #     fi
    #
    #
    #     a b c
    #     """
    # run:
    #     with open(output[0], 'wt') as fout:
    #         fout.write('line1\n')
    #     if resources.attempt > 1:
    #         print('abort job, file is still there')
    #         sys.exit(1)
    #     else:
    #         time.sleep(150)
    #         with open(output[0], 'at') as fout:
    #             fout.write('line2\n')

