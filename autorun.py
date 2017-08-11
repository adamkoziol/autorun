#!/usr/bin/env python 3
from accessoryFunctions.accessoryFunctions import *
from glob import glob
import shutil
__author__ = 'adamkoziol'


class Autorun(object):

    def main(self):
        import time
        printtime('Starting autorun pipeline', self.start)
        while True:
            # try:
            #     self.miseqrun()
            # except ValueError:
            self.nasrun()
            printtime('Restarting loop in {}'.format(time.strftime("%M:%S", time.gmtime(self.sleeptime))), self.start)
            time.sleep(self.sleeptime)

    def miseqrun(self):
        pass

    def nasrun(self):
        """
        Checks the NAS To_Assemble folder for runs to assemble. If runs are not being copied to, it will queue them to be
        moved to the NODE and assembled. Returns a list of runs to assemble, otherwise returns False
        """
        printtime('Checking NAS For unassembled runs', self.start)
        # Check the To_Assemble folder for unassembled runs (e.g. do not have a _Queued, or _Assembled tag appended)
        # Get all the directories in the To_Assemble folder using glob. Sort using the modified time as the key. Ignore
        # non-directories with the os.path.is(dir) list comprehension
        runlist = [folder for folder in sorted(glob(os.path.join(self.assemblyfolder, '*')), key=os.path.getmtime)
                   if os.path.isdir(folder)]
        # If there are no runs to be assembled, allow the program to continue and sleep
        if len(runlist) == 0:
            printtime('No Runs To Process', self.start)
        else:
            # Initialise a list to store runs to be assembled
            verifiedrunlist = list()
            # Iterate through the runs
            for run in runlist:
                # Extract the name of the run from the path
                runname = os.path.split(run)[1]
                # Ignore runs that are already queued or assembled
                if "_Queued" in runname or "_Assembled" in runname:
                    continue
                # Verify fastq files exist along with metadata
                if "_Ready" in runname:
                    fastq_files = glob(os.path.join(run, '*fastq*'))
                    if len(fastq_files) > 0:
                        runinfostart = os.path.join(run, "RunInfo.xml")
                        samplesheetstart = os.path.join(run, "SampleSheet.csv")
                        generatefastqstart = os.path.join(run, "GenerateFASTQRunStatistics.xml")
                        if os.path.isfile(generatefastqstart) and os.path.isfile(samplesheetstart) \
                                and os.path.isfile(runinfostart):
                            pass
                        else:
                            printtime('Some metadata files are missing from {}'.format(runname), self.start)
                        newrunname = run + "_Queued"
                        if len(verifiedrunlist) < 10:
                            shutil.move(run, newrunname)
                            verifiedrunlist.append(newrunname)
                    else:
                        printtime('{} contains no .fastq(.gz) files to assemble!'.format(runname), self.start)
                        continue
                else:
                    printtime('{} is present but not flagged as _Ready'.format(runname), self.start)

    def __init__(self, args):
        self.miseqmount = os.path.join(args.miseqmountpoint, '')
        self.nasmount = os.path.join(args.nasmountpoint, '')
        self.destination = os.path.join(args.destinationmountpoint, '')
        self.assemblyfolder = os.path.join(self.nasmount, args.autoassemblyfolder, '')
        self.sleeptime = int(args.sleeptime)
        self.start = args.start
        self.logpath = os.path.join(self.nasmount, 'AssemblyLogs')
        make_path(self.logpath)
        self.main()

if __name__ == '__main__':
    # Argument parser for user-inputted values, and a nifty help menu
    from argparse import ArgumentParser
    import time
    # Parser for arguments
    parser = ArgumentParser(description='Automatically runs OLCSpades pipeline on completed MiSeq runs, and on '
                                        'folders in a specific directory')
    parser.add_argument('-m', '--miseqmountpoint',
                        default='mnt/miseq',
                        help='Mount point of the MiSeq')
    parser.add_argument('-n', '--nasmountpoint',
                        default='/mnt/nas/',
                        help='Mount point of shared NAS')
    parser.add_argument('-d', '--destinationmountpoint',
                        default='/hdfs',
                        help='Mount point of destination folder on Node')
    parser.add_argument('-a', '--autoassemblyfolder',
                        default='To_Assemble',
                        help='Name of folder containing directories to be assembled. Must be directly in the NAS'
                             'mount point e.g. /mnt/nas/To_Assemble')
    parser.add_argument('-s', '--sleeptime',
                        default='1200',
                        help='Length of time to sleep between each search for new runs to assemble ')
    # Get the arguments into an object
    arguments = parser.parse_args()

    # Define the start time
    arguments.start = time.time()
    # Run it
    Autorun(arguments)
    # Print a bold, green exit statement
    print('\033[92m' + '\033[1m' + "\nElapsed Time: %0.2f seconds" % (time.time() - arguments.start) + '\033[0m')
