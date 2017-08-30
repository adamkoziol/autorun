#!/usr/bin/env python 3
import errno
import os
import shutil
from glob import glob

__author__ = 'adamkoziol'


def printtime(string, start):
    """
    Prints a string in bold with the elapsed time
    :param string: a string to be printed in bold
    :param start: integer of the starting time
    """
    import time
    print('\n\033[1m' + "[Elapsed Time: {:.2f} seconds] {}".format(time.time() - start, string) + '\033[0m')


def make_path(inpath):
    """
    from: http://stackoverflow.com/questions/273192/check-if-a-directory-exists-and-create-it-if-necessary \
    does what is indicated by the URL
    :param inpath: string of the supplied path
    """
    try:
        # os.makedirs makes parental folders as required
        os.makedirs(inpath)
    # Except os errors
    except OSError as exception:
        # If the os error is anything but directory exists, then raise
        if exception.errno != errno.EEXIST:
            raise


class Autorun(object):

    def main(self):
        """
        Run the required methods
        :return:
        """
        import time
        printtime('Starting autorun pipeline', self.start)
        while True:
            # try:
            #     self.miseqrun()
            # except ValueError:
            self.nascheck()
            #
            for run in self.verifiedrunlist:
                self.nasmove(run)
                self.startspades(run)
                self.collectnasresults(run)
            printtime('Restarting loop in {}'.format(time.strftime("%M:%S", time.gmtime(self.sleeptime))), self.start)
            # time.sleep(self.sleeptime)
            self.sleep()

    def miseqrun(self):
        pass

    def nascheck(self):
        """
        Checks the NAS To_Assemble folder for runs to assemble. If runs are not being copied, it will queue them to be
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
                        # Create variables for the names of the metadata files
                        runinfostart = os.path.join(run, "RunInfo.xml")
                        samplesheetstart = os.path.join(run, "SampleSheet.csv")
                        generatefastqstart = os.path.join(run, "GenerateFASTQRunStatistics.xml")
                        # If one or more of the metadata files are absent, write a warning
                        if not os.path.isfile(generatefastqstart) and os.path.isfile(samplesheetstart) \
                                and os.path.isfile(runinfostart):
                            printtime('WARNING: Some metadata files are missing from {}'.format(runname), self.start)
                        newrunname = run + "_Queued"
                        if len(self.verifiedrunlist) < 10:
                            shutil.move(run, newrunname)
                            self.verifiedrunlist.append(newrunname)
                    else:
                        printtime('WARNING: {} contains no .fastq(.gz) files to assemble!'.format(runname), self.start)
                        continue
                else:
                    printtime('WARNING: {} is present but not flagged as _Ready'.format(runname), self.start)
        # If there are folder process, print the list of the queue
        if len(self.verifiedrunlist) > 0:
            printtime('SUCCESS: The Following Folders are Queued for Assembly: {}'
                      .format(','.join(self.verifiedrunlist)), self.start)
        # Otherwise indicate that the runs are processed
        else:
            printtime('All folders processed', self.start)

    def nasmove(self, run):
        """
        Moves a given directory on the nas to the SSD on the node
        :run: directory to be moved to the node
        """
        nodedir = os.path.join(self.destination, os.path.split(run)[1])
        printtime('Copying {} to {} on node'.format(run, nodedir), self.start)
        # If, for whatever reason, a folder with the same name exists in the destination folder, add a number to the
        # name of the folder
        if os.path.exists(nodedir):
            # Allow up to 100 iterations
            for x in range(1, 100):
                # Add _X to the folder name
                newdirname = nodedir + '_{}'.format(x)
                # If the new directory exists as well, skip to the next iteration
                if os.path.isdir(newdirname):
                    continue
                # Otherwise, replace the node directory variable
                else:
                    nodedir = newdirname
                    printtime('WARNING: {} already exists on node! Appending _{} to directory'
                              .format(nodedir, x), self.start)
                    break
        # Copy over the files from the NAS
        try:
            shutil.copytree(run, nodedir)
            printtime('SUCCESS: Copy Complete', self.start)
            return nodedir

        except Exception as e:
            printtime('ERROR: {} {}'.format(e.__doc__, e.message), self.start)
            return False

    def collectnasresults(self, run):
        """
        Varies from collect_miseq_results by sending results to originating folder with the "_Assembled" tag
        :param run: Directory on the SSD containing results
        :return: False if a failure to copy, Directory to Delete
        """
        runname = os.path.split(run)[1]
        newrunname = runname.replace("_Ready_Queued", "_Assembled")
        combined_metadata_path = os.path.join(run, "reports/combinedMetadata.csv")
        nasrun = os.path.join(self.assemblyfolder, newrunname)
        # Check to see if the combinedMetadata file has been created - its presence is an excellent indicator of
        # whether the run assembled successfully
        if os.path.isfile(combined_metadata_path):
            printtime('SUCCESS: {} assembled'.format(runname), self.start)
        else:
            printtime('ERROR: {} did not assemble'.format(runname), self.start)

        # Check that the folder doesn't exist in To_Assemble already
        if os.path.isdir(nasrun):
            # Allow up to 100 tries to rename
            for x in range(1, 100):
                newdirname = nasrun + '_{}'.format(x)
                # If the new directory name exists as well, continue to the next iteration
                if os.path.isdir(newdirname):
                    continue
                else:
                    nasrun = newdirname
                    printtime('WARNING: {} already exists in To_Assemble! Appending _{} to directory'
                              .format(runname, x), self.start)
                    break
        # Copy the assembled run back to the NAS
        printtime('Copying {} to {}'.format(run, nasrun), self.start)
        # Attempt to copy the files to the NAS
        try:
            # Remove the .fastq.gz files in the runname directory, so they won't be copied back
            for fastq in glob(os.path.join(run, '*.fastq.gz')):
                os.remove(fastq)
            # Copy the remaining files and folders
            shutil.copytree(run, nasrun)
            printtime('SUCCESS: Copying Results', self.start)
            printtime('Cleaning up files on node', self.start)
            # Remove the directory on the node
            self.remove_directory(run)
            # Remove the _Ready_Queued folder from the NAS
            readyqueued = os.path.join(self.assemblyfolder, runname)
            self.remove_directory(readyqueued)
        except Exception as e:
            printtime('ERROR: {} {}'.format(e.__doc__, e.message), self.start)
            printtime('SKIPPING cleaning up Files on node due to error', self.start)

    def startspades(self, nodedir):
        """

        :param nodedir:
        """
        import subprocess
        printtime('Running Pipeline on {}'.format(nodedir), self.start)
        try:
            subprocess.call(['OLCspades.py', '-r', "/spadesfiles/", nodedir])
            printtime('SUCCESS: Pipeline Finished', self.start)
        except Exception as e:
            printtime('ERROR: {} {}'.format(e.__doc__, e.message), self.start)

    def remove_directory(self, directory):
        """
        Author: kkubasik
        Link https://stackoverflow.com/questions/303200/how-do-i-remove-delete-a-folder-that-is-not-empty-with-python
        Delete everything reachable from the directory named in 'top', assuming there are no symbolic links.
        CAUTION:  This is dangerous!  For example, if top == '/', it could delete all your disk files.
        """
        # Set variables for directories that cannot be deleted
        root_dir = '/'
        backup_dir = os.path.join(self.nasmount, "MiSeq_Backup")
        to_assemble_dir = os.path.join(self.nasmount, "To_Assemble")
        # If the directory to be deleted is not one of the prohibited ones, proceed
        if directory != root_dir and directory != backup_dir and directory != to_assemble_dir:
            try:
                # os.walk through all the files and folders in the directory
                for root, dirs, files in os.walk(directory, topdown=False):
                    # Remove the files
                    for name in files:
                        os.remove(os.path.join(root, name))
                    # Remove the folders
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
                # Remove the now empty directory
                shutil.rmtree(directory)
                printtime('SUCCESS {} Deleted'.format(directory), self.start)
            except Exception as e:
                printtime('ERROR {} {}'.format(e.__doc__, e.message), self.start)
        else:
            printtime('ERROR! Will not delete root of {}'.format(self.nasmount), self.start)

    def sleep(self):
        """

        """
        import sys
        for i in range(self.sleeptime, 0, -10):
            if i % 100 == 0:
                printtime('Seconds remaining until loop restart: {}'.format(str(i)), self.start)
                time.sleep(10)
            else:
                sys.stdout.write(str(i) + ' ')
                sys.stdout.flush()
                time.sleep(10)

    def __init__(self, args):
        # Initialise variables from arguments
        self.miseqmount = os.path.join(args.miseqmountpoint, '')
        self.nasmount = os.path.join(args.nasmountpoint, '')
        self.destination = os.path.join(args.destinationmountpoint, '')
        self.assemblyfolder = os.path.join(self.nasmount, args.autoassemblyfolder, '')
        self.sleeptime = int(args.sleeptime)
        self.start = args.start
        self.logpath = os.path.join(self.nasmount, 'AssemblyLogs')
        # Initialise a list to store runs to be assembled
        self.verifiedrunlist = list()
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
