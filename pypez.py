import configargparse
import os
import random
import re
import sys
import tempfile
import threading
import time

from StringIO import StringIO
from datetime import timedelta

if sys.version_info < (3,0):
    # subprocess32 backports subprocess moudule improvements from python 3.x to 2.x
    from subprocess32 import Popen
else:
    from subprocess import Popen

def init_command_line_args():
    p = configargparse.getArgumentParser() # usage="usage: %prog [options] [arg1] .. ")

    #p.add("--LSF", dest="use_LSF", action="store_true", help="Execute each command as an LSF job.")
    #p.add("--SGE", dest="use_SGE", action="store_true", help="Execute each command as an SGE job.")
    p.add("-t", "--test", dest="is_test", action="store_true", help="Prints all commands without "
          "actually running them. -t overrides all other options (eg. -f, -F).")
    p.add("-F", "--force_all", dest="should_force_all", action="store_true", help="Forces execution of all commands "
          "even if their output files are up to date.")
    p.add("-f", "--force", dest="force_these", action="append", help="Forces execution of specific step(s) "
          "(-f 1.1  or -f myscript) even if their output files are up to date.")
    
    p.add("-o", "--only", dest="only_these", action="append", help="Only execute commands matching the "
          "given pattern (eg. -o 1.3 or -o myscript). -o can be specified more than once to run several specific "
          "commands and nothing else. -o can be used with -f, -F to force the execution of the given command(s).")
    p.add("-b", "--begin", dest="begin_with_these", action="append", help="Execute commands starting at this "
          "command (eg. -b 1.3 or -b myscript). -b can be specified more than once, in which case they are OR'd "
          "together. -b can be used together with -e and/or -f, -F to execute a specific subset of commands.")
    p.add("-e", "--end", dest="end_with_these", action="append", help="Execute commands up to and including this "
          "command (eg. -e 1.3 or -e myscript). -e can be specified more than once, in which case they are OR'd "
          "together. -e can be used together with -b and/or -f, -F to execute a specific subset of commands.")
    
    p.add("-p", "--num_processes", type=int, help="Number of Jobs to run in parallel. Default: unlimited. "
          "For example, if 4 Jobs are added via job_runner.add_parallel(..), and then the pipeline is run with -p 3, "
          "then 3 of the 4 jobs will be launched right away in parallel, and the 4th job will be launched as soon as "
          "one of the 3 completes.", default=1)
    #p.add("--email", dest="email_addresses", action="append", help="Send an email to the given email "
    #    "address(es) when all jobs complete.")

    #p.add("--clean", dest="clean", action="store_true", help="Deletes output files for all commands. Can be "
    #    " used with -b, -e and/or -o to delete output files from only a subset of the steps.")



class Job:
    """Represents a sequence of commands that should run in series."""

    def __init__(self,
                 cmd=None,
                 working_directory="",
                 name="",
                 log_directory="logs",
                 **kwargs):
        """
        Args:
            cmd: if this Job consists of a single command rather than a series
                of them, it can be specified here.
            working_directory: directory where to execute commands. Default: current directory
            name: short name for this job to be used for logging
            log_directory: where to store log files
            **kwargs: additional args to pass to self.add(..)
        """
        self.directory = working_directory
        self.name = name
        
        self.log_filename = os.path.join(working_directory, log_directory, (name+".log") if name else "job.log")

        self.next_command_id = 1  # used to give each command in this job a unique id
        self.command_objs = []    # the list of __command_struct objects representing actual commands
        if cmd:
            self.add(cmd=cmd, **kwargs)
        elif kwargs:
            raise ValueError("kwargs (%s) should only be used when cmd is not None " % (str(kwargs)))

    def add(self,
            cmd,
            input_filenames = None,
            output_filenames = None,
            dir = None,
            name = None,
            use_tmp_output_filenames = True,
            num_threads_used = 1,
            memory_in_gb = 8,
            needs_to_run_checker = None,
            output_handler = None,
            dont_log_output = False,
            ignore_failure=False):
        """
        Adds another command line for this job to run after previous commands have completed.

        Args (** required):
            cmd **: the command line to execute
            input_filenames: list of input file(s). Specifying this is equivalent to adding "IN:x" tags within the
                command itself. If specified, the command will only execute when any of its output files are
                out of date relative to the input files.
            output_filenames: list of output file(s). Specifying this is equivalent to adding "OUT:x" tags within the
                command itself. If specified, the command will only execute when any of its output files are
                out of date relative to the input files.
            dir: the directory in which this command should be executed
            name: a short name for this command (used for logging)
            use_tmp_output_filenames: whether to string-replace output filenames in command(s) with temp output
                filenames before executing the command, and then, if/when the command completes successfully, rename the
                output files to their original filenames. This prevents incomplete or corrupted output files from being
                mistaken for successfully complete ones and used in subsequent steps of the pipeline.
            num_threads_used: number of threads created by this command. If specified, the pipeline will reserve this
                number of CPUs when the command is submitted to a cluster (eg. SGE)
                Specifying this is equivalent to using a "NUM_THREADS:x" tag within the command line string itself.
            memory_in_gb: how many gigabytes of memory to reserve. Currently only used in SGE and LSF modes.
            needs_to_run_checker: function that will be executed to check whether this command needs to run or
                whether it can be skipped. It takes zero arguments and returns True if the command needs to run. If
                input/output filenames are also provided, the command will run if this function returns True OR if any
                of the output files are out of date relative to the input files.
            output_handler: function that takes one argument. It will be called on every line the command outputs to
                stdout or stderr.
            dont_log_output: don't log the output of this command
            ignore_failure: Whether to run any subsequent commands if this command doesn't end successfully
        """

        if input_filenames == None:
            input_filenames = []
        if output_filenames == None:
            output_filenames = []


        # Parse any "IN:", "OUT:" strings from the command to get additional names of input and output files.
        #   Examples:
        #	    before using IN: OUT: annotations:
        #          job.add_serial("picard CollectGcBiasMetrics R=hg19.fa I=mydata.bam O=mydata.gc_bias.txt CHART=mydata.gc_bias.pdf SUMMARY_OUTPUT=mydata.gc_bias_summary.txt", ["mydata.bam"], ["mydata.gc_bias.txt", "mydata.gc_bias.pdf"])
        #       after adding IN: OUT: annotations:
        # 		   job.add_serial("picard CollectGcBiasMetrics R=IN:hg19.fa I=IN:mydata.bam O=OUT:mydata.gc_bias.txt CHART=OUT:mydata.gc_bias.pdf SUMMARY_OUTPUT=OUT:mydata.gc_bias_summary.txt")
        #

        num_threads_used = [num_threads_used]

        # parse OUT: any IN: strings and append the files to input_filenames and output_filenames.
        def IN_string_match_handler(match):
            filename = match.group(1)
            input_filenames.append(filename.strip())  # add
            return filename # replace the "IN:filename" string with "filename"
        def OUT_string_match_handler(match):
            filename = match.group(1)
            output_filenames.append(filename.strip())
            if use_tmp_output_filenames:
                return "OUT:%s" % filename # keep the OUT: tag so temp filenames can be substituted
            else:
                return filename            # replace the "OUT:filename" string with "filename"
        def NUM_THREADS_string_match_handler(match):
            num_threads_string = match.group(1)
            try:
                num_threads_used[0] = int(num_threads_string)
            except:
                print("NUM_THREADS: annotation attached to '" + num_threads_string + "'. Integer required.")
            return num_threads_string # replace the "OUT:filename" string with "filename"


        cmd = re.sub("IN:([^\s:]+\s?):?", IN_string_match_handler, cmd)
        cmd = re.sub("OUT:([^\s:]+\s?):?", OUT_string_match_handler, cmd)
        cmd = re.sub("NUM_THREADS:([^\s:]+\s?):?", NUM_THREADS_string_match_handler, cmd)

        # eliminate any duplicates
        input_filenames = list(set(input_filenames))
        output_filenames = list(set(output_filenames))
        num_threads_used = num_threads_used[0]

        cmd_obj = self._CommandObj(cmd, input_filenames, output_filenames, dir, name, use_tmp_output_filenames,
            num_threads_used, memory_in_gb, needs_to_run_checker, output_handler, dont_log_output, ignore_failure,
            self.next_command_id)
        self.command_objs.append(cmd_obj)
        self.next_command_id += 1

        return self


    class _CommandObj:  # private class used for holding data about a command to be executed
        def __init__(self, cmd, input_filenames, output_filenames, dir, name,
                     use_tmp_output_filenames, num_threads_used, memory_in_gb,
                     needs_to_run_checker, output_handler, dont_log_output,
                     ignore_failure, next_command_id):

            self.__dict__ = locals()  # save constructor args as fields in this object
            self.directory = dir
            self.command = cmd
            self.command_id = str(next_command_id)



class JobRunner:
    """Executes the Job(s) locally or on the cluster (depending on mode)."""

    _instance = None  # only one JobRunner instance can be created per python execution.

    @staticmethod
    def get_instance():
        if not JobRunner._instance:
            JobRunner._instance = JobRunner() # instanciate a JobRunner

        return JobRunner._instance

    def __init__(self, job=None, num_processes=sys.maxint, shell='/bin/bash'):
        """
        Args:
            job: if only one Job will be run. Otherwise, use .add_parallel(..)
        num_processes: Max number of jobs to run in parallel.
        """
        if JobRunner._instance:
            raise Exception("Creating more than one JobRunner object is not supported. Instead, use "
                            "JobRunner.getInstance() to get the single instance of JobRunner.")
        JobRunner._instance = self


        # TODO put these in config file:
        self.pipeline_shutdown_reason = "" # used to prevent jobs from starting if upstream command or job fails.
        self.LOG_HEARTBEAT_INTERVAL_MINUTES = 2.5  # how frequently to write "still running" message to log
        self.SGE_max_cores_to_reserve = 12  # The maximum number of SGE cores to reserve for a job.

        self.pipeline_id = str(random.randint(10*5, 10**6 - 1))
        self.time_stamp = time.strftime("%Y-%m-%d_%H.%M.%S")
        self.next_job_id = 1  # the job id is prepended to the command id in log output

        self.shell = shell
        self._fix_keyboard_interrupt_handling()

        self.jobs = []  # jobs to run in parallel
        self.threads = []  # thread pool

        if job:
            self.add_parallel(job)

        init_command_line_args()
        
        p = configargparse.getArgumentParser()
        opts, args  = p.parse_known_args()

        self.is_test = opts.is_test
        self.use_SGE = None  # opts.use_SGE 
        self.use_LSF = None  # opts.use_LSF

        self.should_force_all = opts.should_force_all
        self.force_these = opts.force_these

        self.only_these = opts.only_these
        self.begin_with_these = opts.begin_with_these
        self.end_with_these = opts.end_with_these
    
        self.num_processes = opts.num_processes
        self.clean = None # opts.clean


    def add_parallel(self, job):
        for command_obj in job.command_objs:
            command_obj.command_id = str(self.next_job_id) + "." + command_obj.command_id

        job.began_executing_commands = False
        job.ended_executing_commands = False

        self.jobs.append(job)
        self.next_job_id += 1
        return self

    def start(self):
        # if -c arg and not -F arg, warn user about which files will be deleted
        if self.clean and not self.should_force_all:
        	print("WARNING: -c arg used. The following files will be deleted: ")
        	for job in self.jobs:
        		for command_obj in job.command_objs:
        			for output_filename in command_obj.output_filenames:
        				print("   " + output_filename)
        
        
        	while True:
        		answer = raw_input("Do you want to delete these files [y/n]?").lower().strip()
        		if answer:
        			if answer == "y" or answer == "yes":
        				break
        			elif answer == "n" or answer == "no":
        				return



        # create empty queues to be used for handing Jobs to processes
        job_queues = [[] for i in range(min(self.num_processes, len(self.jobs)))]
        for i, job in enumerate(self.jobs):
            job_queues[i % len(job_queues)].append(job)  # assign job i to the next job_queue

        # execute each job queue on a separate thread
        if len(self.jobs) > 0:
            print("Starting %d threads to run %d jobs." % (len(job_queues), len(self.jobs)))

        for i, job_queue in enumerate(job_queues):
            thread = threading.Thread(target=self._execute_jobs, args=[job_queue], verbose=False)
            thread.start()
            thread.join(5) # Let thread run 5 seconds before starting next thread so their outputs don't get jumbled
            self.threads.append(thread)

    def run(self, job=None):
        """Run the job(s) added previously through add_parallel(..), followed by the job (if any) passed in as an arg."""        
        if job is not None:
            self.add_parallel(job)

        self.start()
        self.wait_for_jobs_to_finish()

    def _execute_jobs(self, job_queue):
        """Runs the Job(s) in the given job queue."""

        execute_command = self.execute_command_locally  # default
        if self.is_test:
            execute_command = self.execute_command_locally
        elif self.use_SGE:
            execute_command = self.execute_command_using_SGE
        elif self.use_LSF:
            execute_command = self.execute_command_using_LSF

        for job in job_queue:
            # set up log file for this Job
            meta_log = LogStream(print_time=True, num_tabs=0)
            if self.pipeline_shutdown_reason:
                meta_log.writeln("Command " + self.pipeline_shutdown_reason + " failed upstream. Skipping job: " + job.name)
                continue

            meta_log.writeln("------------------------------")

            job_log = LogStream(name=job.name, print_time=True, num_tabs=0)
            if job.log_filename and not self.is_test:
                # set up the log directory
                if job.directory and not os.path.isabs(job.log_filename):
                    job.log_filename = os.path.join(job.directory, job.log_filename)

                log_dir = os.path.dirname(job.log_filename)
                if not os.path.isdir(log_dir):
                    try:
                        # create log dir if necessary
                        os.mkdir(log_dir)
                    except Exception, e:
                        meta_log.writeln("Couldn't create log directory %s: %s" % (log_dir, e))
                        continue

                else:
                    # move old logs to date-based archive directories
                    for other_log_file in os.listdir(log_dir):
                        other_log_file_abs = os.path.join(log_dir, other_log_file)
                        if os.path.isfile(other_log_file_abs) and other_log_file.count(os.path.basename(job.log_filename)):
                            other_log_file_creation_time = time.localtime(os.stat(other_log_file_abs).st_ctime)
                            other_log_file_creation_time_str = time.strftime("%Y-%m-%d_%I%p", other_log_file_creation_time)
                            #if other_log_file_creation_time.tm_hour < 7: other_log_file_creation_time_str += "_night"
                            #elif other_log_file_creation_time.tm_hour < 11: other_log_file_creation_time_str += "_morning"
                            #elif other_log_file_creation_time.tm_hour < 14: other_log_file_creation_time_str += "_lunch"
                            #elif other_log_file_creation_time.tm_hour < 18: other_log_file_creation_time_str += "_afternoon"
                            #else: other_log_file_creation_time_str += "_evening"
                            archive_dir = os.path.join(log_dir, other_log_file_creation_time_str)
                            if not os.path.isdir(archive_dir):
                                os.mkdir(archive_dir)
                            other_log_file_destination = os.path.join(archive_dir, other_log_file)

                            meta_log.writeln("Moving old log: " + other_log_file + " to " + other_log_file_destination)
                            os.rename(other_log_file_abs, other_log_file_destination)

                # finalize the log filename
                #job.log_filename += strftime(".%Y-%m-%d_%H.%M.%S")
                job.log_filename = os.path.join(os.path.dirname(job.log_filename), # prepend pipeline id to log filename
                    "p" + self.pipeline_id + time.strftime(".%Y-%m-%d_%H.%M.%S.") + os.path.basename(job.log_filename))

                # create the log file
                job_log.add_stream(open_file(job.log_filename, "a+"))
                meta_log.writeln("--> Logging job to file: " + job.log_filename)

            job_log.writeln("--> Original command-line: python " + os.path.basename(sys.argv[0]) + " " + " ".join(sys.argv[1:]))
            job_log.writeln("--> Job Directory: " + str(os.path.abspath(job.directory)))

            job_t0 = time.time() # time this job

            # execute each command in the Job
            for command_obj in job.command_objs:
                # inherit directory from job object
                directory = job.directory
                if command_obj.directory:
                    directory = command_obj.directory  # over-ride job directory with command-specific directory
                directory = os.path.abspath(directory)

                # check whether, if -b was used, the current command matches the string provided to -b
                if self.begin_with_these and not job.began_executing_commands:
                    for begin_with_arg in self.begin_with_these:
                        if self.command_matches_description(command_obj, begin_with_arg):
                            job.began_executing_commands = True
                            break   # proceed with running this command
                    else:
                        job_log.writeln("--> --begin_with " + str(self.begin_with_these) + " arg:  skipping " + command_obj.command_id + ": " + command_obj.command, num_tabs=0, cmd_name=command_obj.name)
                        continue  # skip this command

                # check whether, if -e was used, the current command matches the string provided to -e
                if self.end_with_these:
                    if job.ended_executing_commands:
                        job_log.writeln("--> --end_with " + str(self.end_with_these) + " arg:  skipping " + command_obj.command_id + ": " + command_obj.command, num_tabs=0, cmd_name=command_obj.name)
                        job_log.writeln("--> --end_with " + str(self.end_with_these) + " arg:  skipping all remaining commands.", cmd_name=command_obj.name )
                        break # skip all remaining commands
                    else:
                        for end_with_arg in self.end_with_these:
                            if self.command_matches_description(command_obj, end_with_arg):
                                job.ended_executing_commands = True
                                break # proceed with running this command. But now that job.ended_executing_commands = True, this command will be the last one executed for this job.

                # check whether, if -o was used, the current command matches the string provided to -o
                if self.only_these:
                    for only_arg in self.only_these:
                        if self.command_matches_description(command_obj, only_arg):
                            break # proceed with running this command
                    else:
                        job_log.writeln("--> --only " + str(self.only_these) + " arg:  skipping " + command_obj.command_id + ": " + command_obj.command, num_tabs=0, cmd_name=command_obj.name)
                        continue # skip this command

                # check whether -F was used, or, if -f was used, whether the current command matches the string provided to -f
                if self.should_force_all or (self.force_these and [True for arg in self.force_these if self.command_matches_description(command_obj, arg)]):
                    force_this_command = True
                    force_string = " -f " + str(self.force_these) + " arg: " if self.force_these else " -F arg: "
                else:
                    force_this_command = False
                    force_string = ""

                # handle -c arg
                if self.clean and not self.should_force_all:
                    for output_filename in command_obj.output_filenames:
                        abs_output_filename = get_absolute_path(output_filename, directory)
                        if not os.path.isfile(abs_output_filename):
                            job_log.writeln("Not there: " + output_filename, cmd_name=command_obj.name)
                        else:
                            if not self.is_test:
                                job_log.writeln("Deleting: " + output_filename, cmd_name=command_obj.name)
                                os.remove(abs_output_filename)
                            else:
                                job_log.writeln("Test. Would have deleted: " + output_filename, cmd_name=command_obj.name)

                # modify command so that output will be written to temp files first, and, upon successful completion of the command, moved to the original output filenames.
                use_tmp_output_filenames = command_obj.use_tmp_output_filenames
                if use_tmp_output_filenames:
                    for output_filename in command_obj.output_filenames:
                        temp_output_filename = os.path.join(os.path.dirname(output_filename), "tmp."+ self.time_stamp +"." + os.path.basename(output_filename))
                        command_with_tmp_filenames = command_obj.command.replace("OUT:"+output_filename, temp_output_filename)
                        if command_with_tmp_filenames != command_obj.command:
                            command_obj.command = command_with_tmp_filenames
                        else:
                            command_obj.command = command_obj.command.replace(output_filename, temp_output_filename)

                # check whether the command needs to run
                if not self.is_test and ((command_obj.output_filenames and command_obj.input_filenames) or command_obj.needs_to_run_checker):
                    temp_log_buffer = StringIO()
                    if not force_this_command and not self.does_command_need_to_run(command_obj, directory, temp_log_buffer):
                        job_log.writeln("--> Skipping " + command_obj.command_id + ": " + command_obj.command, num_tabs=0, cmd_name=command_obj.name)
                        if command_obj.directory:
                            job_log.writeln("     in directory: " + command_obj.directory, num_tabs=0, cmd_name=command_obj.name)

                        #job_log.writeln(command_obj.command, num_tabs=2, cmd_name=command_obj.name)

                        for temp_log_line in temp_log_buffer.getvalue().split("\n"):
                            job_log.writeln(temp_log_line, num_tabs=2, cmd_name=command_obj.name)  # copy temp_log_buffer to the job_log

                        continue  # Skip this command
                    else:

                        job_log.writeln("-->" + force_string + " Exec " + command_obj.command_id + ": " + command_obj.command, num_tabs=0, cmd_name=command_obj.name)
                        if command_obj.directory: job_log.writeln("     in directory: " + command_obj.directory, num_tabs=0, cmd_name=command_obj.name)
                        if not force_this_command:
                            for temp_log_line in temp_log_buffer.getvalue().split("\n"):
                                if temp_log_line.count("["): # report the reason this command needs to run
                                    job_log.writeln(temp_log_line, num_tabs=2, cmd_name=command_obj.name)  # copy temp_log_buffer to the job_log

                                    #for temp_log_line in temp_log_buffer.getvalue().split("\n"):
                                    #	job_log.writeln(temp_log_line, num_tabs=2, cmd_name=command_obj.name)

                else:
                    # proceed with running the command
                    job_log.writeln("--> Exec " + command_obj.command_id + ": " + command_obj.command, num_tabs=0, cmd_name=command_obj.name)
                    if command_obj.directory:
                        job_log.writeln("     in directory: " + command_obj.directory, num_tabs=0, cmd_name=command_obj.name)

                # handle self.is_test
                if self.is_test:
                    continue # Skip actually running this command

                try:
                    cmd_t0 = time.time()
                    execute_command(command_obj, directory, job_log)

                    cmd_t1 = time.time()
                    job_log.writeln("    Finished " + command_obj.command_id + ". Running time: "+str(timedelta(seconds=cmd_t1-cmd_t0))+" sec.", cmd_name=command_obj.name)  # + ": " + command_obj.command

                except Exception, e:
                    if not command_obj.ignore_failure:
                        self.pipeline_shutdown_reason = "[" + str(command_obj.name) + "]"  # prevent any new jobs from starting
                    job_log.writeln("    COMMAND FAILED - Exception: " + str(e), cmd_name=command_obj.name)
                    sys.exit(1)

                if not self.is_test and use_tmp_output_filenames:
                    for output_filename in command_obj.output_filenames:
                        temp_output_filename = os.path.join(os.path.dirname(output_filename), "tmp."+ self.time_stamp + "." + os.path.basename(output_filename))
                        # move tmp output path to output path - first check that temp_output_filename is readable
                        if os.access(get_absolute_path(temp_output_filename, directory), os.R_OK):
                            # if output path already exists, move it aside
                            if os.path.exists(get_absolute_path(output_filename, directory)):
                                aside_filename = os.path.join(os.path.dirname(output_filename),  os.path.basename(output_filename) + ".old." + self.time_stamp)
                                os.system("mv -f " +
                                          get_absolute_path(output_filename, directory) + " " +
                                          get_absolute_path(aside_filename, directory))
                                job_log.writeln("    Renamed existing " + output_filename + " to " + aside_filename, cmd_name=command_obj.name)

                            os.system("mv -f " +
                                      get_absolute_path(temp_output_filename, directory) + " " +
                                      get_absolute_path(output_filename, directory))
                            job_log.writeln("    Renamed " + temp_output_filename + " to " + output_filename, cmd_name=command_obj.name)
                        else:
                            job_log.writeln("    WARNING: unable to rename " + temp_output_filename + " to " + output_filename + ". " + temp_output_filename + " is not readable", cmd_name=command_obj.name)
            job_t1 = time.time()
            job_log.writeln("--> Finished job " + job.name + ". Total job running time: "+str(timedelta(seconds=job_t1-job_t0))+" sec.")
            job_log.close()


    def wait_for_jobs_to_finish(self):
        """Wait for all processes to finish and then resets the job and thread lists, allowing more jobs to be added, and the job runner to be started again."""

        # if already running, wait for jobs to finish. Otherwise, add
        for thread in self.threads:
            if thread.isAlive():
                thread.join()

        self.jobs = []
        self.threads = []

    def execute_command_locally(self, command_obj, directory, job_log):
        # create a temporary log file
        temp_log_file = None
        if not command_obj.dont_log_output:
            temp_log_filename = get_absolute_path("tmp.pipelinelog." + command_obj.command_id + "." + str(random.randint(10*10, 10**11 - 1)), directory, allow_compressed_version=False)
            temp_log_file = open(temp_log_filename, "w+")
            #temp_log_file = tempfile.NamedTemporaryFile(bufsize=0) #

        spawned_process = Popen(command_obj.command, bufsize = 0, shell=True, cwd=directory, stdout=temp_log_file, stderr=temp_log_file, executable=self.shell)

        if temp_log_file:
            # while the job is running, continually read from the temp_log_file and copy this to the job_log
            self.copy_command_output_to_log(command_obj, spawned_process, temp_log_file, job_log)

            temp_log_file.close()
            os.remove(temp_log_filename)
            #os.remove(temp_file_path)
        else:
            spawned_process.wait()

        if spawned_process.returncode is not None and spawned_process.returncode != 0:
            raise Exception("Non-zero return code: " + str(spawned_process.returncode))


    def execute_command_using_LSF(self, command_obj, directory, job_log):
        # TODO put this in config file
        tempdir = "/broad/hptmp/weisburd/tmp/" #tempfile.gettempdir()

        #if not os.path.isfile(qsub_executable):
        #	raise Exception("Cannot submit job to SGE. %(qsub_executable)s command not found." % locals())


        num_cores = min(command_obj.num_threads_used, self.SGE_max_cores_to_reserve)

        # compute SGE job name
        lsf_job_name = ""
        if job_log.name:
            lsf_job_name += job_log.name
        if command_obj.name:
            if lsf_job_name:
                lsf_job_name += " " # add spacer
            lsf_job_name += command_obj.name

        if lsf_job_name:
            lsf_job_name = lsf_job_name.replace(" ", "_")
        else:
            lsf_job_name = os.path.basename(command_obj.command.split(" ")[0])

        # create a temporary log file
        temp_log_filename = None
        if not command_obj.dont_log_output:
            temp_log_filename = get_absolute_path("tmp.pipelinelog." + command_obj.command_id + "." + str(random.randint(10*10, 10**11 - 1)), directory, allow_compressed_version=False)
            temp_log_file = open(temp_log_filename, "a+") # create log file
            temp_log_file.close()
            temp_log_file = open(temp_log_filename, "r") # open this file for reading



        qsub_script_name = os.path.join(tempdir, lsf_job_name + ".%d.sh" % random.randint(10**9, 10**10 - 1))

        f = open( qsub_script_name, "w+")
        #f.write("source ~/.bashrc;" + "\n")
        #f.write("/db/ngs-bioinfo/prog/bash-4.2/bin/bash \n") # make sure the shell is a bash shell
        f.write("echo Running on $HOSTNAME. `uptime | cut -c 40-`, cpus: `cat /proc/cpuinfo | grep processor | wc -l`, `cat /proc/meminfo | grep MemTotal`, `cat /proc/meminfo | grep MemFree`;" + "\n")
        f.write("cd " + directory + "\n")
        f.write(command_obj.command + "\n")
        f.close()

        os.system("chmod 777 " + qsub_script_name)
        qsub_command = "bsub "
        qsub_command += " -K " # make hte command run interactively
        qsub_command += " -q week "  # hour or week
        qsub_command += " -J %s " % lsf_job_name
        qsub_command += " -P project "  # email when done
        if num_cores:
            qsub_command += " -n %s " % num_cores
        qsub_command  += " -R rusage[mem=%s] " % command_obj.memory_in_gb # memory usage
        qsub_command += " -o " + temp_log_filename + " "  # this also capture stderr (since stdout is not specified)
        qsub_command += qsub_script_name
        qsub_command += ' > /dev/null'

        spawned_qsub_process = Popen(qsub_command, bufsize = 0, shell=True, cwd=directory)

        # while the job is running, continually read from the temp_log_file and copy this to the job_log
        if temp_log_filename:
            self.copy_command_output_to_log(command_obj, spawned_qsub_process, temp_log_file, job_log)

            temp_log_file.close()
            os.remove(temp_log_filename)
        else:
            spawned_qsub_process.wait()

        if spawned_qsub_process.returncode is not None and spawned_qsub_process.returncode != 0:
            raise Exception("Non-zero return code: " + str(spawned_qsub_process.returncode))


        os.remove(qsub_script_name)

        #  	Using temp files instead of subprocess.PIPE because subprocess.PIPE has subtle bugs:
        #			http://bugs.python.org/issue1652
        #
        #			They were fixed in python 3+, but not in python 2.7 and older. There's a downloadable module with fixes, but it creates an extra dependecy:
        #			http://code.google.com/p/python-subprocess32/



    def execute_command_using_SGE(self, command_obj, directory, job_log):
        # TODO put this in config file
        qsub_executable = "/opt/gridengine/bin/lx-amd64/qsub"
        #qsub_executable = "qsub"
        tempdir = tempfile.gettempdir() # "/db/ngs-bioinfo/tmp"

        #if not os.path.isfile(qsub_executable):
        #	raise Exception("Cannot submit job to SGE. %(qsub_executable)s command not found." % locals())


        num_cores = min(command_obj.num_threads_used, self.SGE_max_cores_to_reserve)

        # compute SGE job name
        sge_job_name = ""
        if job_log.name:
            sge_job_name += job_log.name
        if command_obj.name:
            if sge_job_name:
                sge_job_name += " " # add spacer
            sge_job_name += command_obj.name

        if sge_job_name:
            sge_job_name = sge_job_name.replace(" ", "_")
        else:
            sge_job_name = os.path.basename(command_obj.command.split(" ")[0])

        # create a temporary log file
        temp_log_filename = None
        if not command_obj.dont_log_output:
            temp_log_filename = get_absolute_path("tmp.pipelinelog." + command_obj.command_id + "." + str(random.randint(10*10, 10**11 - 1)), directory, allow_compressed_version=False)
            temp_log_file = open(temp_log_filename, "a+") # create log file
            temp_log_file.close()
            temp_log_file = open(temp_log_filename, "r") # open this file for reading



        qsub_script_name = os.path.join(tempdir, sge_job_name + ".%d.sh" % random.randint(10**9, 10**10 - 1))
        f = open( qsub_script_name, "w+")
        #f.write("source ~/.bashrc;" + "\n")
        f.write("/db/ngs-bioinfo/prog/bash-4.2/bin/bash \n") # make sure the shell is a bash shell
        f.write("echo Running on $HOSTNAME. `uptime | cut -c 40-`, cpus: `cat /proc/cpuinfo | grep processor | wc -l`, `cat /proc/meminfo | grep MemTotal`, `cat /proc/meminfo | grep MemFree`;" + "\n")
        f.write("cd " + directory + "\n")
        f.write(command_obj.command + "\n")
        f.close()

        os.system("chmod 777 " + qsub_script_name)
        qsub_command = qsub_executable + " "
        qsub_command += " -V "  # import all environment
        if num_cores:
            qsub_command += " -pe orte " + str(num_cores) + "  "

        qsub_command += " -sync y "  # write err to out
        qsub_command += " -j y "  # write err to out
        qsub_command += " -o " + temp_log_filename + " "
        #qsub_command += " -e " + stderr + " "
        qsub_command += qsub_script_name # + " >& /dev/null"


        spawned_qsub_process = Popen(qsub_command, bufsize = 0, shell=True, cwd=directory)

        # while the job is running, continually read from the temp_log_file and copy this to the job_log
        if temp_log_filename:
            self.copy_command_output_to_log(command_obj, spawned_qsub_process, temp_log_file, job_log)

            temp_log_file.close()
            os.remove(temp_log_filename)
        else:
            spawned_qsub_process.wait()

        if spawned_qsub_process.returncode is not None and spawned_qsub_process.returncode != 0:
            raise Exception("Non-zero return code: " + str(spawned_qsub_process.returncode))


        os.remove(qsub_script_name)


    def copy_command_output_to_log(self, command_obj, spawned_process, temp_log_file, job_log):
        seconds_since_last_log_output = 0
        while True:
            idx = temp_log_file.tell()
            line = temp_log_file.readline()
            if line:
                job_log.write(line, num_tabs=2, cmd_name=command_obj.name)

                if command_obj.output_handler:
                    try :
                        command_obj.output_handler(line)  # give the output handler a chance to process the command output
                    except Exception, e:
                        job_log.writeln("ERROR: output_handler exception: " + str(e), num_tabs=2, cmd_name=command_obj.name)

                seconds_since_last_log_output = 0
            elif spawned_process.poll() is not None:
                break
            elif seconds_since_last_log_output > self.LOG_HEARTBEAT_INTERVAL_MINUTES*60:
                try: 		load_avg = " - mchn. load = " + str(os.getloadavg()[0])
                except:     load_avg = ""
                job_log.writeln("<still running%s>" % load_avg, num_tabs=2, cmd_name=command_obj.name)
                seconds_since_last_log_output = 0
            else:
                time.sleep(1)
                seconds_since_last_log_output += 1
                temp_log_file.seek(idx)

        return

    @staticmethod
    def command_matches_description(command_obj, description):
        """Utility method used to parse command-line arg descriptions of commands given via -b, -f, etc. """
        return description == command_obj.command_id or (len(description) > 2 and command_obj.command.count(description)) or (command_obj.name and command_obj.name.count(description))


    @staticmethod
    def does_command_need_to_run(command_obj, directory, log_stream):
        """
        Checks whether any output_filenames are outdated relative to input files, or if the command has a
        needs_to_run_checker function which returns True.

        Args:
            command_obj: the command to check
            directory: filenames will be considered relative to this directory
            log_stream: where to log output
        """

        # check whether command needs to run based on one or more output_files being out of date relative to the input_file(s)
        most_recent_mtime = 0
        for input_filename in command_obj.input_filenames:
            abs_input_filename = get_absolute_path(input_filename, directory)
            if not os.access(abs_input_filename, os.R_OK):
                log_stream.write("Input  (last mod N/A): %s  [Error - input file not found]\n" % str(input_filename))
                try: raise Exception("File not found: " + str(input_filename))
                except: import traceback; log_stream.write(traceback.format_exc())  # Print a stack trace
                return False

            input_mtime = os.stat(abs_input_filename).st_mtime
            most_recent_mtime = max(input_mtime, most_recent_mtime)
            log_stream.write("Input  (last mod %s): %s\n" % (
                time.strftime("%m/%d/%Y %I:%M:%S %p", time.localtime(os.stat(abs_input_filename).st_mtime)),
                input_filename))

        return_value = False
        for output_filename in command_obj.output_filenames:
            abs_output_filename = get_absolute_path(output_filename, directory)

            if not os.access(abs_output_filename, os.R_OK):
                log_stream.write("Output (last mod N/A): %s  [doesn't exist yet]\n" % output_filename)
                return_value = True
                continue

            output_mtime = os.stat(abs_output_filename).st_mtime
            output_mtime_string = time.strftime("%m/%d/%Y %I:%M:%S %p", time.localtime(output_mtime))
            if output_mtime < most_recent_mtime:
                log_stream.write("Output (last mod %s): %s  [out of date]\n" % (output_mtime_string, output_filename))
                return_value = True
                continue

            log_stream.write("Output (last mod %s): %s  [up to date]\n" % (output_mtime_string, output_filename))

        # if it hasn't already been decided that the command does need to run, and this command has a
        # 'needs_to_run_checker' function attached to it, execute this function (this might be costly).
        if return_value is False and command_obj.needs_to_run_checker:
            current_dir = os.getcwd()
            os.chdir(directory)
            return_value = command_obj.needs_to_run_checker()
            os.chdir(current_dir)
            log_stream.write("NeedToRunChecker returned:  [%s]\n" % str(return_value))

        return return_value



    def _fix_keyboard_interrupt_handling(self):
        """Make program exit on KeyboardInterrupt. This fixes annoying behavior when pipeline calls another python script."""
        def custom_excepthook(exception_type, value, traceback):
            if exception_type is KeyboardInterrupt:
                print("KEYBOARD INTERRUPT DETECTED")
                sys.exit(1)
            else:
                sys.__excepthook__(exception_type, value, traceback)

        sys.excepthook = custom_excepthook


class LogStream:
    """Used to log command output to the screen as well as to one or more output files or streams."""

    def __init__(self, name=None, print_time=True, num_tabs=0, print_to_stdout=True, *streams):
        """
        Args:
            name: the job name
            print_time: whether to print the time on every line
            num_tabs: how much to indent every line
            print_to_stdout: whether to print log to stdout, and not just to the streams
            streams: one or more output streams to write the log output to
        """
        self.name = name
        self.print_time = print_time
        self.print_to_stdout = print_to_stdout
        self.streams = list(streams)
        self.num_tabs = num_tabs

    def set_name(self, name):
        self.name = name

    def add_stream(self, stream):
        self.streams.append(stream)

    def set_print_to_stdout(self, print_to_stdout):
        self.print_to_stdout = print_to_stdout

    def set_print_time(self, print_time):
        self.print_time = print_time

    def set_num_tabs(self, num_tabs):
        self.num_tabs = num_tabs

    def writeln(self, string="", print_time=None, num_tabs=None, cmd_name=""):
        self.write(string + "\n", print_time, num_tabs, cmd_name)

    def write(self, string="", print_time=None, num_tabs=None, cmd_name=""):
        print_time = print_time if print_time is not None else self.print_time
        num_tabs = num_tabs if num_tabs is not None else self.num_tabs

        final_string = ""
        if print_time:
            final_string += "["+time.ctime()[4:19]+"]"
        if self.name:
            final_string += " "+self.name
        if cmd_name:
            final_string += " "+cmd_name

        if print_time or self.name or cmd_name:
            final_string += ": "

        final_string += "    "*num_tabs
        final_string += string

        streams = list(self.streams) + [sys.stdout] if self.print_to_stdout else self.streams
        for stream in streams:
            stream.write(final_string)
            stream.flush()

    def close(self):
        for stream in self.streams:
            stream.close()


def get_absolute_path(filename, directory=None, allow_compressed_version=True):
    if allow_compressed_version:
        filename = get_path_with_compression(filename)
    if directory and not os.path.isabs(filename):
        filename = os.path.join(directory, filename)
    return os.path.abspath(filename)


def get_path_with_compression(path):
    if not os.path.isfile(path):
        if os.path.isfile(path + ".gz"):
            path += ".gz"
        elif os.path.isfile(path + ".bz2"):
            path += ".bz2"
    return path


def open_file(path, mode="r"):

    if mode.startswith("r"):
        path = get_path_with_compression(path)

    if path.endswith(".gz") or path.endswith(".zip"):
        import gzip
        return gzip.open(path, mode)
    elif path.endswith(".bz2"):
        import bz2
        return bz2.BZ2File(path, mode)
    else:
        path = os.path.normpath(path)
        return open(path, mode)


if __name__ == "__main__":
    sys.argv = [sys.argv[0], "-h"]
    JobRunner()
