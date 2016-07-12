Overview
~~~~~~~~

A simple module for specifying and executing a sequence of commands, with up to 1 level of branching (where several tasks are executed in parallel) before 
continuing with a single line of execution. 


Features
~~~~~~~~

- executes a sequences of command lines, allowing some steps to run in parallel, before proceeding sequentially again.
- save command standard output to a log file.
- use temp output files for each command until it completes successfully. Then move them to their final location.
- by default don't rerurn steps if the output file(s) exist and are newer than the input file(s).
- run locally by default or on SGE (with --use_SGE) or LSF (with --use_LSF).



