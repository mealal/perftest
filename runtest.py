import argparse
import logging
import threading
import concurrent.futures

from util import StringValueDocumentProvider, IntegerValueDocumentProvider, NestedDocumentProvider, kb50DocumentProvider, mb1DocumentProvider
from tests import PerfTest, BulkInsertTest, SingleInsertTest, EqualityQueryTest, RangedQueryTest


# Default constant variables
NUM_RUNS_DEFAULT            = 1000
NUM_DOCS_PER_RUN_DEFAULT    = 10000
NUM_THREADS_DEFAULT         = 1
BATCH_SIZE_DEFAULT          = 1000
DB_NAME_DEFAULT             = "perftest"
DOCUMENT_PROVIDER_DEFAULT   = "StringValueDocumentProvider"


# Other global variables
scriptName                  = "runtest.py"

########################################################################################################################
# Misc Methods
########################################################################################################################



########################################################################################################################
# Setup Methods
########################################################################################################################

def _configureLogger(logLevel):
    format = '%(message)s'
    if logLevel != 'INFO':
        format = '%(levelname)s: %(message)s'
    logging.basicConfig(format=format, level=logLevel.upper())

# def gitVersion():
#     """
#     Git Version
#
#     Gets the git revision (the sha denoting the revision) of the current versionselfself.
#     If the local version of the code does not have .git metadata files returns the
#     version of the script as indicated by version and revDate variables above.
#     """
#     def _getGitCommitSha(cmd):
#         # construct minimal environment
#         env = {}
#         for k in ['SYSTEMROOT', 'PATH']:
#             v = os.environ.get(k)
#             if v is not None:
#                 env[k] = v
#         # LANGUAGE is used on win32
#         env['LANGUAGE'] = 'C'
#         env['LANG'] = 'C'
#         env['LC_ALL'] = 'C'
#         out = subprocess.Popen(cmd, stdout = subprocess.PIPE, env = env).communicate()[0]
#         return out
#
#     commitDate = "N/A"
#     gitVersion = "Unknown"
#     try:
#         # Get last commit version
#         out        = _getGitCommitSha(['git', 'rev-parse', 'HEAD'])
#         gitVersion = out.strip().decode('ascii')
#
#         # Get last commit date
#         out        = _getGitCommitSha(['git', 'show', '-s', '--format=%ci'])
#         commitDate = out.strip().decode('ascii')
#     except OSError:
#         gitVersion = version
#         commitDate = revdate
#         logging.error("Unable to get the version")
#
#     return {
#         'version': gitVersion,
#         'date': commitDate
#     }


def runTest(perfTest):
    perfTest.runTest()

def main(args):
    """
    Main

    args:       An argparse object containing the values of  the command line arguments
    """
    # _configureLogger(args.logLevel.upper())
    # versionInfo = gitVersion()
    print("\n")
    # logging.info("Running {} v({}) last modified {}".format(scriptName, versionInfo['version'][:8], versionInfo['date']))
    print("Running {} ".format(scriptName))
    print("Using command line args: {}\n".format(str(args)))
    # checkOsCompatibility()

    # TODO generate test conditions first THEN inject client. Can we do this in a clever way without too much memory consumption
    numRuns             = int(args.numRuns)
    numDocs             = int(args.numDocs)
    numThreads          = int(args.numThreads)

    batchSize           = int(args.batchSize)

    # Get document provider
    documentProviders = []
    if args.documentProvider.lower() == StringValueDocumentProvider.__name__.lower():
        documentProviders.append(StringValueDocumentProvider(10))
    elif args.documentProvider.lower() == IntegerValueDocumentProvider.__name__.lower():
        documentProviders.append(IntegerValueDocumentProvider())
    elif args.documentProvider.lower() == NestedDocumentProvider.__name__.lower():
        documentProviders.append(NestedDocumentProvider())
    elif args.documentProvider.lower() == kb50DocumentProvider.__name__.lower():
        documentProviders.append(kb50DocumentProvider())
    elif args.documentProvider.lower() == mb1DocumentProvider.__name__.lower():
        documentProviders.append(mb1DocumentProvider())
    else:
        documentProviders.append(StringValueDocumentProvider(10))
        documentProviders.append(IntegerValueDocumentProvider())
        documentProviders.append(NestedDocumentProvider())
        documentProviders.append(kb50DocumentProvider())
        documentProviders.append(mb1DocumentProvider())

    connStrings = args.dbConnStrings.split(";")
    for connString in connStrings:
        for documentProvider in documentProviders:

            # try:
            borderStr       = "###############################################################"
            titleStr        = "Running tests on db {}".format(connString)
            titleStr2       = "Using Document Provider {}".format(documentProvider.__class__.__name__)
            bufferStrLen    = int(max(float((len(borderStr) - len(titleStr))/2), 0))
            bufferStr       = " "*bufferStrLen
            bufferStr2Len   = int(max(float((len(borderStr) - len(titleStr2))/2), 0))
            bufferStr2      = " "*bufferStr2Len

            print(borderStr)
            print(bufferStr + titleStr + bufferStr)
            print(bufferStr2 + titleStr2 + bufferStr2)
            print(borderStr)

            print("------------Testing Connection with a Single Insert------------")
            singleWritePerTest = SingleInsertTest(connString, args.dbName, documentProvider)
            singleWritePerTest.runTest()
            print("\n")

            threads = 1
            if args.cumulThreads:
                while threads <= numThreads:
                    print("------------Running Bulk Insert Test on {} Threads------------".format(threads))
                    numDocsPerThread = int(numDocs / threads)
                    bulkInsertTest = BulkInsertTest(connString, args.dbName, numRuns, threads, numDocsPerThread, batchSize, documentProvider)
                    bulkInsertTest.runTest()
                    print("\n")
                    threads = threads*2
            else:
                print("------------Running Bulk Insert Test on {} Threads------------".format(numThreads))
                numDocsPerThread = int(numDocs / numThreads)
                bulkInsertTest = BulkInsertTest(connString, args.dbName, numRuns, numThreads, numDocsPerThread, batchSize, documentProvider)
                bulkInsertTest.runTest()
                print("\n")

            print("------------Running Equality Query Test------------")
            eqQueryTest = EqualityQueryTest(connString, args.dbName, numRuns, documentProvider)
            eqQueryTest.runTest()
            print("\n")

            print("------------Running Ranged Query Test------------")
            rangedQueryTest = RangedQueryTest(connString, args.dbName, numRuns, documentProvider)
            rangedQueryTest.runTest()
            print("\n")

                # print("------------Running $in Equality Query Test------------")
            # except Exception as e:
            #     print("Encountered error {}".format(e))

            # TODO print test to table

def setupArgs():
    """
    Setup args
    Parses all command line arguments to the script
    """
    parser = argparse.ArgumentParser(description='Runs a deterministic load against multiple MongoDBs')
    parser.add_argument('--numRuns',        required=False, action="store",         dest='numRuns',         default=NUM_RUNS_DEFAULT,           help='The number of runs in the test')
    parser.add_argument('--numDocs',        required=False, action="store",         dest='numDocs',         default=NUM_DOCS_PER_RUN_DEFAULT,   help='The number of docs per test run')
    parser.add_argument('--numThreads',     required=False, action="store",         dest='numThreads',      default=NUM_THREADS_DEFAULT,        help='Num of threads to run parallel tests on')
    parser.add_argument('--cumulThreads',   required=False, action="store_true",    dest='cumulThreads',    default=False,                      help='Include this flag if tests should build up from 1 to numThreads in powers of 2')
    parser.add_argument('--batchSize',      required=False, action="store",         dest='batchSize',       default=BATCH_SIZE_DEFAULT,         help='The batch size for inserts')
    parser.add_argument('--dbConnStrings',  required=True,  action="store",         dest='dbConnStrings',   default=None,                       help='Semi-colon delimitted list of connection strings')
    parser.add_argument('--dbName',         required=False, action="store",         dest='dbName',          default=DB_NAME_DEFAULT,            help='Name of the database into which data will be inserted')
    parser.add_argument('--documentProvider',required=False,action="store",         dest='documentProvider',default=DB_NAME_DEFAULT,            help='Type of document to insert/read')
    # TODO support for async inserts
    return parser.parse_args()


def runScript():
    """
    Run Script

    Runs the functionality of the script, assuming command line arguments have
    not yet been parsed
    """
    args = setupArgs()
    main(args)

#-------------------------------
if __name__ == "__main__":
    runScript()