import pymongo
import time
from scipy import stats
from multiprocessing import Pool

from util import DocumentProvider

def get_document(batch, ident):
    for i in batch:
        if i._doc["_id"] == ident:
            return i._doc


class PerfTestTrial():
    """
    Test

    A base/abstract class for running a performance test

    """
    def __init__(self, connString, dbName):
        """

        """
        self.runTime = 0
        self.testName = "BasePerfTest"
        self.connString = connString
        self.dbName     = dbName
    def runTestTrial(self):
        """
        :return:
        """

class BulkInsertTestTrial(PerfTestTrial):
    """
    Bulk Insert Test Trial

    A performance test that inserts many documents with a given batch size
    """
    def __init__(self, connString, dbName, numThreads, numDocsToInsert, insertBatchSize, documentProvider):
        super().__init__(connString, dbName)
        self.testName           = "BulkInsert"
        self.collName           = self.testName.lower() + "." + documentProvider.__class__.__name__.lower()
        self.numThreads         = numThreads
        self.numDocsToInsert    = numDocsToInsert
        self.insertBatchSize    = insertBatchSize
        self.documentProvider   = documentProvider
    def runTestTrialThread(self, testIdx):
        # Perform inserts
        batch = []
        errors = []
        runTime = 0
        client = pymongo.MongoClient(self.connString)
        mongoColl = client[self.dbName][self.collName].with_options(
            write_concern=pymongo.write_concern.WriteConcern(w=1)
        )
        for i in range(0, self.numDocsToInsert):
            batch.append(pymongo.InsertOne(self.documentProvider.createDocument(testIdx, i)))

            if i % self.insertBatchSize == 0:
                startTime = time.time()
                try:
                    mongoColl.bulk_write(batch, ordered=False)
                except pymongo.errors.BulkWriteError as e:
                    for x in e.details[u'writeErrors']:
                        error_id = x[u'op']['_id']
                        errors.append(get_document(batch, error_id))
                runTime += (time.time() - startTime)
                batch = []
        startTime = time.time()
        try:
            mongoColl.bulk_write(batch, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            for x in e.details[u'writeErrors']:
                error_id = x[u'op']['_id']
                errors.append(get_document(batch, error_id))
        runTime += (time.time() - startTime)
        batch = []
        return runTime
    def runTestTrial(self):
        mongoClient = pymongo.MongoClient(self.connString)
        coll = mongoClient[self.dbName][self.collName].with_options(
            write_concern=pymongo.write_concern.WriteConcern(w=1)
        )

        # Drop collection if it already exists
        coll.drop()
        coll.create_index(self.documentProvider.getIndex(), name="myIndex")

        startTime = time.time()
        with Pool(processes=self.numThreads) as pool:
            bulkInsertFutures = [pool.apply_async(self.runTestTrialThread, ("thread" + str(i),)) for i in range(self.numThreads)]
            for bulkInsertFuture in bulkInsertFutures:
                # print("Thread runtime: {} ".format(bulkInsertFuture.result()))
                bulkInsertFuture.get()
        self.runTime += (time.time() - startTime)
            # print("--------- {} Ran in {} seconds ---------".format(self.testName, str(self.runTime)))

class SingleInsertTestTrial(PerfTestTrial):
    """
    Single Insert Test Trial
    """
    def __init__(self, connString, dbName, documentProvider):
        super().__init__(connString, dbName)
        self.testName = "SingleInsert"
        self.documentProvider = documentProvider
        self.collName = self.testName.lower() + "." + documentProvider.__class__.__name__.lower()
    def runTestTrial(self):
        # Drop collection if it already exists
        mongoClient = pymongo.MongoClient(self.connString)
        coll = mongoClient[self.dbName][self.collName].with_options(
            write_concern=pymongo.write_concern.WriteConcern(w=1)
        )
        coll.drop()
        coll.create_index(self.documentProvider.getIndex(), name="myIndex")

        # Perform inserts
        startTime = time.time()
        try:
            coll.insert_one(self.documentProvider.createDocument("test0", 1))
        except pymongo.errors.WriteError as e:
            print("Encountered write error: {}".format(e))
        self.runTime += (time.time() - startTime)

class EqualityQueryTestTrial(PerfTestTrial):
    """
    Equality Query Test Trial
    """
    def __init__(self, connString, dbName, documentProvider, matchNum):
        super().__init__(connString, dbName)
        self.testName = "EqualityQueryTest"
        self.documentProvider = documentProvider
        self.collName = BulkInsertTestTrial.__name__.lower() + "." + documentProvider.__class__.__name__.lower()
        self.matchNum = matchNum
    def runTestTrial(self):
        mongoClient = pymongo.MongoClient(self.connString)
        coll = mongoClient[self.dbName][self.collName].with_options(
            write_concern=pymongo.write_concern.WriteConcern(w=1)
        )

        startTime = time.time()
        try:
            coll.find_one(self.documentProvider.getEqMatchingCriteria("test0", self.matchNum))
        except pymongo.errors.WriteError as e:
            print("Encountered write error: {}".format(e))
        self.runTime += (time.time() - startTime)

class RangedQueryTestTrial(PerfTestTrial):
    """
    Ranged Query Test Trial
    """
    def __init__(self, connString, dbName, documentProvider, matchNum):
        super().__init__(connString, dbName)
        self.testName = "RangedQueryTest"
        self.documentProvider = documentProvider
        self.collName = BulkInsertTestTrial.__name__.lower() + "." + documentProvider.__class__.__name__.lower()
        self.matchNum = matchNum
    def runTestTrial(self):
        mongoClient = pymongo.MongoClient(self.connString)
        coll = mongoClient[self.dbName][self.collName].with_options(
            write_concern=pymongo.write_concern.WriteConcern(w=1)
        )

        startTime = time.time()
        try:
            coll.find_one(self.documentProvider.getRangeMatchingCriteria("test0", self.matchNum))
        except pymongo.errors.WriteError as e:
            print("Encountered write error: {}".format(e))
        self.runTime += (time.time() - startTime)


class PerfTest():
    """
    Per Test

    A class that supports multiple test trials with results

    """
    def __init__(self, connString, dbName, testName, numTrials):
        self.connString = connString
        self.dbName     = dbName
        self.testName   = testName
        self.numTrials  = numTrials
        self.trials     = []
        self.trialRunTime = []
    def runTest(self):
        for i in range(0, self.numTrials):
            self.trials[i].runTestTrial()
            self.trialRunTime.append(self.trials[i].runTime)
        trialStats = stats.describe(self.trialRunTime)
        print("Test results: {}".format(trialStats))

class BulkInsertTest(PerfTest):
    """
    Bulk Insert Test

    A PerfTest class that conducts multiple test trials for Bulk Inserts

    """
    def __init__(self, connString, dbName, numTrials, numThreads, numDocsToInsert, insertBatchSize, documentProvider):
        super().__init__(connString, dbName, "BulkInsertTest", numTrials)
        self.numDocsToInsert = numDocsToInsert
        self.insertBatchSize = insertBatchSize
        self.documentProvider = documentProvider
        for i in range(0, numTrials):
            self.trials.append(BulkInsertTestTrial(connString, dbName, numThreads, numDocsToInsert, insertBatchSize, documentProvider))


class SingleInsertTest(PerfTest):
    """
    Single Insert Test

    A POC test to test and confirm db connectivity
    """
    def __init__(self, connString, dbName, documentProvider):
        super().__init__(connString, dbName, "SingleInsertTest", 100)
        for i in range(0, self.numTrials):
            self.trials.append(SingleInsertTestTrial(connString, dbName, documentProvider))

class EqualityQueryTest(PerfTest):
    """
    Equality Query Test

    """
    def __init__(self, connString, dbName, numTrials, documentProvider):
        super().__init__(connString, dbName, "EqualityQueryTest", numTrials)
        mongoClient = pymongo.MongoClient(self.connString)
        self.collName = "bulkinsert." + documentProvider.__class__.__name__.lower()
        coll = mongoClient[self.dbName][self.collName].with_options(
            write_concern=pymongo.write_concern.WriteConcern(w=1)
        )
        collSize = coll.count()
        for i in range(0, numTrials):
            psuedoRandomNum = (i*1000+10) % collSize
            self.trials.append(EqualityQueryTestTrial(connString, dbName, documentProvider, psuedoRandomNum))

class RangedQueryTest(PerfTest):
    """
    Equality Query Test

    """
    def __init__(self, connString, dbName, numTrials, documentProvider):
        super().__init__(connString, dbName, "RangedQueryTest", numTrials)
        mongoClient = pymongo.MongoClient(self.connString)
        self.collName = "bulkinsert." + documentProvider.__class__.__name__.lower()
        coll = mongoClient[self.dbName][self.collName].with_options(
            write_concern=pymongo.write_concern.WriteConcern(w=1)
        )
        collSize = coll.count()
        for i in range(0, numTrials):
            psuedoRandomNum = (i*1000+10) % collSize
            self.trials.append(RangedQueryTestTrial(connString, dbName, documentProvider, psuedoRandomNum))