import string
import json

class DocumentProvider():
    """
    Document Provider

    "Abstract Class" for creating varios types of documents
    """

    def __init__(self):
        """

        """
    def createDocument(self, testIdx, num):
        """
        Create Document

        :return:
        """
        return { "testIdx" : testIdx, "value" : num }
    def getEqMatchingCriteria(self, testIdx, num):
        """
        Get Equality Matching Criteria
        :param num:
        :return:
        """
        return { "value" : num }
    def getRangeMatchingCriteria(self, testIdx, num):
        """

        :param num:
        :return:
        """
        return { "value" : { "$gt" : num } }
    def getIndex(self):
        """
        Get Index

        Returns the index that should be created to satisfy queries implied by getEqMatchingCriteria() and getRangeMatchingCriteria()

        :return:
        """
        return "value"



class IntegerValueDocumentProvider(DocumentProvider):
    """
    Integer Value Document Provider

    A class that deterministically creates documents
    """
    def __init__(self):
        DocumentProvider.__init__(self)
    def createDocument(self, testIdx, num):
        """
        Create Document

        :param num:
        :return:
        """
        return { "testIdx" : testIdx, "value1" : num, "value2" : num*num }

    def getEqMatchingCriteria(self, testIdx, num):
        """
        Get Equality Matching Criteria
        :param num:
        :return:
        """
        return { "value1" : num }

    def getRangeMatchingCriteria(self, testIdx, num):
        """

        :param num:
        :return:
        """
        return { "value 1" : { "$gt" : num } }
    def getIndex(self):
        """
        Get Index

        Returns the index that should be created to satisfy queries implied by getEqMatchingCriteria() and getRangeMatchingCriteria()

        :return:
        """
        return "value1"



class StringValueDocumentProvider(DocumentProvider):
    """
    Regex Document Provider
    """
    def __init__(self, length):
        DocumentProvider.__init__(self)
        self.alphaBet = string.ascii_lowercase
        self.alphaBetLength = len(self.alphaBet)
        self.length = length
    def getStr(self, num):
        myStr = ""
        if self.alphaBetLength > num + self.length:
            myStr = self.alphaBet[num:num + self.length]
        else:
            factor = int((self.length + num) / self.alphaBetLength) + 1
            myStr = (self.alphaBet * factor)
            myStr = myStr[num:num + self.length]
        return myStr
    def createDocument(self, testIdx, num):
        """
        Create Document

        :param num:
        :return:
        """
        return { "testIdx" : testIdx, "value1" : self.getStr(num) }
    def getEqMatchingCriteria(self, testIdx, num):
        """
        Get Equality Matching Criteria

        :param num:
        :return:
        """
        return {"value1": self.getStr(num)}

    def getRangeMatchingCriteria(self, testIdx, num):
        """
        Get Range Matching Criteria

        :param num:
        :return:
        """
        return { "value 1" : { "$gt" : self.getStr(num) } }
    def getIndex(self):
        """
        Get Index

        Returns the index that should be created to satisfy queries implied by getEqMatchingCriteria() and getRangeMatchingCriteria()

        :return:
        """
        return "value1"



class NestedDocumentProvider(DocumentProvider):
    """
    Nested Document Provider
    """
    def __init__(self):
        DocumentProvider.__init__(self)
    def createDocument(self, testIdx, num):
        """
        Create Document

        :param testIdx:
        :param num:
        :return:
        """
        myDoc = {
            "testIdx" : testIdx,
            "value1"  : {
                "nestedValue" : {
                    "nestedValue1" : num,
                    "nestedValue2" : num*num
                }
            },
            "value2"  : {
                "nestedValue" : num
            }
        }
        return myDoc
    def getEqMatchingCriteria(self, testIdx, num):
        """
        Get Equality Matching Criteria

        :param testIdx:
        :param num:
        :return:
        """
        return { "value1.nestedValue.nestedValue1" : num }
    def getRangeMatchingCriteria(self, testIdx, num):
        """
        Get Range Matching Criteria

        :param testIdx:
        :param num:
        :return:
        """
        return { "value1.nestedValue.nestedValue1" : { "$gt" : num } }
    def getIndex(self):
        """
        Get Index

        Returns the index that should be created to satisfy queries implied by getEqMatchingCriteria() and getRangeMatchingCriteria()

        :return:
        """
        return "value1.nestedValue.nestedValue1"

class kb50DocumentProvider(DocumentProvider):
    """
    Integer Value Document Provider

    A class that deterministically creates documents
    """
    def __init__(self):
        DocumentProvider.__init__(self)
        f = open("50kb.json","r")
        self.doc = f.read()
    def createDocument(self, testIdx, num):
        """
        Create Document

        :param num:
        :return:
        """
        return json.loads(self.doc.replace("$id", testIdx).replace("$value", str(num)))
    def getEqMatchingCriteria(self, testIdx, num):
        """
        Get Equality Matching Criteria
        :param num:
        :return:
        """
        return { "value" : num }

    def getRangeMatchingCriteria(self, testIdx, num):
        """

        :param num:
        :return:
        """
        return { "value" : { "$gt" : num } }
    def getIndex(self):
        """
        Get Index

        Returns the index that should be created to satisfy queries implied by getEqMatchingCriteria() and getRangeMatchingCriteria()

        :return:
        """
        return "value"

class mb1DocumentProvider(DocumentProvider):
    """
    Integer Value Document Provider

    A class that deterministically creates documents
    """
    def __init__(self):
        DocumentProvider.__init__(self)
        f = open("1mb.json","r")
        self.doc = f.read()
    def createDocument(self, testIdx, num):
        """
        Create Document

        :param num:
        :return:
        """
        return json.loads(self.doc.replace("$id", testIdx).replace("$value", str(num)))
    def getEqMatchingCriteria(self, testIdx, num):
        """
        Get Equality Matching Criteria
        :param num:
        :return:
        """
        return { "value" : num }

    def getRangeMatchingCriteria(self, testIdx, num):
        """

        :param num:
        :return:
        """
        return { "value" : { "$gt" : num } }
    def getIndex(self):
        """
        Get Index

        Returns the index that should be created to satisfy queries implied by getEqMatchingCriteria() and getRangeMatchingCriteria()

        :return:
        """
        return "value"