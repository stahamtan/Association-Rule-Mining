from __future__ import division
from string import join
import time
from datetime import date
from itertools import combinations
import mysql.connector

# Database connection specifications:

mysqlConfig = {
    'user': 'your_username',
    'password': 'your_passowrd',
    'host': 'localhost',
    'database': 'ARM',      # your DB name (ARM: Association Rule Mining)
    'port': '3306'
}

class Rule:
    def __init__(self):
        self.consequent = set()     # A set of products (from Product class)
        self.antecedent = set()     # A set of products (from Product class)
        self.support = 0
        self.confidence = 0

    def __str__(self):
        return (join(map(str, self.antecedent), ', ') + ' ---> ' + join(map(str, self.consequent), ', ') + '\n' +
                'Support = ' + str(self.support) + '\n' +
                'Confidence = ' + str(self.confidence) + '\n')


class Product:
    def __init__(self):
        self.ID = None
        self.name = None
        self.price = None
        self.type = None

    def __str__(self):
        return self.Name


class Receipt:
    def __init__(self):
        self.ID = None
        self.date = None
        self.items = set()      # A set of product ID's


    def __str__(self):
        return 'Receipt Number: ' + str(self.ID) + '\t' + 'Transaction Date: ' + str(self.date)


def getAllProduct():
    """
    Requirements: Database Table
                        1. goods
                            1. Product ID
                            2. Product Name
                            3. Product Price
                            4. Product Type (if applicable)


    :return:    **Data Type: Dictionary     --> (Key: Product ID ; Value: Product Instance)

    """

    global mysqlConfig
    con1 = mysql.connector.Connect(**mysqlConfig)
    cursor = con1.cursor()
    cursor.execute("SELECT * FROM goods")

    ProductDict = {}

    try:

        for aProduct in cursor:
            g = Product()
            g.ID = aProduct[0]
            g.name = aProduct[1]
            g.price = aProduct[2]
            g.type = aProduct[3]

            ProductDict[g.ID] = g

        return ProductDict

    except Exception as err:
        print str(err)
        return None

    finally:

        cursor.close()
        con1.close


def getAllReceipts():

    """
    Requirements: Database Tables
                        1. receipts
                            1. Receipt Number
                            2. Transaction Date

                        2. items
                            1. Receipt Number
                            2. Product ID

    :return:    **Data Type: Dictionary     --> (Key: Receipt ID ; Value: Receipt Instance)

    """

    global mysqlConfig

    con1 = mysql.connector.connect(**mysqlConfig)
    con2 = mysql.connector.connect(**mysqlConfig)

    receiptsCursor = con1.cursor()
    itemsCursor = con2.cursor()

    receiptsCursor.execute("SELECT * FROM receipts")
    receiptsDict = {}

    try:

        for aReceipt in receiptsCursor:
            r = Receipt()
            r.ID = aReceipt[0]
            r.date = aReceipt[1]

            itemsCursor.execute("SELECT Item FROM items WHERE Receipt = '%s' " %r.ID)
            for anItem in itemsCursor:
                r.items.add(anItem[0])

            receiptsDict[r.ID] = r
        return receiptsDict

    except Exception as err:
        print str(err)
        return None

    finally:
        receiptsCursor.close()
        itemsCursor.close()

        con1.close
        con2.close


def generate_k_itemset(ListOfItems, k):

    """
    :param ListOfItems:     **Data Type: List       --> A list of Item ID's - ID can be string or numeric
    :param k:               **Data Type: Int        --> The number of items in an itemset; e.g. k=3 means 3-Itemset
    :return:                **Data Type: List       --> List of all k-itemsets

    """

    return list(combinations(ListOfItems, k))


def Frequent_Itemset(ListOfItems, Transactions, k, minsupport = 0.5):

    """
    :param ListOfItems:     **Data Type: List       --> contains product ID's - ID may be string or numeric
    :param Transactions:    **Data Type: List       --> contains transactions - each transaction must be a Set()
    :param k:               **Data Type: Int        --> The number of items in an itemset; e.g. k=3 means 3-Itemset
    :param minsupport:      **Data Type: Float      --> Minimum support to only return those having greater support
    :return:                **Data Type: Dictionary --> (Key: Itemset ; Value= Support)

    """

    totalNumberOfTransactions = len(Transactions)

    # Generate K-Itemset
    k_itemsets = generate_k_itemset(ListOfItems, k)

    # A dictionary to store itemsets and their support values
    itemset_support = dict()

    for itemset in k_itemsets:

        count = len([trans for trans in Transactions if set(itemset).issubset(trans)])
        sup = count/totalNumberOfTransactions

        if sup >= minsupport:
            itemset_support[itemset] = sup

    return itemset_support


def generate_rules(ListOfItems, Transactions, k, minsupport = 0.5, minConfidence = 0.5):

    # 1. Generate frequent itemsets

    startFrequentItemsets = time.time()

    frequent_k_itemsets = Frequent_Itemset(ListOfItems, Transactions, k, minsupport)

    endFrequentItemsets = time.time()

    rulesList = list()

    # Start of rule generation
    startRuleGeneration = time.time()

    # 2. Loop through frequent itemsets
    for k_itemset in frequent_k_itemsets:

        for i in range(1, k):

            # 3. Generate a list of all combination of items as antecedent for each k-itemset
            comblist = list(combinations(k_itemset, i))

            # 4. Create rule for each antecedent combination
            for aComb in comblist:
                r = Rule()

                # 5. Add items in each combination to rule's antecedent set
                r.antecedent = set(aComb)
                r.consequent = set(k_itemset) - r.antecedent

                # 6. Calculate confidence for each rule
                count_antecedent = len([trans for trans in Transactions if r.antecedent.issubset(trans)])
                count_itemset = len([trans for trans in Transactions if set(k_itemset).issubset(trans)])
                r.confidence = count_itemset/count_antecedent
                r.support = frequent_k_itemsets[k_itemset]

                # 7. Add a rule only if its confidence is greater or equal to minConfidence
                if r.confidence >= minConfidence:
                    rulesList.append(r)

    # End of rule generation
    endRuleGeneration = time.time()

    print "Frequent Itemsets generation time: ", (endFrequentItemsets - startFrequentItemsets)
    print "Rule generation time: ", (endRuleGeneration - startRuleGeneration), '\n\n'

    return rulesList
