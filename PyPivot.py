#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Administrator
#
# Created:     09/06/2013
# Copyright:   (c) Administrator 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------


#Coding Methodology
#Define functions as Methods in the Function class
#functions should take as inputs a list of values and perform the necessary calculation as defined by the function
#in the reduce step
#should only have two types of Columns, Aggregation Columns and Row Columns
#use a dictionary to map what function is applied to what column



import sys
import csv

class Mapper:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.headers = []
        self.key_col = []
        self.concatenate_col = []
        self.aggregate_col = []
        self.doc_dict = {}


    def process_record(self, line):
        layer = self.doc_dict
        depth = len(self.key_col)
        counter = 0

        for i in self.key_col:
            e = line[i]
            layer = layer.setdefault(e,{})
            counter += 1

            if counter == depth:
                for j in self.concatenate_col:
                    h = self.headers[j]
                    conc_collection = layer.setdefault(h,[])
                    conc_collection.append(line[j])

                for k in self.aggregate_col:
                    h = self.headers[k]
                    agg = layer.setdefault(h,0)
                    try:
                        layer[h] += float(line[k])
                    except:
                        pass


    def load_csv(self):

        with open(self.csv_file, 'r') as F:
            #self.headers = F.readline()
            csv_reader = csv.reader(F)
            self.headers = csv_reader.next()


    def process_csv(self):

        with open(self.csv_file, 'r') as F:
            #self.headers = F.readline()
            csv_reader = csv.reader(F)
            csv_reader.next()
            for l in csv_reader:
                #print l
                self.process_record(l)



    def print_header_list(self):
        for idx in range(len(self.headers)):
            print str(idx)+': '+self.headers[idx]


class Reducer:
    def __init__(self, doc_dict):
        self.input_dict = doc_dict
        self.headers = []
        self.key_col = []
        self.concatenate_col = []
        self.aggregate_col = []
        self.table = []

    def concatenate_list(self, input_list):
        unique_val = list(set(input_list))
        output = str(unique_val[0])

        for i in unique_val[1:]:
            output += ('; '+str(i))

        return output

    def recurs_build(self, layer=None, max_depth=None, pk_list=[]):
        #converts dictionary data into table for export to CSV

        if layer is None:
            layer=self.input_dict

        if max_depth is None:
            max_depth=len(self.key_col)


        if max_depth == 0:
            for i in B.concatenate_col:
                field = self.concatenate_list(layer[B.headers[i]])
                pk_list.append(field)

            for i in B.aggregate_col:
                field = layer[B.headers[i]]
                pk_list.append(field)

            self.table.append(pk_list)

        else:
            max_depth -= 1
            for key in layer.keys():
                local_list = list(pk_list)
                local_list.append(key)
                self.recurs_build(layer[key], max_depth, local_list)


    def write_csv(self, filename):
        header = []
        field_col = self.key_col + self.concatenate_col + self.aggregate_col
        for i in field_col:
            header.append(self.headers[i])

        with open(filename, 'wb') as F:
            writer = csv.writer(F)
            writer.writerow(header)
            writer.writerows(self.table)











def main():
    sent_file = sys.argv[1]
    A = Mapper(sent_file)
    A.load_csv()
    print '\n'
    A.print_header_list()

    print '\n'
    print 'All responses should be as a list...[a, b, c]'+'\n'
    print 'Indexing of Columns begins at 0...n' + '\n'

    temp = input('Select Columns for Row Labels')
    if type(temp) != type([]) or temp == []:
        print 'Incorrect Data Type or list is empty. Please Resubmit.'
        temp = input('Select Columns for Row Labels')

    A.key_col = temp

    temp = input('Select Columns to Concatenate(unique strings will be listed with a ";" seperation)')
    if type(temp) != type([]):
        print 'Incorrect Data Type. Please Resubmit.'
        temp = input('Select Columns to Concatenate')

    A.concatenate_col = temp

    temp = input('Select Columns to Sum')
    if type(temp) != type([]):
        print 'Incorrect Data Type. Please Resubmit.'
        temp = input('Select Columns to Concatenate')

    A.aggregate_col = temp

    resp = raw_input('Process Pivot...(y/n)')

    if resp.lower() == 'n':
        resp = raw_input('Process Pivot...(y/n)')

    else:
        A.process_csv()
        wait = ('Program Paused Press any key to advance')
        for i in A.doc_dict:
            print i


if __name__ == "__main__":
    pass
    #main()
    B = Mapper('Book2.csv')
    B.key_col = [1, 4]
    B.concatenate_col = [2, 3, 5 ,6, 7, 8, 9]
    B.aggregate_col = [10, 11]
    B.load_csv()
    B.process_csv()

    C = Reducer(B.doc_dict)
    C.key_col = B.key_col
    C.concatenate_col = B.concatenate_col
    C.aggregate_col = B.aggregate_col
    C.headers = B.headers
    C.recurs_build()
    C.write_csv('test_output.csv')
