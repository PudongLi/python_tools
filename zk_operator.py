#!/usr/bin/env python3

# encoding: utf-8

'''

@author: lipd

@file: zk_operator

@time: 2017/9/1 14:56

@desc:

'''
import sys
import os
import getopt
import time

from kazoo.client import KazooClient

class ZKOperator:


    def __init__(self):
        self.zk = None
        self.IsConn = False
        self.Hosts = "10.255.224.96:2181,10.255.224.96:2181,10.255.224.98:2181"
        self.filename = ''
        self.pattern = ''
        self.process_path = ''

    def connect(self):
        self.zk = KazooClient(self.Hosts)
        self.zk.start()
        self.IsConn = True
        return self.zk


    def upload_file(self, args):
        """

        :param args: args[0] local file
                     args[1] zk node
        :return:
        """
        self.connect()
        local_file = args[0]
        zk_node = args[1]
        if not os.path.isfile(local_file):
            print('%s does not exist' % local_file)
            sys.exit()
        file_size = os.path.getsize(local_file)
        if file_size > 1048576:
            print("%s: `%s': Local file maximum limit of 1M" % ('upload', local_file))
            sys.exit(0)

        if self.zk.exists(zk_node):
            self.zk.delete(zk_node, recursive=True)
        with open(local_file, 'rb') as file:
            data = file.read()

        self.zk.create(zk_node)
        self.zk.set(zk_node, value=data)
        print('upload file success')


    def create_filenamepool(self, args):
        self.connect()
        types = ['LTECNGI', 'LTECNGO']
        #types = ['CNGI', 'CNGO']
        
        provs = ['100','200','210','220','230','240','250','270','280','290','311','351','371','431','451','471','531','551','571','591','731','771','791','851','871','891','898','931','951','991','971']
        #today = time.strftime("%Y%m%d", time.localtime(time.time()))
        today = "20171213"
        zk_node = args[0]
        begin_seq = '0000'
        if len(args) > 1 :
            begin_seq = args[1]
        begin_seq = begin_seq.zfill(4)
        if self.zk.exists(zk_node):
            self.zk.delete(zk_node, recursive=True)
        self.zk.create(zk_node)
        for type in types:
            for prov in provs:
                node = (("%s.%s.%s.%s") % (today, begin_seq, prov, type))
                print(zk_node + "/" + node)
                self.zk.create((zk_node + "/" + node))
        print(" create filename_pool success")


    def download_file(self, args):
        self.connect()
        zk_node = args[0]
        local_path = args[1]
        data, stat = self.zk.get(zk_node)
        if not self.zk.exists(zk_node):
            print("%s:%s': Zookeeper does no exists" % ('download', zk_node))
            sys.exit(0)
        with open(local_path, 'w') as f:
            f.writelines(data.decode())
            print('download file success')

    def get_node(self, args):
        self.connect()
        zk_node = args[0]
        if not self.zk.exists(zk_node):
            print("%s does not exists" % zk_node)
        nodes = self.zk.get_children(zk_node)
        print(nodes)

    def delete_node(self, args):
        self.connect()
        zk_node = args[0]
        if not self.zk.exists(zk_node):
            print("%s does not exists" % zk_node)
        self.zk.delete(zk_node, recursive=True)
        print('delete node success')

    def parase_command_line(self, argv):

        try:
            opts, args = getopt.getopt(argv[1:], 'dulfrh')
            print(type(args))
            opt = opts[0][0]
            if opt == '-d':
                self.download_file(args)
            elif opt == '-u':
                self.upload_file(args)
            elif opt == '-l':
                self.get_node(args)
            elif opt == '-r':
                self.delete_node(args)
            elif opt == '-f':
                self.create_filenamepool(args)
            elif opt == '-h':
                print('-d:download              --arg1:zk_node    arg2:local path')
                print('-u:upload                --arg1:localfile  arg2:zk_node')
                print('-l:get list              --arg1:zk_node')
                print('-f:create filename_pool  --arg1:filenamepool_node   arg2:seq(default=0)')
                print('-r:deleteall             --arg1:zk_node')
                sys.exit()

        except getopt.GetoptError as e:
            print(e)





if __name__ == '__main__':
    zo = ZKOperator()
    zo.parase_command_line(sys.argv)


