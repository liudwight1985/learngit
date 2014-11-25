#!/usr/bin/python

import sys

#------------------- SPECIFY THE FILE NAME AND OUTPUT------------------------
from optparse import OptionParser
parser = OptionParser()
parser.add_option("-f", "--fixdata", dest="fixn",
                  help="This is input:type the full name of the input fix-log file or full directory", metavar="FIXDATA_NAME.txt")
parser.add_option("-b", "--tagname", dest="tagn",
                  help="", metavar="This is input:type the full name of the selected tag list file or full directory.")
parser.add_option("-t", "--tablename", dest="tablen",
                  help="please name the table which will show up in the ddl file", metavar="TABLE_NAME")
parser.add_option("-d", "--dictionary", dest="dictn",
                  help="This is input:type the full name of the fix-log dictionary or directory", metavar="DICT.txt")
parser.add_option("-l", "--ddlname", dest="ddln",
                  help="please name the output ddl file", metavar="DDL_NAME.hql")
parser.add_option("-n", "--dataname", dest="datan",
                  help="please name the output normalized fix-data which will be loadable to the ddl file", metavar="NORM_FIX.txt")
parser.add_option("-p", "--platform", dest="plat",
                  help="type operation platform", metavar="PLATFORM")

parser.add_option("-q", "--quiet",
                  action="store_false",dest="verbose",default="True",
                  help="don't print status messages to stdout")

(options, args) = parser.parse_args()

try:
     #(options, args) = parser.parse_args()
     print "input raw fix data: " + options.fixn
     print "the tag list: " + options.tagn
     print "the output table name: " + options.tablen
     print "the input dictionary: " + options.dictn
     print "the ouput name of DDL file: " + options.ddln
     print "the output normalized fix data: " + options.datan
except Exception:
     print "you need to fully specify\n(1)fix data\n(2)the selected tag list\n(3)the referred tag dictionary\n(4)the name of the table wil be shwoed in ddl\n(5)the name of the ddl file\n(6)the name of the normalized fix data\n(7)the operating system  " 
     print "Any argument above cannot be omitted"
     print "You sould specify like this '<your_script_name> -f nameof_fix_data -b nameof_selected_tag_list -d nameof_refered_fix_dictionary -t nameof_table -l nameof_ddl nameof_normalized_fix_data -p nameof_operating_system '"
     sys.exit(0)
try: 
     print "the interpreter: " + options.plat
except Exception:
     options.plat= 0
     print "You didn't specify the operating system but it's ok." 
# ----------------------specify area-------------------
tagfile=options.tagn
fixdict=options.dictn
raw_data=options.fixn
normfix=options.datan
fix_con=options.ddln
fix_table=options.tablen
opsys=options.plat
if opsys:
   opsys=opsys.lower()
# ------------------------LOAD NEED TAG---------------------------------

try:
        file = open(tagfile, 'r')
except IOError:
        print 'cannot open or there is no this file', tagfile
else:
        print tagfile, 'has', len(file.readlines()), 'lines'

file = open(tagfile, 'r')
input=file.read()
input=input.strip()
input=input.split(',')
select_tag_list=[]
for num in input:
    try:      
          if len(num.split('-'))==1:
               select_tag_list.append(num.strip())
          else:
               num_add=num.split('-')
               num_add_list=range(int(num_add[0]),int(num_add[1])+1)
               num_add_list=map(str,num_add_list)
               select_tag_list.extend(num_add_list)
    except Exception:
          print num +" wrong in " + tagfile
          sys.exit(0)
# -----------------CREATE EMPTY HIVE FIX-------------------------------------
print 'CREATE EMPTY HIVE FIX'
try:
        file = open(fixdict, 'r')
except IOError:
        print 'cannot open or there is no this', fixdict
else:
        print fixdict, 'has', len(file.readlines()), 'lines'
        file.close()
file = open(fixdict, 'r')
input=file.read()
pairs = input.split()
tag_dict = dict()
# Make Fix dictionary
for pair in pairs:
    try:
        div=pair.split('|')
        tag_dict[div[0]]=[div[1],div[2]]
    except Exception:
        print  pair + " wrong in " + fixdict
        sys.exit(0)
fc=open(fix_con,'w+')
fc.write('DROP TABLE if exists '+fix_table+';\nCREATE TABLE '+fix_table+'(\n')
fc.write('     filename  STRING\n')
fc.write('    ,linenumber INT\n')

for needtag in select_tag_list:
       name_type = tag_dict[needtag]
       tag_name=name_type[0]
       tag_type=name_type[1]
       fc.write('    ,'+tag_name+' '+tag_type+'\n')
fc.write(")\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY '|'\nLINES TERMINATED BY '\\n'\nSTORED AS TEXTFILE;")
fc.write("\nLOAD DATA LOCAL INPATH './"+normfix+"'\nOVERWRITE INTO TABLE "+fix_table+";")
fc.close()

# --------------------------- LOAD FIX DATA------------------------------------
nonfirst=False
fixtag_list=['']
fix_value_list=['']
needvalue_list=['']*(len(select_tag_list)+2)

try:
        file = open(raw_data, 'r')
except IOError:
        print 'cannot open', raw_data
else:
        print raw_data, 'has', len(file.readlines()), 'lines'
        file.close()
file = open(raw_data, 'r')
input=file.read().splitlines()
input=''.join(input)
input=input.split('|')
pairs=filter(None,input)
numberline=-1

fd=open(normfix,'w+')
for pair in pairs:
    divpair = pair.split('=')
    if int(divpair[0]) == 8:
       numberline=numberline+1
       if(nonfirst):
           iter=2
           for needtag in select_tag_list:
               try:
                  ind_val = fixtag_list.index(needtag)
                  needvalue_list[iter] = fix_value_list[ind_val]
               except ValueError:
                  ind_val = -1
               iter = iter+1
           needvalue_list[0]=raw_data
           needvalue_list[1]=numberline 
           fd.write('|'.join(map(str,needvalue_list))+'\n')
       else:
          nonfirst=True
    fixtag_list.append(divpair[0])
    fix_value_list.append(divpair[1])
fd.close()
### STOP HERE
#sys.exit(0)

#-------------------------------- PROCESS LOADING-------------------------

if(opsys):
   if opsys== "hive" :
      import subprocess
      import os
      print '###### CREATE EMPTY FIX_TABLE ######'
      print fix_con
      s3 = subprocess.call([opsys,"-f",fix_con] )
      print s3
      if (s3):
           print 'CREATE TABLE ERROR'
           exit
      else:
           print 'create empty'+fix_table+' table success'
      print '###### SHOW CONTENT ######'
      print fix_table
      s5=subprocess.call([opsys,"-e","select * from "+fix_table])
      if (s5):
           print 'SHOW CONTENT ERROR'
           exit
      else:
           print 'show content of loaded '+fix_table+ ' table success'

   else:
      print "You specified the interpreter other than in HIVE. So sorry only Hive supported now"
      print "FINISH"
