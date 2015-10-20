import sqlite3, csv, shutil, time
from enum import IntEnum
from pathlib import PurePath


class Database():
    def __init__(self):
        self.connection = sqlite3.connect('..\\db\\sqlite3\\media.db', 10)
        self.cursor = self.connection.cursor()
        self.connection.row_factory = sqlite3.Row
        self.transaction_depth = 0
        self.isolation_level=None
        
    def begin(self, isolation_level=''):
        levels = [None, '', 'immediate', 'exclusive']
        if levels.index(isolation_level) > levels.index(self.isolation_level):
            self.connection.execute('begin ' + isolation_level)
        self.transaction_depth += 1
##        print('begin ' + str(self.transaction_depth))

    def commit(self):
##        print('commit ' + str(self.transaction_depth))
        if self.transaction_depth > 0:
            self.transaction_depth -= 1
        if self.transaction_depth == 0:
            self.connection.commit()

    def rollback(self):
##        print('rollback ' + str(self.transaction_depth))
        if self.transaction_depth > 0:
            self.transaction_depth -= 1
        if self.transaction_depth == 0:
            self.connection.rollback()

    def drop_table(self, connection, tablename):
        self.connection.execute('Drop Table ' + tablename)
        

class ActiveState(IntEnum):
    inactive = 0
    active = 1
    
class CopyState(IntEnum):
    failed = -1
    idle = 0
    busy = 1
    finished = 2

def printrepeatedly(objects):
    if not hasattr(printrepeatedly, 'index'):
        printrepeatedly.index=0
    printrepeatedly.index += 1
    if printrepeatedly.index % 80 == 0:
        print(objects)
    else:
        print(objects, end='')   


class Table:
    def __init__(self, database, tablename):
        self.database = database
        self.connection = database.connection
        self.cursor = database.connection.cursor()
        self.tablename=tablename

    def create(self):
        self.createtable()
        self.createindex()

    def countall(self):
        self.database.begin()
        try:
            row=self.cursor.execute('Select count(*) from ' + self.tablename).fetchone()      
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
            if row:
               return row[0]
            else:
                return 0
    
    def printall(self):        
        self.database.begin()
        try:    
            for row in self.cursor.execute('Select * from ' + self.tablename):
                print(list(row))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def exists(self, columnnames, columnvalues):
        self.database.begin()
        try:
            select = 'select * from ' + self.tablename + ' where ' + ' and '.join(name + '=?' for name in columnnames)
            row = self.cursor.execute(select, columnvalues).fetchone()
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
            return None != row


class TableHost(Table):
    def __init__(self, database):
        super().__init__(database, 'TableHost')

    def createtable(self):
        self.connection.execute('''create table  if not exists TableHost
            (HostID int, HostAddr text, ActiveState int default 0,
            CopyState int default 0, DestID int,
            CreateTime TimeStamp default (datetime('now', 'localtime')))''')

    def createindex(self):
        self.connection.executescript('''
            create index if not exists iHostID on TableHost(HostID);            
            create index if not exists iHostAddr on TableHost(HostAddr);
            ''')
    
    def append(self, csvfile):
        with open(csvfile) as infile:
            reader=csv.DictReader(infile)
            self.database.begin('immediate')
            try:
                for row in reader:
                    if(not self.exists(('HostAddr', ), (row['HostAddr'], ))):
                        self.connection.execute('''Insert Into TableHost(HostID, HostAddr)
                            Values(?, ?)''', (row['HostID'], row['HostAddr']))
                    print('.', end='')
            except:
                self.database.rollback()
                raise
            else:   
                self.database.commit()

    def updatecopystate(self, dirID, copystate):
        self.database.begin()
        try:
            self.cursor.execute('''update TableHost set CopyState=?
                            where HostID in
                            (Select HostID from TableDir where DirID=?)''',
                           (copystate, dirID))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def _zerocopystate(self):
        self.database.begin('exclusive')
        try:
            self.cursor.execute('update TableHost set CopyState=?', (CopyState.idle, ))    
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
        
    def activate(self, hostid, activestate):  
        self.database.begin()
        try:
            self.cursor.execute('''update TableHost set ActiveState=? where HostID=?''',
                           (activestate, hostid))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def activateall(self, activestate):  
        self.database.begin('exclusive')
        try:
            self.cursor.execute('''update TableHost set ActiveState=? ''',
                           (activestate, ))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()


class TableDir(Table):
    def __init__(self, database):
        super().__init__(database, 'TableDir')

    def createtable(self):
        self.connection.execute('''create table  if not exists TableDir
            (DirID int, DirName text, DirSize int, FilesSize int default 0,
            Location text, ActiveState int default 0, CopyState int default 0,
            HostID int, DestID int, 
            CreateTime TimeStamp default (datetime('now', 'localtime')))''')

    def createindex(self):
        self.connection.executescript('''
            create index if not exists iDirID on TableDir(DirID);            
            create index if not exists iHostID on TableDir(HostID);            
            create index if not exists iDestID on TableDir(DestID);
            create index if not exists iDirName on TableDir(DirName, Location);
            ''')

    def update_filessize(self):
        self.database.begin('exclusive')
        try:
            self.connection.executescript('''
                update TableDir set FilesSize =
                        (select sum(FileSize) from TableFile
                        where TableFile.DirID=TableDir.DirID and TableFile.CopyState=0)
                ''')
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
        
    
    def append(self, csvfile):
        with open(csvfile) as infile:
            reader=csv.DictReader(infile)
            self.database.begin('immediate')
            try:
                for row in reader:
                    if(not self.exists(('DirName', 'Location'), (row['DirName'], row['Location']))):
                        self.connection.execute('''Insert Into TableDir
                            (DirID, DirName, DirSize, Location, HostID)
                            Values(?, ?, ?, ?, ?)''',
                            (row['DirID'], row['DirName'], row['DirSize'], row['Location'], row['HostID']))
                print('.', end='')
            except:
                self.database.rollback()
                raise
            else:                   
                self.database.commit()

    def getdir(self, dirid):
        self.database.begin()
        try:
            self.cursor.execute('select * from TableDir where DirID=?',
                                (dirid, ))
            dir = self.cursor.fetchone()
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
            return dir

    def getdirid(self, path):
        self.database.begin()
        try:
            p = PurePath(path)
            p.name, p.parent
    
            if p.parent == '.':
                where = ' Where DirName=?'
                params = (p.name, )
            else:
                where = ' Where DirName=? And Location=?'
                params = (p.name, str(p.parent))
                
            row = self.cursor.execute('Select DirID from TableDir ' + where, params).fetchone()
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
            if row:
                return row[0]
            else:
                return 0

    def updatecopystate(self, dirID, copystate):
        self.database.begin()
        try:
            self.cursor.execute('''update TableDir set CopyState=?
                            where DirID=? ''', (copystate, dirID))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def _zerocopystate(self):
        self.database.begin('exclusive')
        try:
            self.cursor.execute('update TableDir set CopyState=?', (CopyState.idle, ))      
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def activate(self, dirid, activestate):  
        self.database.begin()
        try:
            self.cursor.execute('''update TableDir set ActiveState=? where DirID=?''',
                           (activestate, dirid))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def activateall(self, activestate):  
        self.database.begin('exclusive')
        try:
            self.cursor.execute('''update TableDir set ActiveState=? ''',
                           (activestate, ))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

        
class TableFile(Table):
    def __init__(self, database):
        super().__init__(database, 'TableFile')

    def createtable(self):
        self.connection.execute('''create table if not exists TableFile
            (FileID int, FileName text, ExtName text, FileSize int,
            Location text, ActiveState int default 0, CopyState int default 0,
            DestID int, DirID int, CopyStatus text,
            CreateTime TimeStamp default (datetime('now', 'localtime')),
            CopyTime TimeStamp)''')

    def createindex(self):
        self.connection.executescript('''
            create index if not exists iFileID on TableFile(FileID);            
            create index if not exists iDirID on TableFile(DirID);
            create index if not exists iDestID on TableFile(DestID);
            create index if not exists iFileName on TableFile(FileName, ExtName, Location);
            ''')

    def dropindex(self):
        self.connection.executescript('''
            drop index iFileID;
            drop index iDirID;
            drop index iDestID;
            drop index iFileName;
            ''')

    def append(self, csvfile, repeated = False):
        tableDir = TableDir(self.database)
        with open(csvfile) as infile:
            reader=csv.DictReader(infile)
            self.database.begin('immediate')
            try:
                for index, row in enumerate(reader):
                    if(repeated or (not self.exists(('FileName', 'ExtName', 'Location'),
                                                    (row['FileName'], row['ExtName'], row['Location'])))):
                        dirID = tableDir.getdirid(row['Location'])
                        self.connection.execute('''Insert Into TableFile
                            (FileID, FileName, ExtName, FileSize, Location, DirID)
                            Values(?, ?, ?, ?, ?, ?)''',
                            (row['FileID'], row['FileName'], row['ExtName'], row['FileSize'], row['Location'], dirID))
                    printrepeatedly('.')
            except:
                self.database.rollback()
                raise
            else:                   
                self.database.commit()

    def getfile(self, fileid):
        self.database.begin()
        try:
            self.cursor.execute('select * from TableFile where FileID=?',
                                (fileid, ))
            file = self.cursor.fetchone()
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
            return file

    def getfilefrom(self, dirid, copystate):
        self.database.begin()
        try:
            self.cursor.execute('''select file.* from TableFile as file
                                inner join TableDir as dir on file.DirID=dir.DirID
                                where file.DirID=? and file.CopyState=? and file.ActiveState=? and dir.ActiveState=?''',
                                (dirid, copystate, ActiveState.active, ActiveState.active))
            file = self.cursor.fetchone()
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()        
            return file

    def updatecopystate(self, fileid, destid, copystate, copystatus = None):
        self.database.begin()
        try:
            self.cursor.execute('''update TableFile set DestID=?, CopyState=?, CopyStatus=? where FileID=?''',
                           (destid, copystate, copystatus, fileid))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def updatecopystateofdir(self, dirid, destid, copystate, copystatus = None):
        self.database.begin()
        try:
            self.cursor.execute('''update TableFile set DestID=?, CopyState=?, CopyStatus=? where DirID=?''',
                           (destid, copystate, copystatus, dirid))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
        
    def _zerocopystate(self):
        self.database.begin('exclusive')
        try:
            self.cursor.execute('update TableFile set DestID=NULL, CopyState=? ', (CopyState.idle, ))      
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def activate(self, fileid, activestate):  
        self.database.begin()
        try:
            self.cursor.execute('''update TableFile set ActiveState=? where FileID=?''',
                           (activestate, fileid))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def activateall(self, activestate):  
        self.database.begin('exclusive')
        try:
            self.cursor.execute('''update TableFile set ActiveState=? ''',
                           (activestate, ))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def activateofdir(self, dirid, activestate):  
        self.database.begin('exclusive')
        try:
            self.cursor.execute('''update TableFile set ActiveState=? where DirID=?''',
                           (activestate, dirid))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

                
class TableDest(Table):
    def __init__(self, database):
        super().__init__(database, 'TableDest')

    def createtable(self):
        self.connection.execute('''create table  if not exists TableDest
            (DestID int, DiskSN text, DiskBatch text, DiskModel text,
            DiskCapacity int, DiskPath text, ActiveState int default 0, CopyState int default 0, 
            CreateTime TimeStamp default (datetime('now', 'localtime')))''')

    def createindex(self):
        self.connection.executescript('''
            create index if not exists iDestID on TableDest(DestID);
            create index if not exists iDiskSN on TableDest(DiskSN, DiskBatch);
            ''')

    def append(self, csvfile):
        num=0
        with open(csvfile) as infile:
            reader=csv.DictReader(infile)
            for row in reader:
                print('.', end='')
                self.database.begin('immediate')
                try:
                    if(not self.exists(('DiskBatch', 'DiskSN'), (row['DiskBatch'], row['DiskSN']))):
                        self.connection.execute('''Insert Into TableDest
                            (DestID, DiskBatch, DiskSN, DiskModel, DiskCapacity, DiskPath)
                            Values(?, ?, ?, ?, ?, ?)''',
                            (row['DestID'], row['DiskBatch'], row['DiskSN'], row['DiskModel'], row['DiskCapacity'], row['DiskPath']))
                except:
                    self.database.rollback()
                    raise
                else:                   
                    self.database.commit()

    def getdest(self, destid):
        self.database.begin()
        try:
            self.cursor.execute('select * from TableDest where DestID=?',
                           (destid, ))
            dest = self.cursor.fetchone()
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
            return dest

    def updatecopystate(self, destinationID, copystate):
        self.database.begin()
        try:
            self.cursor.execute('''update TableDest set CopyState=? where DestID=?''',
                         (copystate, destinationID))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def _zerocopystate(self):
        self.database.begin('exclusive')
        try:
            self.cursor.execute('update TableDest set CopyState=?', (CopyState.idle, ))      
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def activate(self, destid, activestate):  
        self.database.begin()
        try:
            self.cursor.execute('''update TableDest set ActiveState=? where DestID=?''',
                           (activestate, destid))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

    def activateall(self, activestate):  
        self.database.begin('exclusive')
        try:
            self.cursor.execute('''update TableDest set ActiveState=? ''',
                           (activestate, ))
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()

class TableTask(Table):
    def __init__(self, database):
        super().__init__(database, 'TableTask')

    def createtable(self):
        self.cursor.execute('''create table if not exists TableTask
            (TaskID INTEGER PRIMARY KEY AUTOINCREMENT, DestID, DirID int, CopyState int default 0)
            ''')

    def createindex(self):
        self.cursor.executescript('''
            create index if not exists iDestID on TableTask(DestID);
            create index if not exists iDirID on TableTask(DirID);
            ''')

    def dropindex(self):
        self.cursor.executescript('''
            drop index iDestID;
            drop index iDirID;
            ''')

    def gettask(self, taskid):
        self.database.begin()
        try:
            task = self.cursor.execute('''select * from TableTask where taskID=?''',
                                       (taskid, )).fetchone()
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
            return task

    def removetask(self, taskID):
        self.database.begin()
        try:
            self.cursor.execute('''delete from TableTask where TaskID=?''', (taskID, ))
        except:
            self.database.rollback()
            raise
        else:
            self.database.commit()

    def updatecopystate(self, taskid, copystate):
        self.database.begin()
        try:
            self.cursor.execute('''update TableTask set CopyState=? where TaskID=?''',
                                (copystate, taskid))      
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
        
        
class TaskManager:
    def __init__(self, database):
        self.database = database
        self.connection = database.connection
        self.cursor = database.connection.cursor()
        self.tablehost = TableHost(database)
        self.tabledir = TableDir(database)
        self.tablefile = TableFile(database)
        self.tabledest = TableDest(database)
        self.tabletask = TableTask(database)

    def updatecopystate(self, taskid, copystate):
        self.database.begin()
        try:
            task = self.tabletask.gettask(taskid)
            if copystate == CopyState.busy or copystate == CopyState.failed:
                self.tabletask.updatecopystate(task['TaskID'], copystate)
                self.tabledir.updatecopystate(task['DirID'], copystate)            
                self.tablehost.updatecopystate(task['DirID'], copystate)
                self.tabledest.updatecopystate(task['DestID'], copystate)
            else:
                self.tabletask.updatecopystate(task['TaskID'], copystate)
                self.tabledir.updatecopystate(task['DirID'], copystate)            
                self.tablehost.updatecopystate(task['DirID'], CopyState.idle)               
                self.tabledest.updatecopystate(task['DestID'], CopyState.idle)
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
            
    def getdestmax(self):
        self.database.begin()
        try:
            dest = None
            freeusage = 0
            for row in self.cursor.execute('''select * from TableDest
                                     where ActiveState=? and CopyState=? ''',
                                     (ActiveState.active, CopyState.idle)):
                usage = shutil.disk_usage(row['DiskPath'])
                if usage.free > freeusage:
                    freeusage = usage.free  
                    dest = row    
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
            return dest    	

    def requesttask(self):           
        self.database.begin('immediate')
        try:
            task = self.cursor.execute('''select Task.* from TableTask as Task 
                    inner join TableDest on Task.DestID=TableDest.DestID
                    inner join TableDir on Task.DirID=TableDir.DirID
                    inner join TableHost on TableDir.HostID=TableHost.HostID
                    where Task.CopyState=? 
                    and TableDest.ActiveState=? and TableDest.CopyState=? 
                    and TableHost.ActiveState=? and TableHost.CopyState=?''', 
                    (CopyState.idle, ActiveState.active, CopyState.idle, 
                    ActiveState.active, CopyState.idle)).fetchone()

            if not task:
                dest = self.getdestmax()
                if dest:     
                    freeusage = shutil.disk_usage(dest['DiskPath']).free -  pow(2, 30)
                    if freeusage < 0:
                        freeusage = 0
                    directory = self.cursor.execute('''select * from TableDir
                        inner join TableHost on TableDir.HostID=TableHost.HostID
                        where TableDir.ActiveState=? and TableDir.CopyState=? and TableDir.FilesSize<? 
                        and TableHost.ActiveState=? and TableHost.CopyState=?
                        order by TableDir.FilesSize desc''',
                        (ActiveState.active, CopyState.idle, freeusage, 
                         ActiveState.active, CopyState.idle)).fetchone()
                    if directory:
                        self.cursor.execute('''insert into TableTask(DestID, DirID)
                            Values(?, ?) ''', 
                            (dest['DestID'], directory['DirID']))
                        task = self.cursor.execute('''select * from TableTask where DestID=? and DirID=?''',
                                              (dest['DestID'], directory['DirID'])).fetchone()
                    else:
                        self.tabledest.updatecopystate(dest['DestID'], CopyState.finished) 
            if task:
                self.updatecopystate(task['TaskID'], CopyState.busy)
                task = self.tabletask.gettask(task['TaskID'])
        except:
            self.database.rollback();
            raise
        else:
            self.database.commit()
            return task       
    
    def _zerocopystate(self):
        self.database.begin('exclusive')
        try:
            self.connection.execute('delete from TableTask')
            self.tablehost._zerocopystate()
            self.tabledir._zerocopystate()
            self.tablefile._zerocopystate()
            self.tabledest._zerocopystate()
        except:
            self.database.rollback()
            raise
        else:   
            self.database.commit()
                        
    def build_database(self):
        self.tablehost.create()
        self.tabledir.create()
        self.tablefile.create()
        self.tabledest.create()       
        self.tabletask.create()
        self.database.begin('exclusive')
        try:
            self.tablehost.append('./dictionary/StorageHost.csv')
            self.tabledir.append('./dictionary/StorageDir.csv')
            self.tablefile.append('./dictionary/StorageFile.csv')
            self.tabledest.append('./dictionary/Destination.csv')
        except:
            self.database.rollback()
            raise
        else:
            self.database.commit()

    def activateall(self, activestate):
        self.database.begin('exclusive')
        try:
            self.tablehost.activateall(activestate)
            self.tabledir.activateall(activestate)
            self.tablefile.activateall(activestate)
            self.tabledest.activateall(activestate)
        except:
            self.database.rollback()
            raise
        else:
            self.database.commit()

    def activatetask(self, taskid, activestate):
        self.database.begin()
        try:
            task = self.tabletask.gettask(taskid)
            self.tabledest.activate(task['DestID'], activestate)
            self.tabledir.activate(task['DirID'], activestate)            
        except:
            self.database.rollback()
            raise
        else:
            self.database.commit()            
        
if(__name__=='__main__'):
    db = Database()
    tablehost = TableHost(db)
    tabledir = TableDir(db)
    tablefile = TableFile(db)
    tabledest = TableDest(db)
    tabletask = TableTask(db)
    manager = TaskManager(db)
#    tablehost.append('./dictionary/TableHost.csv')
#    tabledir.append('./dictionary/TableDir.csv')
#    tablefile.append('./dictionary/TableFile.csv')
#    tabledest.append('./dictionary/TableDest.csv')                                    
                                    
    
        
