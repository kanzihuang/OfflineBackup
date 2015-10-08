import shutil, os, time, database

from pathlib import PurePath

db = database.Database()
conn = db.connection
cursor = conn.cursor()
manager = database.TaskManager(db)
tablehost = database.TableHost(db)
tabledir = database.TableDir(db)
tablefile = database.TableFile(db)
tabledest = database.TableDest(db)
tabletask = database.TableTask(db)


def run():
    while True:
        task = manager.requesttask()
        if task:
            try:
                copyfiles(task['DestID'], task['DirID'])
            except OSError as why:
                print(why)
                manager.updatecopystate(task['TaskID'], database.CopyState.idle)
            else:
                manager.updatecopystate(task['TaskID'], database.CopyState.finished)
        else:
            print('The process is idle.')

        time.sleep(10)

def getfilepath(file):
    return str(PurePath(file['Location']).joinpath(file['FileName'] + file['ExtName']))

def log(destid, dirname, filename, extname, srcfilepath):
    destid = str(destid)
    logname = '..\\log\\' + destid + '.log'
    with open(logname, 'a') as logfile:
      logfile.write(','.join((destid, dirname, filename, extname, srcfilepath)) + '\n')
            
def copyfiles(destid, dirid):
    dest = tabledest.getdest(destid)
    directory = tabledir.getdir(dirid)
    srcdirpath = str(PurePath(directory['Location']).joinpath(directory['DirName']))
    destdirpath = str(PurePath(dest['DiskPath']).joinpath(directory['DirName']))
    makedirs(destdirpath)
    while True:
        if not (os.path.exists(srcdirpath) and os.path.exists(destdirpath)):
            raise OSError('No such direcory') 
            
        file = tablefile.getfilefrom(dirid, database.CopyState.idle)
        if file:
            srcfilepath = getfilepath(file)
            destfilepath = str(PurePath(destdirpath).joinpath(file['FileName'] + file['ExtName']))
            try:
                copyfile(srcfilepath, destfilepath)
            except OSError as why:
                print(why)
                tablefile.updatecopystate(file['FileID'], destid, database.CopyState.failed, str(why))
                os.remove(destfilepath)
            else:
                tablefile.updatecopystate(file['FileID'], destid, database.CopyState.finished)
                log(destid, directory['DirName'], file['FileName'], file['ExtName'], srcfilepath)
        else:
            break;

def makedirs(dirpath):
    print(dirpath)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
    
def copyfile(srcfilepath, destfilepath):
    print(destfilepath)
    shutil.copy2(srcfilepath, destfilepath)       
##    printrepeatedly('.')

##if __name__ == '__main__':
##    run()




