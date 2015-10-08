
import csv, sys, io, platform
from pathlib import Path
from datetime import date

def isHidenFile(filePath):
    if 'Windows' in platform.system():
        import win32file,win32con
        fileAttr = win32file.GetFileAttributes(filePath)
        if fileAttr & win32con.FILE_ATTRIBUTE_HIDDEN :
            return True
        return False
    return False

def output(text):
    try:
        text=str(text)
        sys.stdout.write(text)
    except UnicodeEncodeError:
        bytes = text.encode(sys.stdout.encoding, 'backslashreplace')
        if hasattr(sys.stdout, 'buffer'):
            sys.stdout.buffer.write(bytes)
        else:
            text = bytes.decode(sys.stdout.encoding, 'strict')
            sys.stdout.write(text)

class PathInfo():
    def __init__(self, path):
        self.path = path    
        self.size = 0
        self.filenum = 0
        self.dirnum = 0
        self.maxlayer = 0

    def __str__(self):
        dateformat='%Y/%m/%d'
        ctime=date.fromtimestamp(self.path.stat().st_ctime).strftime(dateformat)
        utime=date.fromtimestamp(self.path.stat().st_mtime).strftime(dateformat)
        result = ','.join([str(self.path.parent),
                         self.path.stem,
                         self.path.suffix,
                         ctime,
                         utime,
                         str(self.size),
                         str(self.filenum),
                         str(self.dirnum),
                         str(self.maxlayer)])
        result += '\n'
        return result

def query_path(path):  
    result = PathInfo(path)
    if path.is_dir():
        for child in path.iterdir():
##            if isHidenFile(child):
##                continue
            pathinfo = query_path(child)
            result.size += pathinfo.size
            result.dirnum += pathinfo.dirnum
            result.filenum += pathinfo.filenum
            if child.is_dir():
                result.dirnum += 1
            else:
                result.filenum += 1
            if result.maxlayer < pathinfo.maxlayer:
                result.maxlayer = pathinfo.maxlayer
        result.maxlayer += 1
    else:
        result.size = path.stat().st_size

    output(result)
        
    return result       
    
def main():
##    infilename = 'locations_resources.csv'
    if len(sys.argv)<=1:
        print('请提供需要检索的文件夹路径', file=sys.stderr)
        return
    
    infilename = sys.argv[1]

    print('Location,FileName,ExtName,CreateTime,UpdateTime,Size,FileNum,DirNum,MaxLayer')
    with open(infilename, newline='') as infile:
        reader=csv.DictReader(infile)
        for row in reader:
            path=Path(row['Location'])
            query_path(path)


if __name__=='__main__':
    main()

    
    
