// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client()
{
    // ec = new extent_client();
}

chfs_client::chfs_client(std::string port_extent_server)
{
    // printf("construct ec before\n");
    ec = new extent_client(port_extent_server);
    // printf("construct ec middle\n");
    if (ec->put(1, "") != extent_protocol::OK)
    // if (ec->create(extent_protocol::types::T_DIR,1) != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
    // printf("construct ec end\n");
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

std::string 
chfs_client::make_dir_entry(inum num,std::string name)
{  
    return filename(num)+"//"+name+"//";
}

int 
chfs_client::increase_time_of_dir_file(inum num)
{
    int r = OK;
    // printf("increase time begin\n");
    extent_protocol::attr a;
    if (ec->getattr(num, a) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }
    a.mtime++;
    a.ctime++;
    ec->setattr(num,a);

    return r;
}   

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        // printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        // printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    // printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        return true;
    } 

    return false;
}

bool
chfs_client::issymlink(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        return true;
    } 

    return false;
}


int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    // printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    // printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    // printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    // printf("setattr begin\n");

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    extent_protocol::attr attr;
    if(OK!=ec->getattr(ino,attr))
        return NOENT;
    attr.size=size;
    ec->setattr(ino,attr);


    return r;
}

/*
mode can be ignored
*/
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    // printf("begin create file\n");
    //check id dir
    if(!isdir(parent))
        return NOENT;

    //check file whether exist
    bool found=false;
    inum tmp_inode;
    if(OK!=lookup(parent,name,found,tmp_inode))
    {
        // printf("create look up error\n");
        return NOENT;
    }
    if(found==true)
    {
        // printf("file already exist\n");
        return EXIST;
    }
    // printf("same name not exist\n");
    //create file
    if(OK!=ec->create(extent_protocol::T_FILE,ino_out))
        return NOENT;

    //modify parent info
    std::string dir_buf="";
    if(OK!=ec->get(parent,dir_buf))
        return NOENT;

    // std::cout<<"create:"<<std::string(name)<<" inum:"<<filename(ino_out)<<"$"<<std::endl;

    dir_buf+=make_dir_entry(ino_out,std::string(name));

    //write back
    if(OK!=ec->put(parent,dir_buf))
        return NOENT;

    increase_time_of_dir_file(parent);

    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    // printf("mkdir begin\n");

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    if(!isdir(parent))
        return NOENT;

    bool found=false;
    inum tmp_inode;
    if(OK!=lookup(parent,name,found,tmp_inode))
    {
        // printf("mkdir lookup error\n");
        return NOENT;
    }
    if(found==true)
    {
        // printf("dir already exist\n");
        return EXIST;
    }

    if(OK!=ec->create(extent_protocol::T_DIR,ino_out))
        return NOENT;

    //modify parent info
    std::string dir_buf="";
    if(OK!=ec->get(parent,dir_buf))
        return NOENT;

    dir_buf+=make_dir_entry(ino_out,std::string(name));

    //write back
    if(OK!=ec->put(parent,dir_buf))
        return NOENT;

    increase_time_of_dir_file(parent);

    return r;
}

/*
directory format
|<--inode_num-->|//|<--name-->|\0|//|
*/
int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    // printf("look up begin\n");
    // printf("look up name:%s parent:%lld\n",name,parent);
    if(!isdir(parent))
    {
        // printf("look up parent is not dir\n");
        return NOENT;
    }
    std::string dir_buf="";
    if(extent_protocol::OK!=ec->get(parent,dir_buf))
    {
        return NOENT;
    }
    // printf("look up loop begin\n");
    std::cout<<"dir buf:"<<dir_buf<<std::endl;
    int len=dir_buf.size();
    int pos=0;

//todo:lookup one more line
    while (pos<len)
    {
        std::string num_str,tmp_name;
        inum inode_num;

        while(pos<len&&dir_buf[pos]!='/')
        {
            num_str+=dir_buf[pos];
            ++pos;
        }
        inode_num=stoi(num_str);

        pos+=2;
        
        while(pos+1<len&&(dir_buf[pos]!='/'||(dir_buf[pos]=='/'&&dir_buf[pos+1]!='/')))
        {
            tmp_name+=dir_buf[pos];
            ++pos;
        }

        pos+=2;

        // printf("look up:%d$  name:%s\n",inode_num,tmp_name.c_str());

        if(tmp_name.compare(name)==0)
        {
            // printf("look up find\n");
            found=true;
            ino_out=inode_num;
            return r;
        }
    }
    found=false;
    // printf("look up end\n");
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    // printf("readdir begin\n");

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    if(!isdir(dir))
    {
        // printf("not dir\n");
        return NOENT;
    }
    std::string dir_buf="";
    if(extent_protocol::OK!=ec->get(dir,dir_buf))
    {
        return NOENT;
    }
    int len=dir_buf.size();
    int pos=0;

//todo:lookup one more line
    while (pos<len)
    {
        std::string num_str,tmp_name;
        inum inode_num;

        while(pos<len&&dir_buf[pos]!='/')
        {
            num_str+=dir_buf[pos];
            ++pos;
        }
        inode_num=stoi(num_str);

        pos+=2;
        
        while(pos+1<len&&(dir_buf[pos]!='/'||(dir_buf[pos]=='/'&&dir_buf[pos+1]!='/')))
        {
            tmp_name+=dir_buf[pos];
            ++pos;
        }

        pos+=2;

        // std::cout<<"reddir:"<<inode_num<<" name:"<<tmp_name<<std::endl;

        list.push_back(dirent(tmp_name,inode_num));
    }

    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    // printf("read file begin ino:%lld size:%lld off:%d\n",ino,size,off);

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string read_buf;
    if(OK!=ec->get(ino,read_buf))
        return NOENT;

    if(off+size>read_buf.size())
    {
        data=read_buf;
    }
    else
    {
        data=read_buf.substr(off,size);
    }

    // printf("read content:%s\n",data.c_str());
    printf("$read$:%s\n",data);
    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    printf("$write$:%s\n",data);

    // printf("write file begin inum:%lld off:%d size:%d\n",ino,off,size);

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    fileinfo fin;
    getfile(ino,fin);
    int fsize=fin.size;
    
    std::string read_buf,write_buf;
    size_t tmp_byte=0;
    bytes_written=0;
    if(OK!=ec->get(ino,read_buf))
        return NOENT;
    if(off>read_buf.size())
    {
        write_buf+=read_buf+std::string(off-read_buf.size(),'\0');
        tmp_byte+=(off-read_buf.size());
        write_buf+=std::string(data,size);
        tmp_byte+=size;
    }
    else if(off+size>fsize)
    {
        write_buf+=read_buf.substr(0,off);//from 0 to char before off
        write_buf+=std::string(data,size);
        tmp_byte+=size;
    }
    else
    {
        write_buf+=read_buf.replace(off,size,std::string(data,size));
        tmp_byte+=size;
    }

    
    
    bytes_written=tmp_byte;

    if(OK!=ec->put(ino,write_buf))
        return NOENT;

    int file_size=(fsize>write_buf.size())?fsize:write_buf.size();
    setattr(ino,file_size);
    // printf("write end file size:%d buf_size:%d",file_size,write_buf.size());
    increase_time_of_dir_file(ino);
    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    // printf("unlink begin name:%s\n",name);

    std::list<dirent> dir;
    inum inum_to_delete;
    bool find=false;
    std::string dir_buf="";
    
    if(OK!=readdir(parent,dir))
        return NOENT;
    for(auto it=dir.begin();it!=dir.end();++it)
    {
        dirent file=*it;

        if(file.name.compare(std::string(name))==0)
        {
            find=true;
            inum_to_delete=file.inum;
            // std::cout<<"unlink node: "<<inum_to_delete<<std::endl;
            continue;
        }

        dir_buf+=make_dir_entry(file.inum,std::string(file.name));

        
    }

    if(!find) {return NOENT;}

    // std::cout<<"dir buf after unlink: "<<dir_buf<<std::endl;

    ec->put(parent,dir_buf);
    ec->remove(inum_to_delete);
    increase_time_of_dir_file(parent);

    return r;
}


int chfs_client::create_symbolic_link(inum parent,const char *link,const char *name,inum & ino)
{
    // printf("create symbolic link:%s name:%s parent:%lld\n",link,name,parent);
    bool found=false;
    inum tmp_inode;



    //look up
    if(OK!=lookup(parent,name,found,tmp_inode))
    {
        // printf("symbolic link look up error\n");
        return NOENT;
    }
    if(found==true)
    {
        // printf("symbolic link already exist\n");
        return EXIST;
    }

    if(OK!=ec->create(extent_protocol::T_SYMLINK,ino))
    {
        // printf("symbolic create error\n");
        return IOERR;
    }

    // printf("symbolic create success ino:%d\n",ino);

    if(OK!=ec->put(ino,std::string(link)))
    {
        return IOERR;
    }

    //modify parent info
    std::string dir_buf="";
    if(OK!=ec->get(parent,dir_buf))
        return IOERR;

    dir_buf+=make_dir_entry(ino,std::string(name));

    // std::cout<<"make dir buf:"<<dir_buf<<std::endl;

    //write back
    if(OK!=ec->put(parent,dir_buf))
        return IOERR;

    increase_time_of_dir_file(parent);

    // printf("create symbolic link end\n");

    return OK;
}


int chfs_client::readlink(inum ino,std::string& link)
{
    // std::cout<<"read symbolic link"<<std::endl;
    if(!issymlink(ino)) return NOENT;
    ec->get(ino,link);
    // std::cout<<"link:"<<link<<std::endl;
    return OK;
}

