#include "inode_manager.h"
#include <strings.h>
// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}


void
disk::read_block(blockid_t id, char *buf)
{
  char* src=(char*)blocks[id];
  memcpy(buf,src,BLOCK_SIZE);
  // printf("disk read:%s\n",buf);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  printf("write block id:%d$\n",id);
  char* dst=(char*)blocks[id];
  memcpy(dst,buf,BLOCK_SIZE);
}

// block layer -----------------------------------------

bool 
block_manager::is_block_free(uint32_t id)
{
  uint32_t block_id=1+id/BPB,remain=id%BPB;
  char buf[BLOCK_SIZE];
  d->read_block(block_id,buf);
  char byte=buf[remain/8];
  uint32_t bit_in_byte=remain%8;
  char new_byte=byte&(1<<bit_in_byte);
  if(new_byte==0) return true;
  return false;

}

void 
block_manager::set_block_in_bitmap(uint32_t id)
{
  uint32_t block_id=1+id/BPB,remain=id%BPB;
  char buf[BLOCK_SIZE];
  d->read_block(block_id,buf);
  char byte=buf[remain/8];
  uint32_t bit_in_byte=remain%8;
  char new_byte=byte|(1<<bit_in_byte);
  buf[remain/8]=new_byte;
  d->write_block(block_id,buf);
}

void
block_manager::unset_block_in_bitmap(uint32_t id)
{
  uint32_t block_id=1+id/BPB,remain=id%BPB;
  char buf[BLOCK_SIZE];
  d->read_block(block_id,buf);
  char byte=buf[remain/8];
  uint32_t bit_in_byte=remain%8;
  char new_byte=byte&(~(1<<bit_in_byte));
  buf[remain/8]=new_byte;
  d->write_block(block_id,buf);
}

void
block_manager::indirect_free_block(uint32_t id,unsigned int size)
{
  char tmp[BLOCK_SIZE];
  d->read_block(id,tmp);
  int len=sizeof(blockid_t),cursor=0;
  for(;cursor<size*len&&cursor<BLOCK_SIZE;cursor+=len)
  {
    printf("indirec free num:%d\n",cursor/len);
    uint32_t tmp_id;
    memcpy(&tmp_id,tmp+cursor,len);
    free_block(tmp_id);
  }
}


// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */

  uint32_t bitmap_blocks=BLOCK_NUM/BPB;
  uint32_t start_free_block=1+bitmap_blocks;
  for(uint32_t i=start_free_block;i<BLOCK_NUM;++i)
  {
    if(is_block_free(i))
    {
      //set bitmap
      set_block_in_bitmap(i);
      printf("allocate block:%d\n",i);
      return i;
    }
  }

  return 0;
}

uint32_t 
block_manager::alloc_data_block()
{
  uint32_t bitmap_blocks=BLOCK_NUM/BPB;
  uint32_t start_free_block=1+bitmap_blocks+INODE_NUM/IPB;
  for(uint32_t i=start_free_block;i<BLOCK_NUM;++i)
  {
    if(is_block_free(i))
    {
      //set bitmap
      set_block_in_bitmap(i);
      printf("allocate data block:%d\n",i);
      return i;
    }
  }

  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  unset_block_in_bitmap(id);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  // printf("read block %d\n",id);
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  // printf("write block %d\n",id);
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  test_cnt++;
  for(int i=1;i<INODE_NUM;++i)
  {
    if(get_inode(i)==NULL)
    {
      struct inode* tmp=(struct inode*)malloc(sizeof(struct inode));
      memset(tmp,0,sizeof(struct inode));
      tmp->type=type;
      tmp->atime=test_cnt;//test
      put_inode(i,tmp);
      // printf("allocate node:%d\n",i);
      return i;
    }
  }
  
  return 0;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  struct inode* node=get_inode(inum);
  memset(node,0,sizeof(struct inode));
  put_inode(inum,node);
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 1 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  if (ino == NULL)
    return;

  // printf("\tim: put_inode %d atime %d type %d size %d block %d\n", inum, ino->atime,ino->type,ino->size,IBLOCK(inum, bm->sb.nblocks),bm->sb.nblocks);

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  

  struct inode *node=get_inode(inum);
  if(node==NULL) return;
  unsigned int file_size=node->size;
  // printf("read file inum:%d atime:%d type:%d size:%d\n",inum,node->atime,node->type,node->size);
  unsigned int cursor=0,idx=0;
  unsigned int remain_size=file_size%BLOCK_SIZE;
  *size=file_size;
  *buf_out=(char*)malloc(file_size);
  blockid_t* blocks=node->blocks;

  
  for(;(cursor+BLOCK_SIZE<=file_size)&&(idx<NDIRECT);cursor+=BLOCK_SIZE,++idx)
  {
    blockid_t id=node->blocks[idx];
    bm->read_block(id,*buf_out+cursor);
  }

  if(idx==NDIRECT)
  {
    // printf("indirect\n");//test
    char indirec[BLOCK_SIZE];
    blockid_t id=blocks[idx];
    unsigned int indirect_cursor=0,id_len=sizeof(blockid_t);
    bm->read_block(id,indirec);
    // printf("read whole indirec block:%d len:%d\n",id,id_len);
    for(;(cursor+BLOCK_SIZE<=file_size)&&(indirect_cursor<BLOCK_SIZE);cursor+=BLOCK_SIZE,indirect_cursor+=id_len)
    {
      blockid_t tmp_id;
      memcpy(&tmp_id,indirec+indirect_cursor,id_len);
      bm->read_block(tmp_id,*buf_out+cursor);
      // printf("read indirec from:%d indirec_cursor:%d cursor:%d\n",tmp_id,indirect_cursor,cursor);//test
    }
    if(cursor<file_size)
    {
     
      blockid_t tmp_id;
      memcpy(&tmp_id,indirec+indirect_cursor,id_len);
      char remain_block[BLOCK_SIZE];
      bm->read_block(tmp_id,remain_block);
      memcpy(*buf_out+cursor,remain_block,file_size-cursor);
      // printf("indirect remain size:%d\n",file_size-cursor);//test
      // printf("read indirec from:%d indirec_cursor:%d cursor:%d\n",tmp_id,indirect_cursor,cursor);//test
      // printf("read indirec content:%s\n",remain_block);
      // printf("%d ",tmp_id);//test
    }
    return;
  }
  else
  {
    if(cursor<file_size)
    {
      // printf("ordinary remain cursor:%d index:%d remain_size:%d\n",cursor,idx,file_size-cursor);//test
      char remain_block[BLOCK_SIZE];
      bm->read_block(blocks[idx],remain_block);
      memcpy(*buf_out+cursor,remain_block,file_size-cursor);
      // printf("remain : %d %s\n",idx,remain_block);//test
      return;
    }
  }

  

  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  
  struct inode* node=get_inode(inum);
  unsigned int min_size=MIN(size,node->size);
  node->size=(unsigned int)size;
  int index=0;
  unsigned int cursor=0;

  // printf("write file inum:%d$ size:%d\n",inum, size);//test
 
  for(;(index<NDIRECT)&&(cursor+BLOCK_SIZE<=size);++index,cursor+=BLOCK_SIZE)
  {
    uint32_t block_id;
    if(cursor>=min_size) block_id=bm->alloc_data_block();
    else block_id=node->blocks[index];

    char tmp[BLOCK_SIZE+1];
    memcpy(tmp,buf+cursor,BLOCK_SIZE);
    tmp[BLOCK_SIZE]='\0';
    bm->write_block(block_id,tmp);
    node->blocks[index]=block_id;
    // printf("write direct id:%d index:%d\n",block_id, index);//test
    // printf("write tmp:%s\n",tmp);
    //read test
    // char tmp2[BLOCK_SIZE+1];
    // bm->read_block(block_id,tmp2);
    // tmp2[BLOCK_SIZE]='\0';
    // printf("read tmp:%s\n",tmp2);
  }

  if(index==NDIRECT)
  {
    // printf("write indirect\n");//test

    uint32_t block_id;
    char origin_indirec_block[BLOCK_SIZE];
    if(cursor>=min_size) block_id=bm->alloc_data_block();
    else 
    {
      block_id=node->blocks[index];
      bm->read_block(block_id,origin_indirec_block);
    }

    node->blocks[index]=block_id;
    char tmp[BLOCK_SIZE];
    unsigned int indirect_cursor=0,id_len=sizeof(blockid_t);
    for(;(cursor+BLOCK_SIZE<=size)&&(indirect_cursor<BLOCK_SIZE);cursor+=BLOCK_SIZE,indirect_cursor+=id_len)
    {
      uint32_t indirec_block_id;
      if(cursor>=min_size) indirec_block_id=bm->alloc_data_block();
      else{
        memcpy(&indirec_block_id,origin_indirec_block+indirect_cursor,id_len);
      } 

      memcpy(tmp+indirect_cursor,&indirec_block_id,id_len);
      char tmp_copy[BLOCK_SIZE];
      memcpy(tmp_copy,buf+cursor,BLOCK_SIZE);
      bm->write_block(indirec_block_id,tmp_copy);
      // printf("write indirect id:%d cursor:%d indirec_cursor:%d\n",indirec_block_id,cursor,indirect_cursor);//test
    }
    if(cursor<size)
    {
      uint32_t indirec_block_id;
      if(cursor>=min_size) indirec_block_id=bm->alloc_data_block();
      else{
        memcpy(&indirec_block_id,origin_indirec_block+indirect_cursor,id_len);
      } 

      memcpy(tmp+indirect_cursor,&indirec_block_id,id_len);
      char tmp_copy[BLOCK_SIZE];
      memcpy(tmp_copy,buf+cursor,size-cursor);
      bm->write_block(indirec_block_id,tmp_copy);
      // printf("write indirect remain id:%d cursor:%d indirec_cursor:%d\n",indirec_block_id,cursor,indirect_cursor);//test
      // printf("write indirec remain:%s\n",tmp_copy);
    }
    
    // //write back indirect block
    bm->write_block(block_id,tmp);
    put_inode(inum,node);
    return;
  }
  else
  {
    if(cursor<size)
    {
      // printf("write reamin\n");//test
      // printf("oridinary write remain cursor:%d index:%d remain_size:%d content:%s\n",cursor,index,size-cursor,buf+cursor);
      uint32_t block_id;
      if(cursor>=min_size) block_id=bm->alloc_data_block();
      else block_id=node->blocks[index];

      char tmp[BLOCK_SIZE];
      memcpy(tmp,buf+cursor,size-cursor);
      // printf("content:%s\n",tmp);
      bm->write_block(block_id,tmp);
      node->blocks[index]=block_id;

      put_inode(inum,node);

      // printf("--------------\n");
      return;
    }
  }
  put_inode(inum,node);

  // printf("--------------\n");
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode* node=get_inode(inum);
  // printf("get attr atime:%d\n",node->atime);
  if(node!=NULL)
  {
    a.atime=node->atime;
    a.ctime=node->ctime;
    a.mtime=node->mtime;
    a.size=node->size;
    a.type=node->type;
  }
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  struct inode* node=get_inode(inum);
  unsigned int size=node->size;
  blockid_t* blocks=node->blocks;

  unsigned int block_num=(size+BLOCK_SIZE)/BLOCK_SIZE;
  printf("remove file:%d\n",inum);//
  // printf("block 0:%d\n",blocks[0]);//
  if(block_num<=NDIRECT)
  {
    for(int i=0;i<block_num;++i)
    {
      // printf("free block num:%d\n",blocks[i]);//
      bm->free_block(blocks[i]);
    }
  }
  
  if(block_num>NDIRECT)
  {
    blockid_t indirec=blocks[NDIRECT];
    bm->indirect_free_block(indirec,block_num-NDIRECT);
  }


  free_inode(inum);
  return;
}
